use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use sha2::{Digest, Sha256};

use super::verify;
use crate::producer::{
    process::{digest_environment, ProcessLog, TerminationReceipt},
    tla_output::{
        detector_config_kind, detector_label, detector_log_kind, render_detector_config,
        DetectorProbe, DEFAULT_FIXTURE_MODE, DETECTOR_PROBES, MUTATION_SUITE_ARTIFACT_KIND,
        MUTATION_SUITE_LABEL, REQUIRED_MUTATION_TESTS,
    },
};
use crate::{ArtifactRef, InvocationReceipt, ResultBundle};

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[path = "artifact_verify_tla/serialized_tests.rs"]
mod serialized_tests;

#[test]
fn complete_tla_bundle_verifies() {
    let fixture = Fixture::new();
    verify(&fixture.bundle, &fixture.root).expect("complete TLA bundle verifies");
}

#[test]
fn below_floor_membership_trace_fails_closed() {
    let mut fixture = Fixture::new();
    let mut trace = fixture.read_log("tla-trace-log");
    trace.stdout = success_output(28, 28, 28);
    fixture.write_log("tla-trace-log", &trace);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn failed_or_incomplete_mutation_suite_fails_closed() {
    let mut failed = Fixture::new();
    let mut log = failed.read_log(MUTATION_SUITE_ARTIFACT_KIND);
    log.exit_code = Some(1);
    failed.write_log(MUTATION_SUITE_ARTIFACT_KIND, &log);
    assert!(verify(&failed.bundle, &failed.root).is_err());

    let mut incomplete = Fixture::new();
    let mut log = incomplete.read_log(MUTATION_SUITE_ARTIFACT_KIND);
    log.stdout = log
        .stdout
        .replace(
            "test producer::tla_exec::mutation_tests::leader_completeness_uses_commit_authority_term ... ok\n",
            "",
        );
    incomplete.write_log(MUTATION_SUITE_ARTIFACT_KIND, &log);
    assert!(verify(&incomplete.bundle, &incomplete.root).is_err());
}

#[test]
fn mutation_suite_invocation_is_exactly_source_bound() {
    let mut fixture = Fixture::new();
    let mut log = fixture.read_log(MUTATION_SUITE_ARTIFACT_KIND);
    log.invocation.arguments[4] = "another_test_filter".to_owned();
    fixture.write_log(MUTATION_SUITE_ARTIFACT_KIND, &log);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn timed_out_tla_bundle_verifies_progress_without_terminal_proof() {
    let mut fixture = Fixture::new();
    fixture.set_timeout();

    let check = &fixture.bundle.execution.checks[0];
    assert_eq!(check.observations["progress_generated_states"], 181_490_601);
    assert_eq!(check.observations["progress_distinct_states"], 40_062_465);
    assert_eq!(check.observations["progress_states_left"], 19_012_042);
    assert_eq!(check.observations["progress_depth"], 23);
    assert!(!check
        .observations
        .keys()
        .any(|name| name.starts_with("checked:")));
    for terminal in [
        "generated_states",
        "distinct_states",
        "states_left_on_queue",
        "search_depth",
    ] {
        assert!(!check.observations.contains_key(terminal));
    }
    assert!(fixture.bundle.results.iter().all(|result| {
        result.status == crate::EvidenceStatus::Incomplete
            && result.classification == Some(crate::FailureClassification::CoverageNotReached)
    }));
    verify(&fixture.bundle, &fixture.root).expect("timed-out TLA progress verifies");
}

#[test]
fn named_violation_with_malformed_terminal_statistics_returns_a_diagnostic() {
    let mut fixture = Fixture::new();
    fixture.set_malformed_terminal_counterexample("ElectionSafety");

    let diagnostics =
        verify(&fixture.bundle, &fixture.root).expect("named counterexample still verifies");
    assert_eq!(diagnostics.len(), 1);
    assert!(diagnostics[0].contains("TLC 2199 frame has malformed state statistics"));
}

#[test]
fn forged_timeout_progress_fails_closed() {
    let mut fixture = Fixture::new();
    fixture.set_timeout();
    fixture.bundle.execution.checks[0]
        .observations
        .insert("progress_generated_states".to_owned(), 999_999_999);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn missing_timeout_progress_fails_closed() {
    let mut fixture = Fixture::new();
    fixture.set_timeout();
    fixture.bundle.execution.checks[0]
        .observations
        .remove("progress_depth");
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn timeout_without_a_complete_progress_frame_fails_closed() {
    let mut fixture = Fixture::new();
    fixture.set_timeout();
    let mut log = fixture.read_log("tla-log");
    log.stdout = "@!@!@STARTMSG 2185:0 @!@!@\nStarting...\n@!@!@ENDMSG 2185 @!@!@\n".to_owned();
    fixture.write_log("tla-log", &log);
    fixture.bundle.execution.checks[0]
        .observations
        .retain(|name, _| !name.starts_with("progress_"));
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn mismatched_timeout_log_progress_fails_closed() {
    let mut fixture = Fixture::new();
    fixture.set_timeout();
    let mut log = fixture.read_log("tla-log");
    log.stdout = progress_output(181_490_602, 40_062_466, 19_012_043, 24);
    fixture.write_log("tla-log", &log);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn missing_one_detector_pair_fails_closed() {
    let mut fixture = Fixture::new();
    let probe = default_probe("ElectionSafety");
    let config = detector_config_kind(probe).expect("registered probe");
    let log = detector_log_kind(probe).expect("registered probe");
    fixture.bundle.execution.checks[0]
        .artifacts
        .retain(|artifact| artifact.kind != config && artifact.kind != log);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn missing_either_fencing_subprobe_fails_closed() {
    for mode in ["HigherTermRecorderOnly", "StaleAuthorityRecorderOnly"] {
        let mut fixture = Fixture::new();
        let probe = named_probe("StaleLeaderFencing", mode);
        let config = detector_config_kind(probe).expect("registered probe");
        let log = detector_log_kind(probe).expect("registered probe");
        fixture.bundle.execution.checks[0]
            .artifacts
            .retain(|artifact| artifact.kind != config && artifact.kind != log);
        assert!(verify(&fixture.bundle, &fixture.root).is_err());
    }
}

#[test]
fn swapped_detector_pairs_fail_closed() {
    let mut fixture = Fixture::new();
    let election = default_probe("ElectionSafety");
    let matching = named_probe("LogMatching", "LogMatchingRecorderOnly");
    for kind in [detector_config_kind, detector_log_kind] {
        swap_kinds(
            &mut fixture.bundle,
            &kind(election).expect("registered probe"),
            &kind(matching).expect("registered probe"),
        );
    }
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn generic_expected_violation_fails_closed() {
    let mut fixture = Fixture::new();
    let kind = detector_log_kind(default_probe("ElectionSafety")).expect("registered probe");
    let mut log = fixture.read_log(&kind);
    log.stdout = violation_output("ExpectedViolation");
    fixture.write_log(&kind, &log);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn altered_detector_target_fails_closed() {
    let mut fixture = Fixture::new();
    let kind = detector_config_kind(default_probe("ElectionSafety")).expect("registered probe");
    let config = fixture.read_kind(&kind).replace(
        "TargetPredicate = \"ElectionSafety\"",
        "TargetPredicate = \"LogMatching\"",
    );
    fixture.write_kind(&kind, config.as_bytes());
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn mismatched_recorded_config_invocation_fails_closed() {
    let mut fixture = Fixture::new();
    let kind = detector_log_kind(default_probe("ElectionSafety")).expect("registered probe");
    let other_config = fixture.canonical_kind(
        &detector_config_kind(named_probe("LogMatching", "LogMatchingRecorderOnly"))
            .expect("registered probe"),
    );
    let mut log = fixture.read_log(&kind);
    let position = log
        .invocation
        .arguments
        .iter()
        .position(|argument| argument == "-config")
        .expect("config argument exists");
    log.invocation.arguments[position + 1] = other_config;
    fixture.write_log(&kind, &log);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn all_red_bundle_still_verifies_runner_source_binding() {
    let mut fixture = Fixture::new();
    for result in &mut fixture.bundle.results {
        result.status = crate::EvidenceStatus::Error;
        result.classification = Some(crate::FailureClassification::HarnessError);
    }
    fixture.write_kind("tla-runner", b"altered runner");
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn all_red_bundle_still_verifies_exact_invocation() {
    let mut fixture = Fixture::new();
    for result in &mut fixture.bundle.results {
        result.status = crate::EvidenceStatus::Error;
        result.classification = Some(crate::FailureClassification::HarnessError);
    }
    let mut log = fixture.read_log("tla-log");
    log.invocation.arguments.push("-nowarning".to_owned());
    fixture.write_log("tla-log", &log);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn producer_root_rebasing_preserves_exact_tla_paths() {
    let mut fixture = Fixture::new();
    let producer_root = fixture.root.with_extension("producer-root-a");
    fixture.rebase_process_invocations(&producer_root);
    let mutation = fixture.read_log(MUTATION_SUITE_ARTIFACT_KIND);
    assert_eq!(
        mutation.invocation.current_dir,
        producer_root.to_string_lossy()
    );
    verify(&fixture.bundle, &fixture.root).expect("root-rebased TLA invocations verify");

    let mut forged = fixture.read_log("tla-log");
    let classpath = forged
        .invocation
        .arguments
        .iter()
        .position(|argument| argument == "-cp")
        .expect("classpath argument exists");
    forged.invocation.arguments[classpath + 1] = producer_root
        .join("arbitrary/tla2tools.jar")
        .to_string_lossy()
        .into_owned();
    fixture.write_log("tla-log", &forged);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn multi_clause_counterexample_verifies_complete_predicate_fanout() {
    let mut fixture = Fixture::new();
    fixture.set_counterexample("CommittedEntriesHaveQuorum");
    let failed = fixture
        .bundle
        .results
        .iter()
        .filter(|result| result.status == crate::EvidenceStatus::Fail)
        .count();
    assert_eq!(failed, 2);
    verify(&fixture.bundle, &fixture.root).expect("multi-clause counterexample verifies");
}

struct Fixture {
    root: PathBuf,
    bundle: ResultBundle,
}

impl Fixture {
    fn new() -> Self {
        let manifest_dir = fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")))
            .expect("canonicalize manifest directory");
        let workspace = manifest_dir.join("../..");
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = manifest_dir.join("target/test-fixtures").join(format!(
            "rafter-tla-bundle-{}-{sequence}",
            std::process::id()
        ));
        if root.exists() {
            fs::remove_dir_all(&root).expect("remove stale TLA fixture");
        }
        fs::create_dir_all(root.join("specs/tla/raft")).expect("create fixture source directory");
        fs::create_dir_all(root.join("scripts")).expect("create fixture scripts directory");
        fs::create_dir_all(root.join("tools/tla")).expect("create fixture tools directory");
        fs::create_dir_all(root.join("artifacts")).expect("create fixture artifact directory");
        let root = fs::canonicalize(root).expect("canonicalize fixture root");
        let (catalog, manifest) = crate::tests::loaded();
        let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
            .into_iter()
            .find(|bundle| bundle.runner == "tla")
            .expect("synthetic TLA bundle exists");
        bundle.execution.source.environment_sha256 = digest_environment(&BTreeMap::new());
        for artifact in &mut bundle.execution.checks[0].artifacts {
            artifact.path = format!("artifacts/{}", safe_name(&artifact.kind));
        }
        let mut fixture = Self { root, bundle };
        for source in [
            "Raft.tla",
            "RafterInvariantDetectorNegative.tla",
            "RafterInvariantDetectorNegative.cfg",
            "RaftMembershipTraceSample.tla",
            "RaftMembershipTraceSample.cfg",
            "RaftCi.cfg",
        ] {
            fs::copy(
                workspace.join("specs/tla/raft").join(source),
                fixture.root.join("specs/tla/raft").join(source),
            )
            .expect("copy bound TLA source");
        }
        fs::copy(
            workspace.join("scripts/tla-model-check"),
            fixture.root.join("scripts/tla-model-check"),
        )
        .expect("copy TLA runner");
        for source in ["ASSET_ID", "SHA256SUMS"] {
            fs::copy(
                workspace.join("tools/tla").join(source),
                fixture.root.join("tools/tla").join(source),
            )
            .expect("copy TLA tool pin source");
        }
        fixture.populate(&workspace);
        fixture
    }

    fn populate(&mut self, workspace: &Path) {
        let kinds = self.bundle.execution.checks[0]
            .artifacts
            .iter()
            .map(|artifact| artifact.kind.clone())
            .collect::<Vec<_>>();
        for kind in kinds {
            self.write_kind(&kind, b"");
        }
        let config =
            fs::read(workspace.join("specs/tla/raft/RaftCi.cfg")).expect("read main TLA config");
        self.write_kind("tla-config", &config);
        let raft = fs::read(workspace.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
        self.write_kind("tla-spec", &raft);
        let trace_spec = fs::read(workspace.join("specs/tla/raft/RaftMembershipTraceSample.tla"))
            .expect("read trace spec");
        self.write_kind("tla-trace-spec", &trace_spec);
        let trace_config = fs::read(workspace.join("specs/tla/raft/RaftMembershipTraceSample.cfg"))
            .expect("read trace config");
        self.write_kind("tla-trace-config", &trace_config);
        let detector_spec =
            fs::read(workspace.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
                .expect("read detector spec");
        self.write_kind("tla-detector-spec", &detector_spec);
        let template = fs::read_to_string(
            workspace.join("specs/tla/raft/RafterInvariantDetectorNegative.cfg"),
        )
        .expect("read detector config template");
        self.write_kind("tla-detector-config", template.as_bytes());
        let runner = fs::read(workspace.join("scripts/tla-model-check")).expect("read TLA runner");
        self.write_kind("tla-runner", &runner);
        let asset_id = fs::read(workspace.join("tools/tla/ASSET_ID")).expect("read asset ID");
        self.write_kind("tla-tool-asset-id", &asset_id);
        let tool_sha = self.configuration("tool_sha256").to_owned();
        let checksums = fs::read(workspace.join("tools/tla/SHA256SUMS")).expect("read checksums");
        self.write_kind("tla-tool-checksums", &checksums);
        self.artifact_mut("tla-tool").sha256 = tool_sha;
        for probe in DETECTOR_PROBES {
            let kind = detector_config_kind(probe).expect("registered probe");
            let rendered = render_detector_config(&template, probe).expect("render config");
            self.write_kind(&kind, rendered.as_bytes());
        }
        self.write_process_log(
            "tla-log",
            "model-check",
            None,
            success_output(130_000_000, 120_000_000, 1),
            0,
        );
        self.write_process_log(
            "tla-trace-log",
            "trace-sample",
            None,
            success_output(47, 46, 46),
            0,
        );
        for probe in DETECTOR_PROBES {
            let kind = detector_log_kind(probe).expect("registered probe");
            self.write_process_log(
                &kind,
                &detector_label(probe).expect("registered probe"),
                Some(probe),
                violation_output(probe.predicate),
                12,
            );
        }
        self.write_mutation_log();
    }

    fn set_counterexample(&mut self, predicate: &str) {
        let mut log = self.read_log("tla-log");
        log.exit_code = Some(12);
        log.stdout = violation_output(predicate);
        self.write_log("tla-log", &log);
        self.bundle.execution.checks[0].completion = crate::CheckCompletion::Counterexample;
        self.bundle.execution.checks[0]
            .observations
            .retain(|name, _| !name.starts_with("checked:"));
        self.bundle.execution.checks[0].observations.extend([
            ("generated_states".to_owned(), 2),
            ("distinct_states".to_owned(), 2),
            ("states_left_on_queue".to_owned(), 0),
            ("search_depth".to_owned(), 2),
        ]);
        for result in &mut self.bundle.results {
            if result
                .evidence_id
                .rsplit_once('#')
                .is_some_and(|(_, symbol)| symbol == predicate)
            {
                result.status = crate::EvidenceStatus::Fail;
                result.classification = Some(crate::FailureClassification::InvariantViolation);
            } else {
                result.status = crate::EvidenceStatus::Incomplete;
                result.classification = Some(crate::FailureClassification::CoverageNotReached);
            }
        }
    }

    fn set_harness_violation(&mut self, predicate: &str) {
        let mut log = self.read_log("tla-log");
        log.exit_code = Some(12);
        log.stdout = violation_output(predicate);
        self.write_log("tla-log", &log);
        let check = &mut self.bundle.execution.checks[0];
        check.completion = crate::CheckCompletion::HarnessError;
        check
            .observations
            .retain(|name, _| !name.starts_with("checked:"));
        check.observations.extend([
            ("generated_states".to_owned(), 2),
            ("distinct_states".to_owned(), 2),
            ("states_left_on_queue".to_owned(), 0),
            ("search_depth".to_owned(), 2),
        ]);
        for result in &mut self.bundle.results {
            result.status = crate::EvidenceStatus::Error;
            result.classification = Some(crate::FailureClassification::HarnessError);
        }
    }

    fn set_timed_out_counterexample(&mut self, predicate: &str) {
        self.set_counterexample(predicate);
        let mut log = self.read_log("tla-log");
        log.exit_code = None;
        log.timed_out = true;
        log.termination = Some(TerminationReceipt {
            process_group: true,
            term_signal_sent: true,
            grace_ms: 30_000,
            kill_signal_sent: true,
        });
        log.stdout
            .push_str("@!@!@STARTMSG 2200:0 @!@!@\nmalformed progress\n@!@!@ENDMSG 2200 @!@!@\n");
        self.write_log("tla-log", &log);
        self.bundle.execution.checks[0]
            .observations
            .retain(|name, _| {
                !matches!(
                    name.as_str(),
                    "generated_states"
                        | "distinct_states"
                        | "states_left_on_queue"
                        | "search_depth"
                )
            });
    }

    fn set_malformed_terminal_counterexample(&mut self, predicate: &str) {
        self.set_counterexample(predicate);
        let mut log = self.read_log("tla-log");
        log.stdout = log.stdout.replacen(
            "2 states generated, 2 distinct states found, 0 states left on queue.",
            "malformed statistics",
            1,
        );
        self.write_log("tla-log", &log);
        for observation in [
            "generated_states",
            "distinct_states",
            "states_left_on_queue",
            "search_depth",
        ] {
            self.bundle.execution.checks[0]
                .observations
                .insert(observation.to_owned(), 0);
        }
    }

    fn set_timeout(&mut self) {
        let mut log = self.read_log("tla-log");
        log.exit_code = None;
        log.timed_out = true;
        log.termination = Some(TerminationReceipt {
            process_group: true,
            term_signal_sent: true,
            grace_ms: 30_000,
            kill_signal_sent: true,
        });
        log.stdout = progress_output(181_490_601, 40_062_465, 19_012_042, 23);
        self.write_log("tla-log", &log);

        let check = &mut self.bundle.execution.checks[0];
        check.completion = crate::CheckCompletion::Timeout;
        check.observations.retain(|name, _| {
            !name.starts_with("checked:")
                && !matches!(
                    name.as_str(),
                    "generated_states"
                        | "distinct_states"
                        | "states_left_on_queue"
                        | "search_depth"
                )
        });
        check.observations.extend([
            ("progress_generated_states".to_owned(), 181_490_601),
            ("progress_distinct_states".to_owned(), 40_062_465),
            ("progress_states_left".to_owned(), 19_012_042),
            ("progress_depth".to_owned(), 23),
        ]);
        for result in &mut self.bundle.results {
            result.status = crate::EvidenceStatus::Incomplete;
            result.classification = Some(crate::FailureClassification::CoverageNotReached);
        }
    }

    fn write_process_log(
        &mut self,
        kind: &str,
        label: &str,
        probe: Option<DetectorProbe>,
        stdout: String,
        exit_code: i32,
    ) {
        let log = ProcessLog {
            schema_version: 3,
            label: label.to_owned(),
            invocation: self.invocation(label, probe),
            exit_code: Some(exit_code),
            timed_out: false,
            termination: Some(TerminationReceipt {
                process_group: true,
                term_signal_sent: false,
                grace_ms: 30_000,
                kill_signal_sent: false,
            }),
            duration_ms: 1,
            peak_rss_kib: 1,
            stdout,
            stderr: String::new(),
        };
        self.write_log(kind, &log);
    }

    fn write_mutation_log(&mut self) {
        let expected_count = REQUIRED_MUTATION_TESTS.len();
        let mut stdout = format!("running {expected_count} tests\n");
        for name in REQUIRED_MUTATION_TESTS {
            writeln!(
                stdout,
                "test producer::tla_exec::mutation_tests::{name} ... ok"
            )
            .expect("write mutation fixture output");
        }
        writeln!(
            stdout,
            "test result: ok. {expected_count} passed; 0 failed; 0 ignored; 0 measured; 179 filtered out"
        )
        .expect("write mutation fixture summary");
        let environment = BTreeMap::new();
        let log = ProcessLog {
            schema_version: 3,
            label: MUTATION_SUITE_LABEL.to_owned(),
            invocation: InvocationReceipt {
                program: "cargo".to_owned(),
                program_sha256: self.bundle.execution.source.cargo_sha256.clone(),
                arguments: [
                    "test",
                    "--locked",
                    "-p",
                    "rafter-invariants",
                    "producer::tla_exec::mutation_tests",
                    "--",
                    "--ignored",
                    "--test-threads=1",
                ]
                .map(str::to_owned)
                .to_vec(),
                current_dir: self.root.to_string_lossy().into_owned(),
                environment_sha256: digest_environment(&environment),
                environment,
            },
            exit_code: Some(0),
            timed_out: false,
            termination: Some(TerminationReceipt {
                process_group: true,
                term_signal_sent: false,
                grace_ms: 30_000,
                kill_signal_sent: false,
            }),
            duration_ms: 1,
            peak_rss_kib: 1,
            stdout,
            stderr: String::new(),
        };
        self.write_log(MUTATION_SUITE_ARTIFACT_KIND, &log);
    }

    fn invocation(&self, label: &str, probe: Option<DetectorProbe>) -> InvocationReceipt {
        let (config, module, workers) = match probe {
            Some(probe) => (
                self.canonical_kind(&detector_config_kind(probe).expect("registered probe")),
                "RafterInvariantDetectorNegative.tla",
                "1",
            ),
            None if label == "trace-sample" => (
                "RaftMembershipTraceSample.cfg".to_owned(),
                "RaftMembershipTraceSample.tla",
                "1",
            ),
            None => (
                self.configuration("config").to_owned(),
                "Raft.tla",
                self.configuration("workers"),
            ),
        };
        let configuration = &self.bundle.execution.plan.contract.runners["tla"].configuration;
        let main_model_check = probe.is_none() && label != "trace-sample";
        let mut arguments = Vec::new();
        if main_model_check {
            if let Some(max_heap) = configuration.get("max_heap") {
                arguments.push(format!("-Xmx{max_heap}"));
            }
        }
        arguments.extend([
            "-XX:+UseParallelGC".to_owned(),
            "-cp".to_owned(),
            self.root
                .join("tools/cache/tla2tools.jar")
                .to_string_lossy()
                .into_owned(),
            "tlc2.TLC".to_owned(),
            "-tool".to_owned(),
            "-workers".to_owned(),
            workers.to_owned(),
            "-seed".to_owned(),
            self.configuration("seed").to_owned(),
            "-fp".to_owned(),
            "0".to_owned(),
        ]);
        if main_model_check {
            if let Some(fp_mem) = configuration.get("fp_mem") {
                arguments.extend(["-fpmem".to_owned(), fp_mem.clone()]);
            }
        }
        arguments.extend(["-metadir".to_owned(), "/proc/self/fd/3".to_owned()]);
        arguments.extend(["-config".to_owned(), config, module.to_owned()]);
        InvocationReceipt {
            program: "java".to_owned(),
            program_sha256: self.bundle.execution.source.tools["java"].sha256.clone(),
            arguments,
            current_dir: self
                .root
                .join("specs/tla/raft")
                .to_string_lossy()
                .into_owned(),
            environment: BTreeMap::new(),
            environment_sha256: digest_environment(&BTreeMap::new()),
        }
    }

    fn configuration(&self, name: &str) -> &str {
        &self.bundle.execution.plan.contract.runners["tla"].configuration[name]
    }

    fn canonical_kind(&self, kind: &str) -> String {
        fs::canonicalize(self.root.join(&self.artifact(kind).path))
            .expect("canonicalize artifact")
            .to_string_lossy()
            .into_owned()
    }

    fn read_log(&self, kind: &str) -> ProcessLog {
        serde_json::from_str(&self.read_kind(kind)).expect("read process log")
    }

    fn rebase_process_invocations(&mut self, producer_root: &Path) {
        assert!(producer_root.is_absolute());
        let aggregate_root = self.root.clone();
        let kinds = self.bundle.execution.checks[0]
            .artifacts
            .iter()
            .filter(|artifact| {
                matches!(
                    artifact.kind.as_str(),
                    "tla-log" | "tla-trace-log" | MUTATION_SUITE_ARTIFACT_KIND
                ) || artifact.kind.starts_with("tla-detector-log")
            })
            .map(|artifact| artifact.kind.clone())
            .collect::<Vec<_>>();
        for kind in kinds {
            let mut log = self.read_log(&kind);
            log.invocation.current_dir =
                rebase_fixture_path(&log.invocation.current_dir, &aggregate_root, producer_root);
            for argument in &mut log.invocation.arguments {
                if Path::new(argument).starts_with(&aggregate_root) {
                    *argument = rebase_fixture_path(argument, &aggregate_root, producer_root);
                }
            }
            self.write_log(&kind, &log);
        }
    }

    fn write_log(&mut self, kind: &str, log: &ProcessLog) {
        self.write_kind(
            kind,
            serde_json::to_string(log)
                .expect("serialize process log")
                .as_bytes(),
        );
    }

    fn read_kind(&self, kind: &str) -> String {
        fs::read_to_string(self.root.join(&self.artifact(kind).path)).expect("read artifact")
    }

    fn write_kind(&mut self, kind: &str, bytes: &[u8]) {
        let path = self.root.join(&self.artifact(kind).path);
        fs::write(path, bytes).expect("write artifact");
        let artifact = self.artifact_mut(kind);
        artifact.size_bytes = bytes.len() as u64;
        artifact.sha256 = format!("{:x}", Sha256::digest(bytes));
    }

    fn artifact(&self, kind: &str) -> &ArtifactRef {
        self.bundle.execution.checks[0]
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == kind)
            .expect("artifact kind exists")
    }

    fn artifact_mut(&mut self, kind: &str) -> &mut ArtifactRef {
        self.bundle.execution.checks[0]
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.kind == kind)
            .expect("artifact kind exists")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn swap_kinds(bundle: &mut ResultBundle, first: &str, second: &str) {
    for artifact in &mut bundle.execution.checks[0].artifacts {
        if artifact.kind == first {
            artifact.kind = second.to_owned();
        } else if artifact.kind == second {
            artifact.kind = first.to_owned();
        }
    }
}

fn rebase_fixture_path(path: &str, from: &Path, to: &Path) -> String {
    let relative = Path::new(path)
        .strip_prefix(from)
        .expect("fixture invocation path belongs to the aggregate checkout");
    if relative.as_os_str().is_empty() {
        to.to_string_lossy().into_owned()
    } else {
        to.join(relative).to_string_lossy().into_owned()
    }
}

fn default_probe(predicate: &'static str) -> DetectorProbe {
    named_probe(predicate, DEFAULT_FIXTURE_MODE)
}

fn named_probe(predicate: &'static str, mode: &'static str) -> DetectorProbe {
    DETECTOR_PROBES
        .into_iter()
        .find(|probe| probe.predicate == predicate && probe.mode == mode)
        .expect("registered detector probe")
}

fn safe_name(kind: &str) -> String {
    kind.replace(':', "-")
}

fn success_output(generated: u64, distinct: u64, depth: u64) -> String {
    format!(
        "@!@!@STARTMSG 2193:0 @!@!@\nNo error.\n@!@!@ENDMSG 2193 @!@!@\n\
         @!@!@STARTMSG 2199:0 @!@!@\n{generated} states generated, {distinct} distinct states found, 0 states left on queue.\n@!@!@ENDMSG 2199 @!@!@\n\
         @!@!@STARTMSG 2194:0 @!@!@\nThe depth of the complete state graph search is {depth}.\n@!@!@ENDMSG 2194 @!@!@\n\
         @!@!@STARTMSG 2186:0 @!@!@\nFinished.\n@!@!@ENDMSG 2186 @!@!@\n"
    )
}

fn violation_output(predicate: &str) -> String {
    format!(
        "@!@!@STARTMSG 2110:1 @!@!@\nInvariant {predicate} is violated.\n@!@!@ENDMSG 2110 @!@!@\n\
         @!@!@STARTMSG 2199:0 @!@!@\n2 states generated, 2 distinct states found, 0 states left on queue.\n@!@!@ENDMSG 2199 @!@!@\n\
         @!@!@STARTMSG 2194:0 @!@!@\nThe depth of the complete state graph search is 2.\n@!@!@ENDMSG 2194 @!@!@\n\
         @!@!@STARTMSG 2186:0 @!@!@\nFinished.\n@!@!@ENDMSG 2186 @!@!@\n"
    )
}

fn progress_output(generated: u64, distinct: u64, states_left: u64, depth: u64) -> String {
    format!(
        "@!@!@STARTMSG 2200:0 @!@!@\nProgress(21) at 2026-07-13 19:18:31: 23,784,130 states generated (4,670,725 s/min), 6,246,309 distinct states found (1,150,848 ds/min), 3,294,097 states left on queue.\n@!@!@ENDMSG 2200 @!@!@\n\
         @!@!@STARTMSG 2200:0 @!@!@\nProgress({depth}) at 2026-07-13 19:52:32: {generated} states generated (4,966,137 s/min), {distinct} distinct states found (1,000,915 ds/min), {states_left} states left on queue.\n@!@!@ENDMSG 2200 @!@!@\n"
    )
}
