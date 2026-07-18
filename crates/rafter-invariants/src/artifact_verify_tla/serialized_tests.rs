use std::{fs, path::Path, process::Command};

use sha2::{Digest, Sha256};

use super::Fixture;
use crate::producer::{
    process::{digest_environment, ProcessLog},
    tla_output::MUTATION_SUITE_ARTIFACT_KIND,
};

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn producer_root_a_invocations_verify_in_aggregate_checkout_root_b() {
    let mut fixture = Fixture::new();
    let producer_root = fixture.root.with_extension("producer-root-a");
    assert_ne!(producer_root, fixture.root);
    assert!(!producer_root.exists());

    let (loaded, _) = fixture.load_and_aggregate_from(&producer_root);
    assert_eq!(
        loaded.bundles[0].execution.invocation.current_dir,
        producer_root.to_string_lossy()
    );
    for artifact in &loaded.bundles[0].execution.checks[0].artifacts {
        if is_tla_process_artifact(&artifact.kind) {
            let log = fixture.read_log(&artifact.kind);
            assert!(
                Path::new(&log.invocation.current_dir).starts_with(&producer_root),
                "{} retained a non-producer-root invocation: {}",
                artifact.kind,
                log.invocation.current_dir
            );
        }
    }
    assert!(
        loaded.harness_errors.is_empty(),
        "root-rebased serialized TLA evidence failed verification: {:?}",
        loaded.harness_errors
    );
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn typeok_violation_verifies_as_a_harness_class_violation() {
    let mut fixture = Fixture::new();
    fixture.set_harness_violation("TypeOK");
    let (loaded, report) = fixture.load_and_aggregate();
    assert!(loaded.harness_errors.is_empty());
    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(loaded.bundles[0].results.iter().all(|result| {
        result.status == crate::EvidenceStatus::Error
            && result.classification == Some(crate::FailureClassification::HarnessError)
            && result.message.as_deref() == Some("TLA fixture harness error")
            && !result.artifacts.is_empty()
    }));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn malformed_progress_cannot_erase_a_complete_counterexample() {
    let mut fixture = Fixture::new();
    fixture.set_timed_out_counterexample("ElectionSafety");
    let (loaded, report) = fixture.load_and_aggregate();
    assert_eq!(loaded.harness_errors.len(), 1);
    assert!(loaded.harness_errors[0].contains("malformed progress statistics"));
    let failed = loaded.bundles[0]
        .results
        .iter()
        .find(|result| result.status == crate::EvidenceStatus::Fail)
        .expect("ElectionSafety result fails")
        .clone();
    let verdict = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == failed.invariant_id)
        .expect("counterexample invariant verdict");
    let issue = verdict
        .issues
        .iter()
        .find(|issue| {
            issue.evidence_id == failed.evidence_id
                && issue.classification == crate::FailureClassification::InvariantViolation
        })
        .expect("semantic counterexample survives final aggregation");
    assert_eq!(issue.message, "TLA fixture invariant violation");
    assert_eq!(issue.artifacts, failed.artifacts);
    assert!(verdict.issues.iter().any(|issue| {
        issue.evidence_id == "aggregate/harness"
            && issue.classification == crate::FailureClassification::HarnessError
            && issue.message.contains("malformed progress statistics")
    }));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn malformed_terminal_statistics_are_secondary_to_a_complete_counterexample() {
    let mut fixture = Fixture::new();
    fixture.set_malformed_terminal_counterexample("ElectionSafety");
    let (loaded, report) = fixture.load_and_aggregate();
    assert_eq!(loaded.harness_errors.len(), 1);
    assert!(loaded.harness_errors[0].contains("malformed state statistics"));

    let failed = loaded.bundles[0]
        .results
        .iter()
        .find(|result| result.status == crate::EvidenceStatus::Fail)
        .expect("ElectionSafety result fails");
    let verdict = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == failed.invariant_id)
        .expect("counterexample invariant verdict");
    assert!(verdict.issues.iter().any(|issue| {
        issue.evidence_id == failed.evidence_id
            && issue.classification == crate::FailureClassification::InvariantViolation
    }));
    assert!(verdict.issues.iter().any(|issue| {
        issue.evidence_id == "aggregate/harness"
            && issue.classification == crate::FailureClassification::HarnessError
            && issue.message.contains("malformed state statistics")
    }));
}

impl Fixture {
    fn load_and_aggregate(&mut self) -> (crate::aggregate::LoadedEvidence, crate::VerdictReport) {
        let producer_root = self.root.clone();
        self.load_and_aggregate_from(&producer_root)
    }

    fn load_and_aggregate_from(
        &mut self,
        producer_root: &Path,
    ) -> (crate::aggregate::LoadedEvidence, crate::VerdictReport) {
        let workspace = fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .expect("canonicalize workspace root");
        self.materialize_serialized_bundle(&workspace, producer_root);
        let bundle_path = self.root.with_extension("result.json");
        fs::write(
            &bundle_path,
            format!(
                "{}\n",
                serde_json::to_string_pretty(&self.bundle).expect("serialize TLA fixture bundle")
            ),
        )
        .expect("write TLA fixture bundle");
        let loaded =
            crate::aggregate::load_evidence_at(std::slice::from_ref(&bundle_path), &self.root);
        let _ = fs::remove_file(bundle_path);
        assert_eq!(
            loaded.bundles.len(),
            1,
            "serialized TLA bundle was discarded: {:?}",
            loaded.harness_errors
        );
        let (catalog, manifest) = crate::tests::loaded();
        let report = crate::aggregate_with_harness_errors(
            &catalog,
            &manifest,
            "pr",
            &loaded.bundles[0].source_ref,
            &loaded.bundles,
            &loaded.harness_errors,
        )
        .expect("aggregate serialized TLA fixture");
        (loaded, report)
    }

    fn materialize_serialized_bundle(&mut self, workspace: &Path, producer_root: &Path) {
        copy_tracked_workspace(workspace, &self.root);
        for (path, input) in [
            (
                "verification/raft-invariants.yaml",
                &mut self.bundle.execution.plan.registry,
            ),
            (
                "verification/raft-invariant-profiles.json",
                &mut self.bundle.execution.plan.manifest,
            ),
            (
                "verification/invariant-result-schema.json",
                &mut self.bundle.execution.plan.result_schema,
            ),
            (
                "verification/invariant-verdict-schema.json",
                &mut self.bundle.execution.plan.verdict_schema,
            ),
        ] {
            let bytes = fs::read(workspace.join(path)).expect("read plan input");
            let destination = self.root.join(path);
            fs::create_dir_all(destination.parent().expect("plan input parent"))
                .expect("create plan input directory");
            fs::write(destination, &bytes).expect("write plan input");
            input.path = path.to_owned();
            input.sha256 = format!("{:x}", Sha256::digest(&bytes));
            input.size_bytes = bytes.len() as u64;
        }
        crate::producer::fetch_tla_tool_at(workspace)
            .expect("fetch and verify pinned TLC tool fixture");
        let tool = fs::read(workspace.join("tools/cache/tla2tools.jar"))
            .expect("read pinned TLC tool fixture");
        self.write_kind("tla-tool", &tool);
        self.write_execution_artifacts();
        fs::write(self.root.join(".gitignore"), "artifacts/\ntarget/\n")
            .expect("ignore generated fixture evidence");
        git(&self.root, &["init", "-q"]);
        git(&self.root, &["add", "."]);
        git(
            &self.root,
            &[
                "-c",
                "user.name=Rafter Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-q",
                "-m",
                "test: materialize TLA evidence fixture",
            ],
        );
        let old_source_ref = self.bundle.source_ref.clone();
        let source = crate::producer::source::capture_for_layer_at("tla", &self.root)
            .expect("capture fixture source identity");
        self.bundle.source_ref = source.commit.clone();
        self.bundle.execution.source = source;
        self.rebind_process_logs(&old_source_ref);
        self.rebase_process_invocations(producer_root);
        self.bind_producer(producer_root);
        self.bind_nonpass_results();
        self.bind_resource_metrics();
    }

    fn write_execution_artifacts(&mut self) {
        let artifacts = self.bundle.execution.artifacts.clone();
        for artifact in artifacts {
            let bytes: &[u8] = if artifact.kind == "producer-binary" {
                b"fixture TLA producer binary"
            } else {
                b"fixture TLA execution log"
            };
            let destination = self.root.join(&artifact.path);
            fs::create_dir_all(destination.parent().expect("execution artifact parent"))
                .expect("create execution artifact directory");
            fs::write(destination, bytes).expect("write execution artifact");
            let bound = self
                .bundle
                .execution
                .artifacts
                .iter_mut()
                .find(|candidate| candidate.path == artifact.path)
                .expect("execution artifact exists");
            bound.sha256 = format!("{:x}", Sha256::digest(bytes));
            bound.size_bytes = bytes.len() as u64;
            if bound.kind == "producer-binary" {
                self.bundle.execution.producer.executable = bound.clone();
            }
        }
    }

    fn rebind_process_logs(&mut self, old_source_ref: &str) {
        let source_prefix = self
            .bundle
            .source_ref
            .get(..12)
            .unwrap_or(&self.bundle.source_ref)
            .to_owned();
        let old_prefix = old_source_ref.get(..12).unwrap_or(old_source_ref);
        let kinds = self.bundle.execution.checks[0]
            .artifacts
            .iter()
            .filter(|artifact| is_tla_process_artifact(&artifact.kind))
            .map(|artifact| artifact.kind.clone())
            .collect::<Vec<_>>();
        let environment = crate::producer::process::base_environment();
        let environment_sha256 = digest_environment(&environment);
        let java_sha256 = self.bundle.execution.source.tools["java"].sha256.clone();
        let cargo_sha256 = self.bundle.execution.source.cargo_sha256.clone();
        for kind in kinds {
            let mut log = self.read_log(&kind);
            log.invocation.environment = environment.clone();
            log.invocation.environment_sha256 = environment_sha256.clone();
            log.invocation.program_sha256 = if kind == MUTATION_SUITE_ARTIFACT_KIND {
                cargo_sha256.clone()
            } else {
                java_sha256.clone()
            };
            for argument in &mut log.invocation.arguments {
                *argument = argument.replace(
                    &format!("/tla/{old_prefix}/pr/"),
                    &format!("/tla/{source_prefix}/pr/"),
                );
            }
            self.write_log(&kind, &log);
        }
    }

    fn bind_producer(&mut self, producer_root: &Path) {
        let executable = self.bundle.execution.producer.executable.clone();
        self.bundle.execution.invocation.program_sha256 = executable.sha256.clone();
        self.bundle.execution.invocation.program =
            crate::producer_image::image_path(producer_root, &executable.sha256)
                .to_string_lossy()
                .into_owned();
        self.bundle.execution.invocation.current_dir = producer_root.to_string_lossy().into_owned();
    }

    fn bind_nonpass_results(&mut self) {
        let replay = self.artifact("tla-log").clone();
        for result in &mut self.bundle.results {
            match result.status {
                crate::EvidenceStatus::Pass => {}
                crate::EvidenceStatus::Fail => {
                    result.message = Some("TLA fixture invariant violation".to_owned());
                    result.artifacts = vec![replay.clone()];
                }
                crate::EvidenceStatus::Incomplete => {
                    result.message = Some("TLA fixture coverage not reached".to_owned());
                    result.artifacts = vec![replay.clone()];
                }
                crate::EvidenceStatus::Error => {
                    result.message = Some("TLA fixture harness error".to_owned());
                    result.artifacts = vec![replay.clone()];
                }
            }
        }
    }

    fn bind_resource_metrics(&mut self) {
        let metrics = self.bundle.execution.checks[0]
            .artifacts
            .iter()
            .filter(|artifact| is_tla_process_artifact(&artifact.kind))
            .map(|artifact| {
                serde_json::from_str::<ProcessLog>(
                    &fs::read_to_string(self.root.join(&artifact.path))
                        .expect("read process metric artifact"),
                )
                .expect("parse process metric artifact")
            })
            .fold((0_u64, 0_u64), |(duration, peak), log| {
                (
                    duration
                        .checked_add(log.duration_ms)
                        .expect("fixture duration fits u64"),
                    peak.max(log.peak_rss_kib),
                )
            });
        self.bundle.execution.checks[0].duration_ms = metrics.0;
        self.bundle.execution.checks[0].peak_rss_kib = metrics.1;
        self.bundle.execution.duration_ms = metrics.0;
        self.bundle.execution.peak_rss_kib = metrics.1;
    }
}

fn is_tla_process_artifact(kind: &str) -> bool {
    matches!(
        kind,
        "tla-log" | "tla-trace-log" | MUTATION_SUITE_ARTIFACT_KIND
    ) || kind.starts_with("tla-detector-log")
}

fn copy_tracked_workspace(from: &Path, to: &Path) {
    let output = Command::new("git")
        .args(["ls-files", "-z"])
        .current_dir(from)
        .output()
        .expect("list tracked fixture source files");
    assert!(
        output.status.success(),
        "git ls-files failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    for entry in output.stdout.split(|byte| *byte == 0) {
        if entry.is_empty() {
            continue;
        }
        let path = std::str::from_utf8(entry).expect("tracked path is utf-8");
        let source = from.join(path);
        let destination = to.join(path);
        fs::create_dir_all(destination.parent().expect("tracked file parent"))
            .expect("create tracked file parent");
        fs::copy(&source, &destination).unwrap_or_else(|error| {
            panic!(
                "copy tracked fixture source {} to {}: {error}",
                source.display(),
                destination.display()
            )
        });
    }
}

fn git(root: &Path, arguments: &[&str]) {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()
        .expect("run git fixture command");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
