use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use sha2::{Digest, Sha256};

use super::verify;
use crate::producer::{
    process::{digest_environment, ProcessLog},
    tla_output::{
        detector_config_kind, detector_label, detector_log_kind, render_detector_config,
        REGISTERED_PREDICATES,
    },
};
use crate::{ArtifactRef, InvocationReceipt, ResultBundle};

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn complete_tla_bundle_verifies() {
    let fixture = Fixture::new();
    verify(&fixture.bundle, &fixture.root).expect("complete TLA bundle verifies");
}

#[test]
fn missing_one_detector_pair_fails_closed() {
    let mut fixture = Fixture::new();
    let config = detector_config_kind("ElectionSafety").expect("registered predicate");
    let log = detector_log_kind("ElectionSafety").expect("registered predicate");
    fixture.bundle.execution.checks[0]
        .artifacts
        .retain(|artifact| artifact.kind != config && artifact.kind != log);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn swapped_detector_pairs_fail_closed() {
    let mut fixture = Fixture::new();
    for kind in [detector_config_kind, detector_log_kind] {
        swap_kinds(
            &mut fixture.bundle,
            &kind("ElectionSafety").expect("registered predicate"),
            &kind("LogMatching").expect("registered predicate"),
        );
    }
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn generic_expected_violation_fails_closed() {
    let mut fixture = Fixture::new();
    let kind = detector_log_kind("ElectionSafety").expect("registered predicate");
    let mut log = fixture.read_log(&kind);
    log.stdout = violation_output("ExpectedViolation");
    fixture.write_log(&kind, &log);
    assert!(verify(&fixture.bundle, &fixture.root).is_err());
}

#[test]
fn altered_detector_target_fails_closed() {
    let mut fixture = Fixture::new();
    let kind = detector_config_kind("ElectionSafety").expect("registered predicate");
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
    let kind = detector_log_kind("ElectionSafety").expect("registered predicate");
    let other_config =
        fixture.canonical_kind(&detector_config_kind("LogMatching").expect("registered predicate"));
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

struct Fixture {
    root: PathBuf,
    bundle: ResultBundle,
}

impl Fixture {
    fn new() -> Self {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "rafter-tla-bundle-{}-{sequence}",
            std::process::id()
        ));
        if root.exists() {
            fs::remove_dir_all(&root).expect("remove stale TLA fixture");
        }
        fs::create_dir_all(root.join("specs/tla/raft")).expect("create fixture source directory");
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
            "RaftCi.cfg",
        ] {
            fs::copy(
                workspace.join("specs/tla/raft").join(source),
                fixture.root.join("specs/tla/raft").join(source),
            )
            .expect("copy bound TLA source");
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
        let detector_spec =
            fs::read(workspace.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
                .expect("read detector spec");
        self.write_kind("tla-detector-spec", &detector_spec);
        let template = fs::read_to_string(
            workspace.join("specs/tla/raft/RafterInvariantDetectorNegative.cfg"),
        )
        .expect("read detector config template");
        self.write_kind("tla-detector-config", template.as_bytes());
        let asset_id = self.configuration("tool_asset_id").to_owned();
        self.write_kind("tla-tool-asset-id", format!("{asset_id}\n").as_bytes());
        let tool_sha = self.configuration("tool_sha256").to_owned();
        self.write_kind(
            "tla-tool-checksums",
            format!("{tool_sha}  tla2tools.jar\n").as_bytes(),
        );
        self.artifact_mut("tla-tool").sha256 = tool_sha;
        for predicate in REGISTERED_PREDICATES {
            let kind = detector_config_kind(predicate).expect("registered predicate");
            let rendered = render_detector_config(&template, predicate).expect("render config");
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
            success_output(4, 4, 4),
            0,
        );
        for predicate in REGISTERED_PREDICATES {
            let kind = detector_log_kind(predicate).expect("registered predicate");
            self.write_process_log(
                &kind,
                &detector_label(predicate).expect("registered predicate"),
                Some(predicate),
                violation_output(predicate),
                12,
            );
        }
    }

    fn write_process_log(
        &mut self,
        kind: &str,
        label: &str,
        predicate: Option<&str>,
        stdout: String,
        exit_code: i32,
    ) {
        let log = ProcessLog {
            schema_version: 2,
            label: label.to_owned(),
            invocation: self.invocation(label, predicate),
            exit_code: Some(exit_code),
            timed_out: false,
            duration_ms: 1,
            peak_rss_kib: 1,
            stdout,
            stderr: String::new(),
        };
        self.write_log(kind, &log);
    }

    fn invocation(&self, label: &str, predicate: Option<&str>) -> InvocationReceipt {
        let (config, module, workers) = match predicate {
            Some(predicate) => (
                self.canonical_kind(
                    &detector_config_kind(predicate).expect("registered predicate"),
                ),
                "RafterInvariantDetectorNegative.tla",
                "1",
            ),
            None if label == "trace-sample" => {
                ("RaftTraceSample.cfg".to_owned(), "RaftTraceSample.tla", "1")
            }
            None => (
                self.configuration("config").to_owned(),
                "Raft.tla",
                self.configuration("workers"),
            ),
        };
        let source_prefix = self
            .bundle
            .source_ref
            .get(..12)
            .unwrap_or(&self.bundle.source_ref);
        InvocationReceipt {
            program: "java".to_owned(),
            program_sha256: self.bundle.execution.source.tools["java"].sha256.clone(),
            arguments: vec![
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
                "-metadir".to_owned(),
                self.root
                    .join("target/rafter-invariants/tla")
                    .join(source_prefix)
                    .join(&self.bundle.profile)
                    .join(label)
                    .to_string_lossy()
                    .into_owned(),
                "-config".to_owned(),
                config,
                module.to_owned(),
            ],
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
