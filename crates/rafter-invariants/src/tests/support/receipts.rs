//! Synthetic artifact, plan, invocation, and source receipts.
//!
//! Every digest is a constant and every counter the smallest honest value, so
//! a receipt says only what the scenario over it needs said. The per-runner
//! source receipt carries the tool pins that layer's contract asks for and
//! nothing beyond them.

use crate::*;

pub(in crate::tests) fn artifact(path: &str) -> ArtifactRef {
    artifact_kind(path, "summary")
}

pub(super) fn artifact_kind(path: &str, kind: &str) -> ArtifactRef {
    ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    }
}

pub(in crate::tests) fn producer_binding(path: &str) -> ProducerBindingReceipt {
    ProducerBindingReceipt {
        binding: crate::provenance::image::PRODUCER_BINDING.to_owned(),
        executable: artifact_kind(path, "producer-binary"),
    }
}

pub(in crate::tests) fn source_receipt(commit: &str) -> SourceReceipt {
    SourceReceipt {
        commit: commit.to_owned(),
        tree: "tree".to_owned(),
        materialization: SourceMaterializationReceipt {
            contract: "git-head-worktree-raw-v1".to_owned(),
            sha256: "0".repeat(64),
            tracked_entries: 1,
            submodules: 0,
        },
        cargo_lock_sha256: "0".repeat(64),
        cargo: "cargo test".to_owned(),
        cargo_sha256: "0".repeat(64),
        cargo_config_sha256: "0".repeat(64),
        rustc: "rustc test".to_owned(),
        rustc_sha256: "0".repeat(64),
        target: "test-target".to_owned(),
        build_profile: "test".to_owned(),
        features: Vec::new(),
        tools: std::collections::BTreeMap::new(),
        process_runtime: crate::receipt::fixture_process_runtime(false),
        environment_sha256: "0".repeat(64),
        clean: true,
    }
}

pub(crate) fn plan_receipt(manifest: &ProfileManifest, profile: &str) -> ExecutionPlanReceipt {
    ExecutionPlanReceipt {
        schema_version: PLAN_SCHEMA_VERSION,
        profile: profile.to_owned(),
        registry: plan_input("verification/raft-invariants.yaml"),
        manifest: plan_input("verification/raft-invariant-profiles.json"),
        result_schema: plan_input("verification/invariant-result-schema.json"),
        verdict_schema: plan_input("verification/invariant-verdict-schema.json"),
        contract: manifest.profiles[profile].clone(),
    }
}

pub(super) fn plan_input(path: &str) -> PlanInput {
    PlanInput {
        path: path.to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    }
}

pub(in crate::tests) fn invocation_receipt(runner: &str) -> InvocationReceipt {
    invocation_receipt_for_profile(runner, "pr")
}

pub(super) fn invocation_receipt_for_profile(runner: &str, profile: &str) -> InvocationReceipt {
    InvocationReceipt {
        program: format!(
            "/workspace/rafter/target/rafter-invariants/producer-images/{}/rafter-invariants",
            "0".repeat(64)
        ),
        program_sha256: "0".repeat(64),
        arguments: vec![
            "run".to_owned(),
            "--profile".to_owned(),
            profile.to_owned(),
            "--layer".to_owned(),
            runner.to_owned(),
        ],
        current_dir: "/workspace/rafter".to_owned(),
        environment: std::collections::BTreeMap::new(),
        environment_sha256: crate::provenance::invocation::digest_environment(
            &std::collections::BTreeMap::new(),
        )
        .expect("valid fixture environment"),
        launchers: Vec::new(),
    }
}

pub(super) fn synthetic_source(
    runner: &str,
    manifest: &ProfileManifest,
    profile: &str,
) -> SourceReceipt {
    let mut source = source_receipt("abc");
    match runner {
        "tests" => source.features = vec!["no-default-features".to_owned()],
        "simulator" => {
            source.build_profile = "release-and-test".to_owned();
            source.features = vec!["internal-test-hooks".to_owned()];
        }
        "tla" => {
            source.build_profile = "tla".to_owned();
            insert_synthetic_tool(&mut source, "java");
        }
        "maelstrom" => bind_synthetic_maelstrom_source(&mut source, manifest, profile),
        _ => unreachable!("catalog emitted an unknown evidence layer"),
    }
    source
}

pub(super) fn bind_synthetic_maelstrom_source(
    source: &mut SourceReceipt,
    manifest: &ProfileManifest,
    profile: &str,
) {
    source.build_profile = "maelstrom-debug".to_owned();
    for tool in ["java", "maelstrom", "dot", "gnuplot"] {
        insert_synthetic_tool(source, tool);
    }
    source.tools.get_mut("java").expect("java tool").version = "openjdk version \"21\"".to_owned();
    source
        .tools
        .get_mut("maelstrom")
        .expect("Maelstrom tool")
        .sha256 = manifest.profiles[profile].runners["maelstrom"].configuration
        ["maelstrom_executable_sha256"]
        .clone();
    source.process_runtime.insert(
        "bash".to_owned(),
        crate::evidence::ExecutableReceipt {
            program: "/bin/bash".to_owned(),
            sha256: "0".repeat(64),
        },
    );
}

pub(super) fn insert_synthetic_tool(source: &mut SourceReceipt, name: &str) {
    source.tools.insert(
        name.to_owned(),
        ToolReceipt {
            version: format!("{name} test"),
            sha256: "0".repeat(64),
        },
    );
}
