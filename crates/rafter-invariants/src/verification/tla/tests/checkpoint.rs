//! TLA+ checkpoint metadata acceptance scenarios.

use std::fs;

use sha2::{Digest, Sha256};

use super::{expected_contract, verify_checkpoint};
use crate::evidence::format::tla::checkpoint::{
    RecoveryReport, RecoveryStatus, RECOVERY_REPORT_KIND,
};

#[test]
fn verifier_derivation_rejects_missing_and_duplicate_checkpoint_inputs() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tla")
        .expect("TLA bundle");
    let configuration = &bundle.execution.plan.contract.runners["tla"].configuration;
    let artifacts = &bundle.execution.checks[0].artifacts;

    let missing = artifacts
        .iter()
        .filter(|artifact| artifact.kind != "tla-tool")
        .cloned()
        .collect::<Vec<_>>();
    assert!(expected_contract(&bundle.profile, configuration, &missing).is_err());

    let mut duplicate = artifacts.clone();
    let repeated = artifacts
        .iter()
        .find(|artifact| artifact.kind == "tla-tool")
        .expect("checkpoint input")
        .clone();
    duplicate.push(repeated);
    assert!(expected_contract(&bundle.profile, configuration, &duplicate).is_err());
}

#[test]
fn checkpointed_counterexample_verifies_without_abandoned_final_metadata() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tla")
        .expect("TLA bundle");
    bundle.profile = "weekly".to_owned();
    bundle.execution.plan.profile = "weekly".to_owned();
    bundle.execution.plan.contract = manifest.profiles["weekly"].clone();
    let check = bundle.execution.checks.first_mut().expect("TLA check");
    check.completion = crate::CheckCompletion::Counterexample;

    let configuration = &bundle.execution.plan.contract.runners["tla"].configuration;
    let contract = expected_contract("weekly", configuration, &check.artifacts)
        .expect("derive weekly checkpoint contract");
    let report = RecoveryReport {
        schema_version: 1,
        status: RecoveryStatus::Fresh,
        contract_sha256: contract.sha256().expect("digest checkpoint contract"),
        candidate_present: false,
        recovery_attempted: false,
        recovered_checkpoint: None,
        error: None,
    };
    let root = std::env::temp_dir().join(format!(
        "rafter-checkpoint-counterexample-verifier-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("artifacts")).expect("create checkpoint verifier fixture");
    let bytes = serde_json::to_vec_pretty(&report).expect("serialize recovery report");
    let path = "artifacts/checkpoint-recovery-report.json";
    fs::write(root.join(path), &bytes).expect("write recovery report");
    check.artifacts.push(crate::ArtifactRef {
        kind: RECOVERY_REPORT_KIND.to_owned(),
        path: path.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        size_bytes: bytes.len() as u64,
    });

    let check = bundle.execution.checks.first().expect("TLA check");
    verify_checkpoint(&bundle, check, &root, true)
        .expect("checkpointed counterexample retains recovery evidence");
    bundle.execution.checks[0].completion = crate::CheckCompletion::HarnessError;
    let check = bundle.execution.checks.first().expect("TLA check");
    verify_checkpoint(&bundle, check, &root, true)
        .expect("checkpointed TypeOK violation retains recovery evidence");
    let _ = fs::remove_dir_all(root);
}
