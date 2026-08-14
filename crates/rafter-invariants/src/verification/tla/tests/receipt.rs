//! TLA+ runner receipt artifact-inventory scenarios.

use std::collections::BTreeMap;

use super::required_proof_artifact_kinds;
use crate::contract::profile::{
    ObligationCompletion, ProofObligationContract, RunnerContract, SimulatorCheckContract,
};
use crate::evidence::format::tla::{
    detector_config_kind, detector_log_kind, obligation_config_kind, obligation_log_kind,
    DETECTOR_PROBES,
};

fn contract(obligations: Vec<ProofObligationContract>) -> RunnerContract {
    RunnerContract {
        producer: "rafter-invariants-tla-v16".to_owned(),
        command: vec!["cargo".to_owned()],
        configuration: BTreeMap::new(),
        simulator_checks: BTreeMap::<String, SimulatorCheckContract>::new(),
        obligations,
        minimum_observed_checks: 1,
        require_peak_rss: true,
    }
}

fn obligation(id: &str, config: &str) -> ProofObligationContract {
    ProofObligationContract {
        id: id.to_owned(),
        config: config.to_owned(),
        completion: ObligationCompletion::FrontierExhausted,
        minimum_generated_states: 10,
        minimum_distinct_states: 5,
        soft_timeout: "9m".to_owned(),
        seed: "7".to_owned(),
    }
}

#[test]
fn passing_receipt_requires_two_artifacts_per_detector_probe() {
    let kinds = required_proof_artifact_kinds(false, false, &contract(Vec::new()));
    assert_eq!(kinds.len(), 13 + 2 * DETECTOR_PROBES.len());
    assert!(!kinds.contains("tla-detector-log"));
    for probe in DETECTOR_PROBES {
        assert!(kinds.contains(&detector_log_kind(probe).expect("registered probe")));
        assert!(kinds.contains(&detector_config_kind(probe).expect("registered probe")));
    }
}

#[test]
fn checkpointed_pass_requires_recovery_and_final_inventory_artifacts() {
    let empty = contract(Vec::new());
    let fresh = required_proof_artifact_kinds(true, false, &empty);
    assert!(fresh.contains("tla-checkpoint-recovery-report"));
    assert!(fresh.contains("tla-checkpoint-contract"));
    assert!(fresh.contains("tla-checkpoint-inventory"));
    assert!(!fresh.contains("tla-checkpoint-recovered-contract"));

    let recovered = required_proof_artifact_kinds(true, true, &empty);
    assert!(recovered.contains("tla-checkpoint-recovered-contract"));
    assert!(recovered.contains("tla-checkpoint-recovered-inventory"));
}

/// Each obligation adds exactly its configuration and its log, and adds no
/// checkpoint vocabulary: obligations never checkpoint, so a receipt that
/// carried a checkpoint artifact for one would be describing something the
/// contract does not permit.
#[test]
fn every_obligation_adds_exactly_a_config_and_a_log() {
    let baseline = required_proof_artifact_kinds(false, false, &contract(Vec::new()));
    let with_obligations = required_proof_artifact_kinds(
        false,
        false,
        &contract(vec![
            obligation(
                "joint-quorum-focused-init",
                "RaftJointQuorumFocusedInit.cfg",
            ),
            obligation(
                "joint-quorum-focused-next",
                "RaftJointQuorumFocusedNext.cfg",
            ),
        ]),
    );

    assert_eq!(with_obligations.len(), baseline.len() + 4);
    for id in ["joint-quorum-focused-init", "joint-quorum-focused-next"] {
        assert!(with_obligations.contains(&obligation_log_kind(id)));
        assert!(with_obligations.contains(&obligation_config_kind(id)));
    }
    assert!(!with_obligations.contains("tla-checkpoint-contract"));
}

/// The empty obligation list must be the identity: a profile that declares no
/// obligations produces byte-identical artifact expectations to the vocabulary
/// never having existed, which is what keeps the deterministic PR gate stable.
#[test]
fn an_empty_obligation_list_changes_no_artifact_expectation() {
    for (checkpointed, recovered) in [(false, false), (true, false), (true, true)] {
        assert_eq!(
            required_proof_artifact_kinds(checkpointed, recovered, &contract(Vec::new())),
            required_proof_artifact_kinds(checkpointed, recovered, &contract(vec![])),
        );
    }
}

/// The PR lane is the one that blocks a merge, and its primary genuinely
/// drains. A PR receipt claiming a reporting continuation would be relaxing
/// that gate from inside the receipt, so the policy is read from the pinned
/// profile and a disagreeing receipt is refused outright.
#[test]
fn a_pr_receipt_cannot_claim_a_reporting_continuation() {
    use crate::evidence::PRIMARY_COMPLETION_KEY;

    let (catalog, manifest) = crate::tests::loaded();
    let expected = catalog.required_evidence(&manifest.profiles["pr"]);
    let expected = expected
        .values()
        .flatten()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tla")
        .expect("synthetic TLA bundle");
    let contract = manifest.profiles["pr"].runners["tla"].clone();
    super::validate(&bundle, &expected, &contract).expect("the gating PR receipt validates");

    // Demote the pinned contract and restate the receipt consistently: even a
    // fully self-consistent reporting claim must be refused for this profile.
    let mut demoted_contract = contract.clone();
    demoted_contract.configuration.insert(
        PRIMARY_COMPLETION_KEY.to_owned(),
        "reporting-continuation".to_owned(),
    );
    let mut demoted = bundle.clone();
    demoted.execution.checks[0]
        .tla_continuation
        .as_mut()
        .expect("continuation binding")
        .policy = crate::PrimaryCompletionPolicy::ReportingContinuation;
    assert!(super::validate(&demoted, &expected, &demoted_contract).is_err());

    // A receipt whose declared policy simply disagrees with the pinned one is
    // refused for every profile, not just PR.
    let mut mismatched = bundle;
    mismatched.execution.checks[0]
        .tla_continuation
        .as_mut()
        .expect("continuation binding")
        .policy = crate::PrimaryCompletionPolicy::ReportingContinuation;
    assert!(super::validate(&mismatched, &expected, &contract).is_err());
}

/// Intake sees the receipt and the contract, never the TLC logs -- the
/// aggregate is what rederives a completion from proof artifacts. So this is
/// the layer where a receipt has to agree with itself: a completion claiming a
/// drained frontier beside a binding reporting an elapsed budget is refused
/// here, before anything reads a log, and so is the reverse claim.
#[test]
fn a_receipt_completion_must_agree_with_its_continuation_outcome() {
    use crate::evidence::PRIMARY_COMPLETION_KEY;

    let (catalog, manifest) = crate::tests::loaded();
    let expected = catalog.required_evidence(&manifest.profiles["pr"]);
    let expected = expected
        .values()
        .flatten()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tla")
        .expect("synthetic TLA bundle");
    let mut reporting_contract = manifest.profiles["pr"].runners["tla"].clone();
    reporting_contract.configuration.insert(
        PRIMARY_COMPLETION_KEY.to_owned(),
        "reporting-continuation".to_owned(),
    );
    let mut reporting = bundle.clone();
    reporting.profile = "nightly".to_owned();
    reporting.execution.checks[0]
        .tla_continuation
        .as_mut()
        .expect("continuation binding")
        .policy = crate::PrimaryCompletionPolicy::ReportingContinuation;

    // Drained completion, elapsed outcome: the inversion the variant exists to
    // expose.
    let mut inverted = reporting.clone();
    inverted.execution.checks[0]
        .tla_continuation
        .as_mut()
        .expect("continuation binding")
        .outcome = crate::ContinuationOutcome::BudgetElapsedFrontierOpen;
    assert!(super::validate(&inverted, &expected, &reporting_contract).is_err());

    // Elapsed completion, drained outcome: refused just as flatly.
    let mut reversed = reporting;
    reversed.execution.checks[0].completion = crate::CheckCompletion::BudgetElapsedFrontierOpen;
    assert!(super::validate(&reversed, &expected, &reporting_contract).is_err());

    // And a gating profile may not record the elapsed completion at all, even
    // with a binding that agrees with it.
    let mut gating = bundle;
    gating.execution.checks[0].completion = crate::CheckCompletion::BudgetElapsedFrontierOpen;
    gating.execution.checks[0]
        .tla_continuation
        .as_mut()
        .expect("continuation binding")
        .outcome = crate::ContinuationOutcome::BudgetElapsedFrontierOpen;
    assert!(super::validate(&gating, &expected, &manifest.profiles["pr"].runners["tla"]).is_err());
}

/// A TLA+ receipt that omits the binding entirely cannot be accepted: the
/// field is additive on the wire precisely because only this layer needs it,
/// so this layer is where its absence has to fail closed.
#[test]
fn a_tla_receipt_without_a_continuation_binding_fails_closed() {
    let (catalog, manifest) = crate::tests::loaded();
    let expected = catalog.required_evidence(&manifest.profiles["pr"]);
    let expected = expected
        .values()
        .flatten()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tla")
        .expect("synthetic TLA bundle");
    bundle.execution.checks[0].tla_continuation = None;

    assert!(super::validate(&bundle, &expected, &manifest.profiles["pr"].runners["tla"]).is_err());
}
