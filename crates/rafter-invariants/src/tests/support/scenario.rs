//! Loading the reviewed registry and driving one aggregation over it.
//!
//! The registry and profile manifest are read from the workspace rather than
//! restated here, and the TLA+ continuation and obligation frames follow
//! whatever the manifest pins, so a recalibrated contract reaches the suite
//! without a fixture edit.

use std::path::{Path, PathBuf};

use super::receipts::plan_receipt;
use crate::*;

pub(super) fn workspace_file(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

pub(crate) fn loaded() -> (Catalog, ProfileManifest) {
    let catalog = Catalog::load(&workspace_file("verification/raft-invariants.yaml"))
        .expect("registry loads");
    let manifest =
        ProfileManifest::load(&workspace_file("verification/raft-invariant-profiles.json"))
            .expect("profile manifest loads");
    (catalog, manifest)
}

/// Proof obligations the profile actually declares.
///
/// Synthetic TLA+ evidence is built from whatever the reviewed manifest asks
/// for rather than from a list frozen into the fixtures, so wiring a new
/// obligation cannot silently leave the fixtures modelling the old contract.
pub(crate) fn tla_obligations<'a>(
    manifest: &'a ProfileManifest,
    profile: &str,
) -> &'a [ProofObligationContract] {
    manifest.profiles[profile]
        .runners
        .get("tla")
        .map_or(&[], |runner| runner.obligations.as_slice())
}

/// The continuation policy the profile pins, decoded the way the runners do.
pub(crate) fn tla_policy(manifest: &ProfileManifest, profile: &str) -> PrimaryCompletionPolicy {
    PrimaryCompletionPolicy::parse(
        &manifest.profiles[profile].runners["tla"].configuration
            [crate::evidence::PRIMARY_COMPLETION_KEY],
    )
    .expect("profile pins a reviewed primary_completion policy")
}

/// Synthetic continuation binding for the profile's pinned policy.
///
/// A gating profile models a drained monolith; a reporting profile models the
/// shape its lane actually produces -- a continuation that spent its budget
/// with the frontier still open. Both are honest receipts for their contract.
pub(crate) fn synthetic_continuation_binding(
    manifest: &ProfileManifest,
    profile: &str,
) -> TlaContinuationBinding {
    let policy = tla_policy(manifest, profile);
    TlaContinuationBinding {
        policy,
        outcome: if policy.gates() {
            ContinuationOutcome::FrontierExhausted
        } else {
            ContinuationOutcome::BudgetElapsedFrontierOpen
        },
    }
}

/// The terminal frame an obligation reports when it discharges.
///
/// The queue is drained and both calibrated ratchets are met exactly. That is
/// the weakest run the contract accepts, so it is the honest fixture: anything
/// larger would let a floor regression pass unnoticed.
pub(crate) fn synthetic_obligation_summary(
    obligation: &ProofObligationContract,
) -> crate::producer::tla_output::TlcSummary {
    crate::producer::tla_output::TlcSummary {
        generated_states: obligation.minimum_generated_states,
        distinct_states: obligation.minimum_distinct_states,
        states_left: 0,
        search_depth: 1,
        completed_without_error: true,
        process_finished: true,
        violated_invariant: None,
    }
}

pub(crate) fn aggregate_unverified(
    catalog: &Catalog,
    manifest: &ProfileManifest,
    profile: &str,
    source_ref: &str,
    bundles: &[ResultBundle],
) -> Result<VerdictReport, crate::verification::AggregateError> {
    let plan = plan_receipt(manifest, profile);
    let request = crate::verification::VerificationRequest::new(
        catalog,
        manifest,
        &plan,
        source_ref,
        Path::new("."),
        crate::verification::VerificationContext::ProducingJob,
    );
    let intake = crate::verification::verify_receipts_for_test(request, bundles, Vec::new())?;
    crate::verdict::reduce(catalog, manifest, &intake)
}

pub(in crate::tests) fn aggregate(
    catalog: &Catalog,
    manifest: &ProfileManifest,
    profile: &str,
    source_ref: &str,
    bundles: &[ResultBundle],
) -> Result<VerdictReport, crate::verification::AggregateError> {
    aggregate_unverified(catalog, manifest, profile, source_ref, bundles)
}

pub(in crate::tests) fn verify_layer_bundle(
    catalog: &Catalog,
    manifest: &ProfileManifest,
    profile: &str,
    layer: &str,
    bundle: &ResultBundle,
) -> Result<(), crate::verification::AggregateError> {
    let plan = plan_receipt(manifest, profile);
    let request = crate::verification::VerificationRequest::new(
        catalog,
        manifest,
        &plan,
        &bundle.source_ref,
        Path::new("."),
        crate::verification::VerificationContext::ProducingJob,
    );
    let intake = crate::verification::verify_receipts_for_test(
        request,
        std::slice::from_ref(bundle),
        Vec::new(),
    )?;
    crate::verification::require_passing_layer(request, layer, &intake)
}

pub(in crate::tests) fn assert_only_parent_red(report: &VerdictReport, invariant_id: &str) {
    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 43);
    assert_eq!(report.summary.red, 1);
    let red = report
        .invariants
        .iter()
        .filter(|verdict| verdict.status == VerdictStatus::Red)
        .map(|verdict| verdict.invariant_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(red, [invariant_id]);
}
