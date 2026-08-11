//! Independent acceptance of focused TLA+ proof obligations.
//!
//! This mirrors the primary model check rather than trusting it: the obligation
//! set comes from the pinned profile contract, each obligation's log is read
//! back from an authenticated artifact, its exact TLC argv is reconstructed
//! from the contract, and its terminal frames are re-parsed here. Nothing in
//! this module consults the producer's verdict, its observation frame, or its
//! ordering decisions -- only the bytes it published.
//!
//! Obligations fail fast and in order, so a receipt legitimately carries logs
//! for a prefix of the reviewed set. The rule enforced here makes that prefix
//! meaningful: every obligation before the last present one must have
//! discharged, no obligation after it may have produced a log, and the layer
//! passes only when the prefix covers the whole set.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use crate::{
    contract::profile::{ProofObligationContract, RunnerContract},
    evidence::{
        format::tla::{
            obligation_config_kind, obligation_discharged, obligation_label, obligation_log_kind,
            obligation_observation, obligation_observations, OBLIGATION_METRICS,
        },
        CheckReceipt, ResultBundle,
    },
    verification::{AggregateError, AuthenticatedArtifacts},
};

use super::{
    artifact::has_kind,
    invocation::read_process_log,
    observation::{parse_main_summary, successful_log},
};

pub(super) fn contracted<'a>(
    bundle: &'a ResultBundle,
) -> Result<&'a [ProofObligationContract], AggregateError> {
    Ok(&bundle
        .execution
        .plan
        .contract
        .runners
        .get(&bundle.runner)
        .ok_or_else(|| {
            AggregateError::new(format!("execution plan omitted runner {}", bundle.runner))
        })?
        .obligations)
}

pub(super) fn verify(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    root: &Path,
    producer_repository: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(BTreeMap<String, u64>, bool), AggregateError> {
    let obligations = contracted(bundle)?;
    let mut derived = BTreeMap::new();
    let mut discharged_all = true;
    let mut ended = false;
    for obligation in obligations {
        let kind = obligation_log_kind(&obligation.id);
        if !has_kind(check, &kind)? {
            // A missing log is only legitimate once the sequence has already
            // stopped. An unexplained gap would let a receipt skip an
            // obligation it did not want to run.
            if !ended {
                discharged_all = false;
                ended = true;
            }
            continue;
        }
        if ended {
            return Err(AggregateError::new(format!(
                "TLA receipt logs proof obligation {} after the sequence stopped",
                obligation.id
            )));
        }
        let log = read_process_log(
            bundle,
            check,
            &kind,
            &obligation_label(&obligation.id),
            root,
            producer_repository,
            authenticated,
        )?;
        let (summary, _) = parse_main_summary(Some(&log));
        let discharged = successful_log(&log)
            && summary.as_ref().is_some_and(|summary| {
                obligation_discharged(
                    summary,
                    obligation.minimum_generated_states,
                    obligation.minimum_distinct_states,
                )
            });
        if let Some(summary) = summary.as_ref() {
            derived.extend(obligation_observations(
                &obligation.id,
                summary,
                discharged,
            ));
        }
        if !discharged {
            discharged_all = false;
            ended = true;
        }
    }
    Ok((derived, discharged_all))
}

/// Observation keys a passing receipt must carry for every reviewed
/// obligation. A frontier-exhausted layer is only reachable when all of them
/// discharged, so all of them must be framed.
pub(super) fn expected_observations(contract: &RunnerContract) -> BTreeSet<String> {
    contract
        .obligations
        .iter()
        .flat_map(|obligation| {
            OBLIGATION_METRICS
                .iter()
                .map(move |metric| obligation_observation(&obligation.id, metric))
        })
        .collect()
}

/// Exactly two artifacts per obligation: the configuration TLC read and the log
/// it produced. No checkpoint contract, inventory, or recovery report, because
/// an obligation never has any.
pub(super) fn artifact_kinds(contract: &RunnerContract) -> BTreeSet<String> {
    contract
        .obligations
        .iter()
        .flat_map(|obligation| {
            [
                obligation_log_kind(&obligation.id),
                obligation_config_kind(&obligation.id),
            ]
        })
        .collect()
}

/// Re-checks every obligation's framed observations against its own ratchets.
/// These floors are calibrated per obligation and are deliberately unrelated to
/// the primary configuration's monolith floors.
pub(super) fn floors_cleared(contract: &RunnerContract, observed: &dyn Fn(&str) -> u64) -> bool {
    contract.obligations.iter().all(|obligation| {
        let metric = |name: &str| observed(&obligation_observation(&obligation.id, name));
        metric("frontier_exhausted") == 1
            && metric("search_depth") > 0
            && metric("generated_states") >= obligation.minimum_generated_states
            && metric("distinct_states") >= obligation.minimum_distinct_states
    })
}
