//! Sequential, checkpoint-free execution of focused TLA+ proof obligations.
//!
//! # Why obligations run before the primary configuration
//!
//! Obligations are minutes-scale models that must drain their queue outright.
//! Running them first turns the expensive part of the layer into a decision
//! that has already been made: a broken theorem is red within minutes instead
//! of after a five-hour continuation, and the primary configuration then
//! inherits whatever remains of the shared execution window, which is exactly
//! the budget it is supposed to have. Running them afterwards would invert
//! that -- the monolith would consume the window first and the obligations
//! would be squeezed into the finalization reserve, where a timeout is
//! indistinguishable from a refutation.
//!
//! Ordering alone does not finish the job. An obligation is only started when
//! the window still holds its whole calibrated budget; anything less is
//! reported as a budget shortfall and never as an undischarged theorem. A
//! truncated obligation would be killed at a wall it was never given, and the
//! resulting diagnosis would name the model rather than the harness.
//!
//! # Why obligations never checkpoint
//!
//! Each obligation runs from scratch into an ephemeral state directory. It
//! writes no checkpoint, recovers none, and contributes nothing to the primary
//! checkpoint contract or to any cache key. An obligation that cannot exhaust
//! its frontier in one bounded run is not an obligation; it is a second
//! monolith, and it belongs in the primary configuration's continuation. The
//! same rule keeps the primary lineage stable: retuning or replacing an
//! obligation cannot invalidate accumulated primary TLC state.

use std::{collections::BTreeMap, error::Error, path::Path};

use crate::{
    contract::profile::ProofObligationContract,
    evidence::format::tla::{
        obligation_discharged, obligation_label, obligation_log_kind, obligation_observations,
    },
};

use super::{
    super::{
        contract::{parse_timeout, required_configuration},
        process, tla_output,
    },
    budget::ExecutionBudget,
    command::{run_tlc, TlcRequest, TlcState},
    model::{ObligationFailure, ObligationOutcome, ProbeStatus},
};

pub(super) fn run_obligations(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    obligations: &[ProofObligationContract],
    output_dir: &Path,
    budget: ExecutionBudget,
) -> Result<ObligationOutcome, Box<dyn Error>> {
    let mut outcome = ObligationOutcome::default();
    if obligations.is_empty() {
        return Ok(outcome);
    }
    outcome.status = ProbeStatus::Passed;
    for obligation in obligations {
        process::ensure_execution_deadline(
            profile,
            "tla",
            &format!("TLA proof obligation {}", obligation.id),
        )?;
        // The full calibrated cap or nothing. An obligation started under a
        // truncated clock would be killed at the wall and diagnosed as a
        // frontier it failed to exhaust, which is the one confusion the
        // ordering above exists to prevent.
        let cap = parse_timeout(&obligation.soft_timeout)?;
        let Some(timeout) = budget.whole_phase_timeout(cap) else {
            return Ok(underfunded(outcome, obligation));
        };
        let run = run_tlc(TlcRequest {
            profile,
            source_ref,
            config: &obligation.config,
            module: "Raft.tla",
            // Workers, heap, and fingerprint memory are inherited: an
            // obligation is a different model on the same machine, not a
            // different machine budget.
            workers: required_configuration(configuration, "workers")?,
            seed: &obligation.seed,
            timeout,
            output_dir,
            label: &obligation_label(&obligation.id),
            artifact_kind: &obligation_log_kind(&obligation.id),
            max_heap: configuration.get("max_heap").map(String::as_str),
            fp_mem: configuration.get("fp_mem").map(String::as_str),
            state: TlcState::Ephemeral,
        })?;
        outcome.artifacts.push(run.artifact);
        outcome.peak_rss_kib = outcome.peak_rss_kib.max(run.output.peak_rss_kib);
        outcome.duration_ms = outcome
            .duration_ms
            .saturating_add(process::duration_ms(run.output.duration));
        let summary = tla_output::parse(&run.output.stdout).ok();
        let discharged = !run.output.timed_out
            && run.output.status.success()
            && summary.as_ref().is_some_and(|summary| {
                obligation_discharged(
                    summary,
                    obligation.minimum_generated_states,
                    obligation.minimum_distinct_states,
                )
            });
        if let Some(summary) = summary.as_ref() {
            outcome.observations.extend(obligation_observations(
                &obligation.id,
                summary,
                discharged,
            ));
        }
        if !discharged {
            outcome.status = ProbeStatus::Failed;
            outcome.failure = Some(undischarged(
                obligation,
                run.output.timed_out,
                summary.as_ref(),
            ));
            return Ok(outcome);
        }
    }
    Ok(outcome)
}

fn underfunded(
    mut outcome: ObligationOutcome,
    obligation: &ProofObligationContract,
) -> ObligationOutcome {
    outcome.status = ProbeStatus::Failed;
    outcome.failure = Some(ObligationFailure::Underfunded(format!(
        "proof obligation {} was not started: the shared execution window no longer holds its {} budget",
        obligation.id, obligation.soft_timeout
    )));
    outcome
}

/// An obligation that ran with the whole budget it was promised and still did
/// not discharge. Only reachable after `whole_phase_timeout` granted the full
/// cap, which is what makes the wall in the timed-out diagnosis below a real
/// statement about the model.
pub(super) fn undischarged(
    obligation: &ProofObligationContract,
    timed_out: bool,
    summary: Option<&tla_output::TlcSummary>,
) -> ObligationFailure {
    ObligationFailure::Undischarged(diagnosis(obligation, timed_out, summary))
}

fn diagnosis(
    obligation: &ProofObligationContract,
    timed_out: bool,
    summary: Option<&tla_output::TlcSummary>,
) -> String {
    let id = &obligation.id;
    if timed_out {
        return format!(
            "proof obligation {id} did not exhaust its frontier within {}",
            obligation.soft_timeout
        );
    }
    let Some(summary) = summary else {
        return format!("proof obligation {id} produced no readable TLC summary");
    };
    if let Some(invariant) = &summary.violated_invariant {
        return format!("proof obligation {id} refuted {invariant}");
    }
    if summary.states_left != 0 || !summary.completed_without_error || !summary.process_finished {
        return format!("proof obligation {id} exited without draining its queue");
    }
    format!(
        "proof obligation {id} generated {}/{} and distinct {}/{} states",
        summary.generated_states,
        obligation.minimum_generated_states,
        summary.distinct_states,
        obligation.minimum_distinct_states
    )
}
