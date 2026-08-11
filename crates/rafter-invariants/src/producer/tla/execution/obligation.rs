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
    model::{ObligationOutcome, ProbeStatus},
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
        let Some(timeout) = budget.phase_timeout(parse_timeout(&obligation.soft_timeout)?) else {
            return Ok(exhausted_budget(outcome, obligation));
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
            outcome
                .observations
                .extend(obligation_observations(&obligation.id, summary, discharged));
        }
        if !discharged {
            outcome.status = ProbeStatus::Failed;
            outcome.failure = Some(diagnosis(obligation, run.output.timed_out, summary.as_ref()));
            return Ok(outcome);
        }
    }
    Ok(outcome)
}

fn exhausted_budget(
    mut outcome: ObligationOutcome,
    obligation: &ProofObligationContract,
) -> ObligationOutcome {
    outcome.status = ProbeStatus::Failed;
    outcome.failure = Some(format!(
        "proof obligation {} had no execution budget left",
        obligation.id
    ));
    outcome
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
