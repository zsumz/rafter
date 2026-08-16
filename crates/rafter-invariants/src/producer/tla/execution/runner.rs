//! TLA+ qualification phases and main model-check orchestration.

use std::{collections::BTreeMap, error::Error, path::Path, time::Duration};

use crate::{contract::profile::ProofObligationContract, evidence::ArtifactRef};

use super::{
    super::contract::required_configuration,
    budget::{probe_timeout, ExecutionBudget},
    command::{run_tlc, TlcRequest, TlcState},
    model::{DetectorProbes, ObligationOutcome, ProbeStatus, TlaExecution, TlcRun},
    obligation::run_obligations,
    outcome::{
        checkpoint_failure, complete_main_execution, detector_failure, main_budget_failure,
        obligation_failure, prepare_checkpoint, trace_budget_failure, trace_failure,
        MainCompletion,
    },
    probes::{run_detector_probes, run_trace_probe, trace_succeeded},
};

#[derive(Clone, Copy)]
pub(in crate::producer::tla) struct ExecutionRequest<'a> {
    pub(in crate::producer::tla) profile: &'a str,
    pub(in crate::producer::tla) source_ref: &'a str,
    pub(in crate::producer::tla) config: &'a str,
    pub(in crate::producer::tla) configuration: &'a BTreeMap<String, String>,
    pub(in crate::producer::tla) obligations: &'a [ProofObligationContract],
    pub(in crate::producer::tla) timeout: Duration,
    pub(in crate::producer::tla) output_dir: &'a Path,
}

pub(in crate::producer::tla) fn execute(
    request: &ExecutionRequest<'_>,
    mut artifacts: Vec<ArtifactRef>,
) -> Result<TlaExecution, Box<dyn Error>> {
    let ExecutionRequest {
        profile,
        source_ref,
        configuration,
        obligations,
        output_dir,
        ..
    } = *request;
    let budget = ExecutionBudget::from_configuration(profile, configuration)?;
    let Some(trace_timeout) = budget.phase_timeout(probe_timeout(profile)) else {
        return Ok(trace_budget_failure(artifacts));
    };
    let trace = run_trace_probe(
        profile,
        source_ref,
        configuration,
        output_dir,
        trace_timeout,
    )?;
    artifacts.push(trace.artifact.clone());
    if !trace_succeeded(&trace) {
        return Ok(trace_failure(&trace, artifacts));
    }
    let mut detectors =
        run_detector_probes(profile, source_ref, configuration, output_dir, &budget)?;
    artifacts.append(&mut detectors.artifacts);
    if !detectors.succeeded {
        return Ok(detector_failure(&trace, detectors, artifacts));
    }
    // Obligations run after harness qualification and before the primary
    // configuration: the trace and detector probes prove the harness can still
    // see a counterexample, and only then is an obligation's silence worth
    // anything. See `obligation.rs` for the ordering and checkpoint rationale.
    let mut obligation_outcome = run_obligations(
        profile,
        source_ref,
        configuration,
        obligations,
        output_dir,
        budget,
    )?;
    artifacts.append(&mut obligation_outcome.artifacts);
    if obligation_outcome.status == ProbeStatus::Failed {
        return Ok(obligation_failure(
            &trace,
            detectors,
            obligation_outcome,
            artifacts,
        ));
    }
    run_primary_continuation(
        &PrimaryContinuation {
            request,
            budget,
            trace: &trace,
        },
        detectors,
        obligation_outcome,
        artifacts,
    )
}

/// The primary configuration's own phase: checkpoint preparation, the
/// continuation run, and its terminal classification.
///
/// Split from `execute` at the ownership seam that matters -- everything above
/// decides whether this phase is entitled to run at all, and everything here
/// consumes the qualification results rather than producing them.
#[derive(Clone, Copy)]
struct PrimaryContinuation<'a> {
    request: &'a ExecutionRequest<'a>,
    budget: ExecutionBudget,
    trace: &'a TlcRun,
}

fn run_primary_continuation(
    context: &PrimaryContinuation<'_>,
    detectors: DetectorProbes,
    obligation_outcome: ObligationOutcome,
    mut artifacts: Vec<ArtifactRef>,
) -> Result<TlaExecution, Box<dyn Error>> {
    let PrimaryContinuation {
        request,
        budget,
        trace,
    } = *context;
    let ExecutionRequest {
        profile,
        source_ref,
        config,
        configuration,
        timeout,
        output_dir,
        ..
    } = *request;
    let mut checkpoint = prepare_checkpoint(
        profile,
        source_ref,
        configuration,
        &artifacts,
        output_dir,
        budget,
    )?;
    let checkpoint_report = checkpoint
        .as_ref()
        .map(|preparation| preparation.report.clone());
    if let Some(error) = checkpoint
        .as_ref()
        .and_then(|preparation| preparation.error.clone())
    {
        let Some(preparation) = checkpoint.take() else {
            return Err("checkpoint error was reported without checkpoint state".into());
        };
        artifacts.extend(preparation.finish(output_dir, budget.total_deadline)?);
        return Ok(checkpoint_failure(
            trace,
            detectors,
            obligation_outcome,
            artifacts,
            checkpoint_report,
            error,
        ));
    }
    let Some(main_timeout) = budget.phase_timeout(timeout) else {
        if let Some(preparation) = checkpoint {
            artifacts.extend(preparation.finish(output_dir, budget.total_deadline)?);
        }
        return Ok(main_budget_failure(
            trace,
            detectors,
            obligation_outcome,
            artifacts,
            checkpoint_report,
        ));
    };
    let state = if let Some(preparation) = checkpoint.as_ref() {
        TlcState::Checkpoint {
            state_dir: preparation
                .state_handle
                .as_ref()
                .ok_or("compatible checkpoint preparation omitted state handle")?,
            recover_from: preparation.recover_handle.as_ref(),
            checkpoint_minutes: required_configuration(configuration, "checkpoint_minutes")?,
        }
    } else {
        TlcState::Ephemeral
    };
    let main = run_tlc(TlcRequest {
        profile,
        source_ref,
        config,
        module: "Raft.tla",
        workers: required_configuration(configuration, "workers")?,
        seed: required_configuration(configuration, "seed")?,
        timeout: main_timeout,
        output_dir,
        label: "model-check",
        artifact_kind: "tla-log",
        max_heap: configuration.get("max_heap").map(String::as_str),
        fp_mem: configuration.get("fp_mem").map(String::as_str),
        state,
    })?;
    complete_main_execution(
        MainCompletion {
            trace,
            detectors,
            obligations: obligation_outcome,
            artifacts,
            checkpoint,
            checkpoint_report,
            output_dir,
            total_deadline: budget.total_deadline,
        },
        main,
    )
}
