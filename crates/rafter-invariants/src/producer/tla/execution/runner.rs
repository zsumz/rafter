//! TLA+ qualification phases and main model-check orchestration.

use std::{collections::BTreeMap, error::Error, path::Path, time::Duration};

use crate::evidence::ArtifactRef;

use super::{
    super::contract::required_configuration,
    budget::ExecutionBudget,
    command::{run_tlc, TlcRequest, TlcState},
    model::TlaExecution,
    outcome::{
        checkpoint_failure, complete_main_execution, detector_failure, main_budget_failure,
        prepare_checkpoint, trace_budget_failure, trace_failure, MainCompletion,
    },
    probes::{run_detector_probes, run_trace_probe, trace_succeeded, PROBE_TIMEOUT},
};

pub(in crate::producer::tla) fn execute(
    profile: &str,
    source_ref: &str,
    config: &str,
    configuration: &BTreeMap<String, String>,
    timeout: Duration,
    output_dir: &Path,
    mut artifacts: Vec<ArtifactRef>,
) -> Result<TlaExecution, Box<dyn Error>> {
    let budget = ExecutionBudget::from_configuration(profile, configuration)?;
    let Some(trace_timeout) = budget.phase_timeout(PROBE_TIMEOUT) else {
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
            &trace,
            detectors,
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
            &trace,
            detectors,
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
            trace: &trace,
            detectors,
            artifacts,
            checkpoint,
            checkpoint_report,
            output_dir,
            total_deadline: budget.total_deadline,
        },
        main,
    )
}
