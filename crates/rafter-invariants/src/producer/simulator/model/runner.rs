//! Simulator run-plan orchestration, event collection, and canonical scheduling identities.

use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path};

use serde_json::Value;

use super::{
    build::build,
    types::{ModelRun, SimulatorExecution},
};
use crate::producer::{artifact, process};

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

pub(super) fn completed_successfully(output: &process::ProcessOutput) -> bool {
    output.status.success() && !output.timed_out
}

fn record_model_run(
    profile: &str,
    source_ref: &str,
    label: &str,
    output_dir: &Path,
    output: &process::ProcessOutput,
    execution: &mut SimulatorExecution,
) -> Result<(), Box<dyn Error>> {
    execution.runtime_peak_rss_kib = execution.runtime_peak_rss_kib.max(output.peak_rss_kib);
    execution.duration_ms = execution
        .duration_ms
        .saturating_add(process::duration_ms(output.duration));
    execution.processes_succeeded &= completed_successfully(output);
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    execution.artifacts.push(artifact::write(
        output_dir,
        Path::new(&format!("{profile}-simulator/{source_prefix}/{label}.log")),
        "simulator-log",
        &process::combined_log(label, output)?,
    )?);
    collect_events(profile, &output.stdout, &mut execution.events)?;
    Ok(())
}

pub(in crate::producer) fn execute(
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<SimulatorExecution, Box<dyn Error>> {
    let build = build(profile, source_ref, output_dir)?;
    let binary = build.binary;
    let binary_handle = build.binary_handle;
    let target_guard = build.target_dir;
    let mut artifacts = build.artifacts;
    binary_handle.verify_path_binding()?;
    target_guard.verify_path_binding()?;
    let binary_artifact = artifact::capture(
        output_dir,
        Path::new(&format!("{profile}-simulator/inputs")),
        &binary,
        "simulator-binary",
    )?;
    artifacts.push(binary_artifact);
    let execution = SimulatorExecution {
        events: BTreeMap::new(),
        artifacts,
        runtime_peak_rss_kib: 0,
        build_peak_rss_kib: build.peak_rss_kib,
        duration_ms: 0,
        build_duration_ms: build.duration_ms,
        processes_succeeded: true,
        harness_errors: Vec::new(),
    };
    let program = binary
        .to_str()
        .ok_or("simulator binary path is not UTF-8")?;
    Ok(execute_plan(
        profile,
        source_ref,
        output_dir,
        execution_plan(profile, source_ref)?,
        execution,
        |run| {
            binary_handle.verify_path_binding()?;
            target_guard.verify_path_binding()?;
            process::timed_for(
                process::ProcessKind::SimulatorExecution,
                program,
                &run.arguments,
                &process::base_environment(),
                Path::new("."),
            )
        },
    ))
}

pub(super) fn execute_plan<F>(
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
    runs: Vec<ModelRun>,
    mut execution: SimulatorExecution,
    mut invoke: F,
) -> SimulatorExecution
where
    F: FnMut(&ModelRun) -> Result<process::ProcessOutput, Box<dyn Error>>,
{
    for run in runs {
        let output = match invoke(&run) {
            Ok(output) => output,
            Err(error) => {
                execution.processes_succeeded = false;
                execution.harness_errors.push(format!(
                    "simulator invocation {} failed before producing a receipt: {error}",
                    run.label
                ));
                break;
            }
        };
        if let Err(error) = record_model_run(
            profile,
            source_ref,
            &run.label,
            output_dir,
            &output,
            &mut execution,
        ) {
            execution.processes_succeeded = false;
            execution.harness_errors.push(format!(
                "simulator invocation {} could not be recorded: {error}",
                run.label
            ));
            break;
        }
        if !completed_successfully(&output) {
            execution.harness_errors.push(format!(
                "simulator invocation {} did not complete successfully",
                run.label
            ));
            break;
        }
    }
    execution
}

pub(super) fn execution_plan(
    profile: &str,
    source_ref: &str,
) -> Result<Vec<ModelRun>, Box<dyn Error>> {
    let runs = match profile {
        "pr" => vec![model_run("fast", None), model_run("raft-soak", None)],
        // Which model profile a scheduled lane invokes is contract-owned;
        // weekly currently shares nightly's, so it must not be spelled here.
        "nightly" | "weekly" => {
            let model_profile = crate::contract::profile::scheduled_model_profile(profile)
                .ok_or_else(|| format!("unsupported simulator profile {profile}"))?;
            vec![model_run(
                model_profile,
                expected_scheduled_seeds(profile, source_ref),
            )]
        }
        _ => return Err(format!("unsupported simulator profile {profile}").into()),
    };
    Ok(runs)
}

pub(super) fn model_run(profile: &str, seeds: Option<String>) -> ModelRun {
    let mut arguments = vec![OsString::from("--profile"), OsString::from(profile)];
    if let Some(seeds) = seeds {
        arguments.extend([OsString::from("--seed"), OsString::from(seeds)]);
    }
    ModelRun {
        label: profile.to_owned(),
        arguments,
    }
}

pub(crate) fn expected_scheduled_seeds_with_count(
    profile: &str,
    source_ref: &str,
    count: usize,
) -> Option<String> {
    crate::contract::profile::scheduled_simulator_seeds(profile, source_ref, count)
}

pub(crate) fn expected_scheduled_seeds(profile: &str, source_ref: &str) -> Option<String> {
    // Mirrors `seed_count` in each lane's reviewed simulator configuration.
    // Weekly ran 10 seeds under its deep profile; it runs nightly's 6 while
    // the deep bounds await a >=32GB runner.
    let count = match profile {
        "nightly" | "weekly" => 6,
        _ => return None,
    };
    expected_scheduled_seeds_with_count(profile, source_ref, count)
}

pub(super) fn collect_events(
    profile: &str,
    stdout: &[u8],
    events: &mut BTreeMap<String, Vec<Value>>,
) -> Result<(), Box<dyn Error>> {
    for source in String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|line| line.strip_prefix(EVENT_PREFIX))
    {
        let event = serde_json::from_str::<Value>(source)?;
        let check_id = event["check_id"]
            .as_str()
            .ok_or("simulator event omitted check_id")?;
        events
            .entry(check_id.to_owned())
            .or_default()
            .push(event.clone());
        if let Some(canonical) = canonical_check_id(profile, check_id) {
            let mut canonical_event = event;
            canonical_event["check_id"] = Value::String(canonical.clone());
            events.entry(canonical).or_default().push(canonical_event);
        }
    }
    Ok(())
}

pub(crate) fn canonical_check_id(profile: &str, check_id: &str) -> Option<String> {
    crate::contract::profile::canonical_simulator_check_id(profile, check_id)
}
