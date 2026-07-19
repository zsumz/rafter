use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
    path::{Path, PathBuf},
    time::Instant,
};

use serde_json::Value;

use crate::{
    execution::filesystem::{
        self as producer_fs, HeldDirectory, HeldFile, OperationDeadline, TREE_LIMITS,
    },
    ArtifactRef,
};

use super::{artifact, process};

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

pub(crate) struct SimulatorExecution {
    pub events: BTreeMap<String, Vec<Value>>,
    pub artifacts: Vec<ArtifactRef>,
    pub runtime_peak_rss_kib: u64,
    pub build_peak_rss_kib: u64,
    pub duration_ms: u64,
    pub build_duration_ms: u64,
    pub processes_succeeded: bool,
    pub harness_errors: Vec<String>,
}

struct SimulatorBuild {
    binary: PathBuf,
    binary_handle: HeldFile,
    target_dir: HeldDirectory,
    artifacts: Vec<ArtifactRef>,
    peak_rss_kib: u64,
    duration_ms: u64,
}

fn completed_successfully(output: &process::ProcessOutput) -> bool {
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

#[derive(Debug, Eq, PartialEq)]
struct ModelRun {
    label: String,
    arguments: Vec<OsString>,
}

pub(super) fn execute(
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

fn execute_plan<F>(
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

#[cfg(all(test, unix))]
pub(crate) struct SimulatorFixtureInvocation<'a> {
    pub label: &'a str,
    pub program: &'a str,
    pub arguments: &'a [OsString],
    pub environment: &'a BTreeMap<String, String>,
    pub current_dir: &'a Path,
    pub output_dir: &'a Path,
}

#[cfg(all(test, unix))]
pub(super) fn timed_out_zero_exit_fixture(
    profile: &str,
    source_ref: &str,
    stdout: &str,
    output_dir: &Path,
) -> Result<(SimulatorExecution, process::LabeledProcess), Box<dyn Error>> {
    const SCRIPT: &str = r#"trap 'exit 0' TERM
printf '%s\n' 'RAFTER_FIXTURE_READY'
printf '%s' "$1"
while :; do
    sleep 1
done
"#;
    timed_out_zero_exit_fixture_at(
        profile,
        source_ref,
        &SimulatorFixtureInvocation {
            label: "fast",
            program: "/bin/sh",
            arguments: &[
                OsString::from("-c"),
                OsString::from(SCRIPT),
                OsString::from("simulator-timeout-fixture"),
                OsString::from(stdout),
            ],
            environment: &process::base_environment(),
            current_dir: Path::new("."),
            output_dir,
        },
    )
}

#[cfg(all(test, unix))]
pub(crate) fn timed_out_zero_exit_fixture_at(
    profile: &str,
    source_ref: &str,
    invocation: &SimulatorFixtureInvocation<'_>,
) -> Result<(SimulatorExecution, process::LabeledProcess), Box<dyn Error>> {
    let output = process::timed_with_timeout(
        invocation.program,
        invocation.arguments,
        invocation.environment,
        invocation.current_dir,
        std::time::Duration::from_secs(1),
    )?;
    if !output.status.success() || !output.timed_out {
        return Err(format!(
            "simulator timeout fixture expected a timed-out zero exit, got {:?} (timed_out={})",
            output.status.code(),
            output.timed_out
        )
        .into());
    }
    if !output.stdout.starts_with(b"RAFTER_FIXTURE_READY\n") {
        return Err(
            "simulator timeout fixture was terminated before installing its TERM trap".into(),
        );
    }
    let receipt = process::combined_log(invocation.label, &output)?;
    let mut parsed = process::parse_combined_processes(&String::from_utf8(receipt)?)?;
    let [observed] = parsed.as_mut_slice() else {
        return Err("simulator timeout fixture emitted an invalid process receipt".into());
    };
    let observed = observed.clone();
    let execution = SimulatorExecution {
        events: BTreeMap::new(),
        artifacts: Vec::new(),
        runtime_peak_rss_kib: 0,
        build_peak_rss_kib: 0,
        duration_ms: 0,
        build_duration_ms: 0,
        processes_succeeded: true,
        harness_errors: Vec::new(),
    };
    let run = ModelRun {
        label: invocation.label.to_owned(),
        arguments: invocation.arguments.to_vec(),
    };
    let mut output = Some(output);
    let execution = execute_plan(
        profile,
        source_ref,
        invocation.output_dir,
        vec![run],
        execution,
        |_| {
            output
                .take()
                .ok_or_else(|| "timeout fixture invoked more than once".into())
        },
    );
    Ok((execution, observed))
}

#[cfg(all(test, unix))]
pub(crate) fn later_launch_error_fixture_at(
    profile: &str,
    source_ref: &str,
    invocation: &SimulatorFixtureInvocation<'_>,
) -> SimulatorExecution {
    let execution = SimulatorExecution {
        events: BTreeMap::new(),
        artifacts: Vec::new(),
        runtime_peak_rss_kib: 0,
        build_peak_rss_kib: 0,
        duration_ms: 0,
        build_duration_ms: 0,
        processes_succeeded: true,
        harness_errors: Vec::new(),
    };
    let runs = vec![model_run("fast", None), model_run("raft-soak", None)];
    let mut first = true;
    execute_plan(
        profile,
        source_ref,
        invocation.output_dir,
        runs,
        execution,
        |_| {
            if first {
                first = false;
                process::timed_with_timeout(
                    invocation.program,
                    invocation.arguments,
                    invocation.environment,
                    invocation.current_dir,
                    std::time::Duration::from_secs(5),
                )
            } else {
                Err("injected raft-soak launch failure".into())
            }
        },
    )
}

#[cfg(all(test, unix))]
pub(super) fn later_launch_error_fixture(
    profile: &str,
    source_ref: &str,
    stdout: &str,
    output_dir: &Path,
) -> SimulatorExecution {
    let execution = SimulatorExecution {
        events: BTreeMap::new(),
        artifacts: Vec::new(),
        runtime_peak_rss_kib: 0,
        build_peak_rss_kib: 0,
        duration_ms: 0,
        build_duration_ms: 0,
        processes_succeeded: true,
        harness_errors: Vec::new(),
    };
    let runs = vec![model_run("fast", None), model_run("raft-soak", None)];
    execute_plan(profile, source_ref, output_dir, runs, execution, |run| {
        if run.label == "fast" {
            process::timed_with_timeout(
                "/bin/sh",
                &[
                    OsString::from("-c"),
                    OsString::from("printf '%s' \"$1\""),
                    OsString::from("simulator-first-run"),
                    OsString::from(stdout),
                ],
                &process::base_environment(),
                Path::new("."),
                std::time::Duration::from_secs(5),
            )
        } else {
            process::timed_with_timeout(
                "/definitely/missing/rafter-model-check-fast",
                &run.arguments,
                &process::base_environment(),
                Path::new("."),
                std::time::Duration::from_secs(5),
            )
        }
    })
}

fn execution_plan(profile: &str, source_ref: &str) -> Result<Vec<ModelRun>, Box<dyn Error>> {
    let runs = match profile {
        "pr" => vec![model_run("fast", None), model_run("raft-soak", None)],
        "nightly" => vec![model_run(
            "raft-nightly",
            expected_scheduled_seeds(profile, source_ref),
        )],
        "weekly" => vec![model_run(
            "raft-weekly",
            expected_scheduled_seeds(profile, source_ref),
        )],
        _ => return Err(format!("unsupported simulator profile {profile}").into()),
    };
    Ok(runs)
}

fn model_run(profile: &str, seeds: Option<String>) -> ModelRun {
    let mut arguments = vec![OsString::from("--profile"), OsString::from(profile)];
    if let Some(seeds) = seeds {
        arguments.extend([OsString::from("--seed"), OsString::from(seeds)]);
    }
    ModelRun {
        label: profile.to_owned(),
        arguments,
    }
}

fn source_derived_seeds(profile: &str, source_ref: &str, count: usize) -> String {
    (0..count)
        .map(|index| {
            let value = artifact::deterministic_u64(
                "scheduled-simulator-seed-v1",
                &format!("{profile}\0{source_ref}\0{index}"),
            );
            format!("0x{value:x}")
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn expected_scheduled_seeds_with_count(
    profile: &str,
    source_ref: &str,
    count: usize,
) -> Option<String> {
    matches!(profile, "nightly" | "weekly")
        .then(|| source_derived_seeds(profile, source_ref, count))
}

pub(crate) fn expected_scheduled_seeds(profile: &str, source_ref: &str) -> Option<String> {
    let count = match profile {
        "nightly" => 6,
        "weekly" => 10,
        _ => return None,
    };
    expected_scheduled_seeds_with_count(profile, source_ref, count)
}

fn build(
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<SimulatorBuild, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let target_dir = Path::new("target/rafter-invariants/simulator-build")
        .join(source_prefix)
        .join(profile);
    let (execution_deadline, _) = process::active_layer_deadlines(profile, "simulator")?;
    let target_guard = reset_simulator_build_scratch(&target_dir, execution_deadline)?;
    target_guard.verify_path_binding()?;
    let mut environment = process::base_environment();
    environment.insert(
        "CARGO_TARGET_DIR".to_owned(),
        target_guard.external_path().to_string_lossy().into_owned(),
    );
    let arguments = [
        "build".into(),
        "--release".into(),
        "--locked".into(),
        "-p".into(),
        "rafter-sim".into(),
        "--bin".into(),
        "rafter-model-check-fast".into(),
        "--message-format=json-render-diagnostics".into(),
    ];
    target_guard.verify_path_binding()?;
    let output = process::timed_for(
        process::ProcessKind::Compile,
        "cargo",
        &arguments,
        &environment,
        Path::new("."),
    )?;
    let log = artifact::write(
        output_dir,
        Path::new(&format!("{profile}-simulator/{source_prefix}/compile.log")),
        "compile-log",
        &process::combined_log("simulator compile", &output)?,
    )?;
    if !completed_successfully(&output) {
        return Err("simulator release build failed".into());
    }
    let binary = executable_from_messages(&output.stdout)?;
    let binary_handle = producer_fs::hold_file(&binary)?;
    binary_handle.verify_path_binding()?;
    Ok(SimulatorBuild {
        binary,
        binary_handle,
        target_dir: target_guard,
        artifacts: vec![log],
        peak_rss_kib: output.peak_rss_kib,
        duration_ms: process::duration_ms(output.duration),
    })
}

pub(super) fn reset_simulator_build_scratch(
    path: &Path,
    deadline: Instant,
) -> Result<HeldDirectory, Box<dyn Error>> {
    HeldDirectory::replace_tree(
        path,
        TREE_LIMITS,
        OperationDeadline::at(deadline, "simulator build scratch cleanup"),
    )
}

fn executable_from_messages(bytes: &[u8]) -> Result<PathBuf, Box<dyn Error>> {
    let mut executables = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if message["reason"] == "compiler-artifact"
            && message["target"]["name"] == "rafter-model-check-fast"
            && message["target"]["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind == "bin"))
        {
            if message["fresh"] == true {
                return Err("fresh cached simulator binary is forbidden".into());
            }
            if let Some(executable) = message["executable"].as_str() {
                executables.push(PathBuf::from(executable));
            }
        }
    }
    if executables.len() != 1 {
        return Err(format!(
            "expected one simulator executable, found {}",
            executables.len()
        )
        .into());
    }
    let executable = executables.remove(0);
    if !executable.is_absolute() {
        return Err("Cargo emitted a non-absolute simulator executable".into());
    }
    Ok(executable)
}

fn collect_events(
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
            events.entry(canonical).or_default().push(event);
        }
    }
    Ok(())
}

pub(crate) fn canonical_check_id(profile: &str, check_id: &str) -> Option<String> {
    let suffix = match profile {
        "nightly" => "nightly",
        "weekly" => "weekly",
        _ => return None,
    };
    let scheduled_soak = format!("raft-{suffix}-soak");
    if let Some(rest) = check_id.strip_prefix(&scheduled_soak) {
        return Some(format!("raft-soak{rest}"));
    }
    check_id
        .strip_suffix(&format!("-{suffix}"))
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::{canonical_check_id, execution_plan};

    #[test]
    fn scheduled_plans_use_stable_source_derived_seed_counts() {
        let first = execution_plan("nightly", "abc123").expect("nightly plan");
        let second = execution_plan("nightly", "abc123").expect("nightly plan");
        assert_eq!(first, second);
        assert_eq!(first.len(), 1);
        let seeds = first[0].arguments[3].to_string_lossy();
        assert_eq!(seeds.split(',').count(), 6);

        let weekly = execution_plan("weekly", "abc123").expect("weekly plan");
        assert_eq!(
            weekly[0].arguments[3].to_string_lossy().split(',').count(),
            10
        );
        assert_ne!(first[0].arguments[3], weekly[0].arguments[3]);
    }

    #[test]
    fn scheduled_check_ids_bind_to_canonical_registry_checks() {
        assert_eq!(
            canonical_check_id("nightly", "raft-commit-nightly").as_deref(),
            Some("raft-commit")
        );
        assert_eq!(
            canonical_check_id("weekly", "raft-election-prevote-weekly").as_deref(),
            Some("raft-election-prevote")
        );
        assert_eq!(
            canonical_check_id("nightly", "raft-nightly-soak-membership").as_deref(),
            Some("raft-soak-membership")
        );
        assert_eq!(canonical_check_id("pr", "raft-commit"), None);
    }

    #[test]
    fn unsupported_simulator_profile_has_no_execution_plan() {
        assert!(execution_plan("adhoc", "abc123").is_err());
    }
}
