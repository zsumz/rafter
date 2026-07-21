//! Unix simulator timeout and later-launch-failure fixtures.

use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path};

use crate::{
    evidence::format::process::{parse_combined_v4, LabeledProcess},
    producer::process,
};

use super::{
    runner::{execute_plan, model_run},
    types::{ModelRun, SimulatorExecution},
};

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
pub(in crate::producer) fn timed_out_zero_exit_fixture(
    profile: &str,
    source_ref: &str,
    stdout: &str,
    output_dir: &Path,
) -> Result<(SimulatorExecution, LabeledProcess), Box<dyn Error>> {
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
) -> Result<(SimulatorExecution, LabeledProcess), Box<dyn Error>> {
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
    let mut parsed = parse_combined_v4(&String::from_utf8(receipt)?)?;
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
pub(in crate::producer) fn later_launch_error_fixture(
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
