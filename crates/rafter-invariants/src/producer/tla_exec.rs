use std::{collections::BTreeMap, error::Error, fs, path::Path, time::Duration};

use super::{artifact, process, tla_contract::required_configuration, tla_output};

const TRACE_CONFIG: &str = "RaftTraceSample.cfg";
const DETECTOR_CONFIG: &str = "RafterInvariantDetectorNegative.cfg";
const JAR: &str = "tools/cache/tla2tools.jar";

pub(super) struct TlaExecution {
    pub(super) main: Option<tla_output::TlcSummary>,
    pub(super) main_parse_error: Option<String>,
    pub(super) main_status: MainStatus,
    pub(super) trace_status: ProbeStatus,
    pub(super) detector_status: ProbeStatus,
    pub(super) peak_rss_kib: u64,
    pub(super) duration_ms: u64,
    pub(super) artifacts: Vec<crate::ArtifactRef>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MainStatus {
    NotRun,
    Succeeded,
    Failed,
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProbeStatus {
    NotRun,
    Passed,
    Failed,
}

pub(super) fn execute(
    profile: &str,
    source_ref: &str,
    config: &str,
    configuration: &BTreeMap<String, String>,
    timeout: Duration,
    output_dir: &Path,
    mut artifacts: Vec<crate::ArtifactRef>,
) -> Result<TlaExecution, Box<dyn Error>> {
    let trace = run_trace_probe(profile, source_ref, configuration, output_dir)?;
    artifacts.push(trace.artifact);
    let trace_summary = tla_output::parse(&trace.output.stdout).ok();
    let trace_succeeded = trace.output.status.success()
        && !trace.output.timed_out
        && trace_summary.as_ref().is_some_and(|summary| {
            summary.completed_without_error
                && summary.process_finished
                && summary.distinct_states >= 4
                && summary.states_left == 0
                && summary.search_depth >= 4
        });
    if !trace_succeeded {
        return Ok(TlaExecution {
            main: None,
            main_parse_error: None,
            main_status: MainStatus::NotRun,
            trace_status: ProbeStatus::Failed,
            detector_status: ProbeStatus::NotRun,
            peak_rss_kib: trace.output.peak_rss_kib,
            duration_ms: process::duration_ms(trace.output.duration),
            artifacts,
        });
    }
    let detector = run_detector_probe(profile, source_ref, configuration, output_dir)?;
    artifacts.push(detector.artifact);
    let detector_summary = tla_output::parse(&detector.output.stdout).ok();
    let detector_succeeded = detector.output.status.code() == Some(12)
        && !detector.output.timed_out
        && detector_summary.as_ref().is_some_and(|summary| {
            !summary.completed_without_error
                && summary.process_finished
                && summary.violated_invariant.as_deref() == Some("ExpectedViolation")
                && summary.distinct_states >= 2
                && summary.states_left == 0
                && summary.search_depth >= 2
        });
    if !detector_succeeded {
        return Ok(TlaExecution {
            main: None,
            main_parse_error: None,
            main_status: MainStatus::NotRun,
            trace_status: ProbeStatus::Passed,
            detector_status: ProbeStatus::Failed,
            peak_rss_kib: trace.output.peak_rss_kib.max(detector.output.peak_rss_kib),
            duration_ms: process::duration_ms(trace.output.duration)
                .saturating_add(process::duration_ms(detector.output.duration)),
            artifacts,
        });
    }
    let main = run_tlc(TlcRequest {
        profile,
        source_ref,
        config,
        module: "Raft.tla",
        workers: required_configuration(configuration, "workers")?,
        seed: required_configuration(configuration, "seed")?,
        timeout,
        output_dir,
        label: "model-check",
    })?;
    let (summary, main_parse_error) = match tla_output::parse(&main.output.stdout) {
        Ok(summary) => (Some(summary), None),
        Err(error) => (None, Some(error)),
    };
    let peak_rss_kib = trace
        .output
        .peak_rss_kib
        .max(detector.output.peak_rss_kib)
        .max(main.output.peak_rss_kib);
    let duration_ms = process::duration_ms(trace.output.duration)
        .saturating_add(process::duration_ms(detector.output.duration))
        .saturating_add(process::duration_ms(main.output.duration));
    let main_status = if main.output.timed_out {
        MainStatus::TimedOut
    } else if main.output.status.success() {
        MainStatus::Succeeded
    } else {
        MainStatus::Failed
    };
    artifacts.push(main.artifact);
    Ok(TlaExecution {
        main: summary,
        main_parse_error,
        main_status,
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Passed,
        peak_rss_kib,
        duration_ms,
        artifacts,
    })
}

fn run_trace_probe(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
) -> Result<TlcRun, Box<dyn Error>> {
    run_tlc(TlcRequest {
        profile,
        source_ref,
        config: TRACE_CONFIG,
        module: "RaftTraceSample.tla",
        workers: "1",
        seed: required_configuration(configuration, "seed")?,
        timeout: Duration::from_secs(120),
        output_dir,
        label: "trace-sample",
    })
}

fn run_detector_probe(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
) -> Result<TlcRun, Box<dyn Error>> {
    run_tlc(TlcRequest {
        profile,
        source_ref,
        config: DETECTOR_CONFIG,
        module: "RafterInvariantDetectorNegative.tla",
        workers: "1",
        seed: required_configuration(configuration, "seed")?,
        timeout: Duration::from_secs(120),
        output_dir,
        label: "detector-negative",
    })
}

struct TlcRun {
    output: process::ProcessOutput,
    artifact: crate::ArtifactRef,
}

#[derive(Clone, Copy)]
struct TlcRequest<'a> {
    profile: &'a str,
    source_ref: &'a str,
    config: &'a str,
    module: &'a str,
    workers: &'a str,
    seed: &'a str,
    timeout: Duration,
    output_dir: &'a Path,
    label: &'a str,
}

fn run_tlc(request: TlcRequest<'_>) -> Result<TlcRun, Box<dyn Error>> {
    let source_prefix = request.source_ref.get(..12).unwrap_or(request.source_ref);
    let state_dir = Path::new("target/rafter-invariants/tla")
        .join(source_prefix)
        .join(request.profile)
        .join(request.label);
    if state_dir.exists() {
        fs::remove_dir_all(&state_dir)?;
    }
    fs::create_dir_all(&state_dir)?;
    let state_dir = fs::canonicalize(state_dir)?;
    let jar = fs::canonicalize(JAR)?;
    let arguments = [
        "-XX:+UseParallelGC".into(),
        "-cp".into(),
        jar.into_os_string(),
        "tlc2.TLC".into(),
        "-tool".into(),
        "-workers".into(),
        request.workers.into(),
        "-seed".into(),
        request.seed.into(),
        "-fp".into(),
        "0".into(),
        "-metadir".into(),
        state_dir.into_os_string(),
        "-config".into(),
        request.config.into(),
        request.module.into(),
    ];
    let output = process::timed_with_timeout(
        "java",
        &arguments,
        &process::base_environment(),
        Path::new("specs/tla/raft"),
        request.timeout,
    )?;
    let artifact = artifact::write(
        request.output_dir,
        Path::new(&format!(
            "{}-tla/{source_prefix}/{}.log",
            request.profile, request.label
        )),
        match request.label {
            "model-check" => "tla-log",
            "trace-sample" => "tla-trace-log",
            "detector-negative" => "tla-detector-log",
            _ => return Err("unknown TLA proof-log label".into()),
        },
        &process::json_log(request.label, &output)?,
    )?;
    Ok(TlcRun { output, artifact })
}
