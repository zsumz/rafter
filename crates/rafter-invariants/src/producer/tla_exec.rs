use std::{collections::BTreeMap, error::Error, fs, path::Path, time::Duration};

use super::{artifact, process, tla_checkpoint, tla_contract::required_configuration, tla_output};
use tla_output::{
    detector_config_kind, detector_invariant, detector_label, detector_log_kind,
    detector_observation, probe_slug, render_detector_config, DetectorProbe, DETECTOR_PROBES,
    REGISTERED_PREDICATES,
};

const TRACE_CONFIG: &str = "RaftTraceSample.cfg";
const DETECTOR_CONFIG: &str = "RafterInvariantDetectorNegative.cfg";
const JAR: &str = "tools/cache/tla2tools.jar";

pub(super) struct TlaExecution {
    pub(super) main: Option<tla_output::TlcSummary>,
    pub(super) main_parse_error: Option<String>,
    pub(super) main_status: MainStatus,
    pub(super) trace_status: ProbeStatus,
    pub(super) detector_status: ProbeStatus,
    pub(super) detector_qualifications: BTreeMap<String, u64>,
    pub(super) peak_rss_kib: u64,
    pub(super) duration_ms: u64,
    pub(super) artifacts: Vec<crate::ArtifactRef>,
    pub(super) checkpoint_report: Option<tla_checkpoint::RecoveryReport>,
    pub(super) checkpoint_error: Option<String>,
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
    artifacts.push(trace.artifact.clone());
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
        return Ok(trace_failure(&trace, artifacts));
    }
    let mut detectors = run_detector_probes(profile, source_ref, configuration, output_dir)?;
    artifacts.append(&mut detectors.artifacts);
    if !detectors.succeeded {
        return Ok(detector_failure(&trace, detectors, artifacts));
    }
    let mut checkpoint =
        prepare_checkpoint(profile, source_ref, configuration, &artifacts, output_dir)?;
    let checkpoint_report = checkpoint
        .as_ref()
        .map(|preparation| preparation.report.clone());
    if let Some(error) = checkpoint
        .as_ref()
        .and_then(|preparation| preparation.error.clone())
    {
        artifacts.extend(
            checkpoint
                .take()
                .expect("checkpoint exists")
                .finish(output_dir)?,
        );
        return Ok(checkpoint_failure(
            &trace,
            detectors,
            artifacts,
            checkpoint_report,
            error,
        ));
    }
    let state = checkpoint
        .as_ref()
        .map_or(TlcState::Ephemeral, |preparation| TlcState::Checkpoint {
            state_dir: &preparation.state_dir,
            recover_from: preparation.recover_from.as_deref(),
            checkpoint_minutes: required_configuration(configuration, "checkpoint_minutes")
                .expect("validated checkpoint interval"),
            max_heap: required_configuration(configuration, "max_heap")
                .expect("validated checkpoint heap"),
        });
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
        artifact_kind: "tla-log",
        state,
    })?;
    let (summary, main_parse_error) = match tla_output::parse(&main.output.stdout) {
        Ok(summary) => (Some(summary), None),
        Err(error) => (None, Some(error)),
    };
    let peak_rss_kib = trace
        .output
        .peak_rss_kib
        .max(detectors.peak_rss_kib)
        .max(main.output.peak_rss_kib);
    let duration_ms = process::duration_ms(trace.output.duration)
        .saturating_add(detectors.duration_ms)
        .saturating_add(process::duration_ms(main.output.duration));
    let main_status = classify_main_status(&main.output);
    artifacts.push(main.artifact);
    if let Some(preparation) = checkpoint {
        artifacts.extend(preparation.finish(output_dir)?);
    }
    Ok(TlaExecution {
        main: summary,
        main_parse_error,
        main_status,
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Passed,
        detector_qualifications: detectors.qualifications,
        peak_rss_kib,
        duration_ms,
        artifacts,
        checkpoint_report,
        checkpoint_error: None,
    })
}

fn classify_main_status(output: &process::ProcessOutput) -> MainStatus {
    if output.timed_out {
        MainStatus::TimedOut
    } else if output.status.success() {
        MainStatus::Succeeded
    } else {
        MainStatus::Failed
    }
}

fn prepare_checkpoint(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    artifacts: &[crate::ArtifactRef],
    output_dir: &Path,
) -> Result<Option<tla_checkpoint::Preparation>, Box<dyn Error>> {
    tla_checkpoint::enabled(configuration)
        .then(|| tla_checkpoint::prepare(profile, source_ref, configuration, artifacts, output_dir))
        .transpose()
}

fn trace_failure(trace: &TlcRun, artifacts: Vec<crate::ArtifactRef>) -> TlaExecution {
    TlaExecution {
        main: None,
        main_parse_error: None,
        main_status: MainStatus::NotRun,
        trace_status: ProbeStatus::Failed,
        detector_status: ProbeStatus::NotRun,
        detector_qualifications: empty_detector_qualifications(),
        peak_rss_kib: trace.output.peak_rss_kib,
        duration_ms: process::duration_ms(trace.output.duration),
        artifacts,
        checkpoint_report: None,
        checkpoint_error: None,
    }
}

fn detector_failure(
    trace: &TlcRun,
    detectors: DetectorProbes,
    artifacts: Vec<crate::ArtifactRef>,
) -> TlaExecution {
    TlaExecution {
        main: None,
        main_parse_error: None,
        main_status: MainStatus::NotRun,
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Failed,
        detector_qualifications: detectors.qualifications,
        peak_rss_kib: trace.output.peak_rss_kib.max(detectors.peak_rss_kib),
        duration_ms: process::duration_ms(trace.output.duration)
            .saturating_add(detectors.duration_ms),
        artifacts,
        checkpoint_report: None,
        checkpoint_error: None,
    }
}

fn checkpoint_failure(
    trace: &TlcRun,
    detectors: DetectorProbes,
    artifacts: Vec<crate::ArtifactRef>,
    checkpoint_report: Option<tla_checkpoint::RecoveryReport>,
    error: String,
) -> TlaExecution {
    TlaExecution {
        main: None,
        main_parse_error: None,
        main_status: MainStatus::NotRun,
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Passed,
        detector_qualifications: detectors.qualifications,
        peak_rss_kib: trace.output.peak_rss_kib.max(detectors.peak_rss_kib),
        duration_ms: process::duration_ms(trace.output.duration)
            .saturating_add(detectors.duration_ms),
        artifacts,
        checkpoint_report,
        checkpoint_error: Some(error),
    }
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
        artifact_kind: "tla-trace-log",
        state: TlcState::Ephemeral,
    })
}

fn run_detector_probes(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
) -> Result<DetectorProbes, Box<dyn Error>> {
    let mut aggregate = DetectorProbes::default();
    for probe in DETECTOR_PROBES {
        let detector = run_detector_probe(profile, source_ref, configuration, output_dir, probe)?;
        aggregate.peak_rss_kib = aggregate.peak_rss_kib.max(detector.run.output.peak_rss_kib);
        aggregate.duration_ms = aggregate
            .duration_ms
            .saturating_add(process::duration_ms(detector.run.output.duration));
        let expected_invariant = detector_invariant(probe).expect("registered detector probe");
        let summary = tla_output::parse(&detector.run.output.stdout).ok();
        let qualified = detector_qualified(
            detector.run.output.status.code(),
            detector.run.output.timed_out,
            summary.as_ref(),
            &expected_invariant,
        );
        aggregate.succeeded &= qualified;
        let observation =
            detector_observation(probe.predicate).expect("registered detector predicate");
        let predicate_qualified = aggregate.qualifications.entry(observation).or_insert(1);
        *predicate_qualified &= u64::from(qualified);
        aggregate.artifacts.push(detector.config_artifact);
        aggregate.artifacts.push(detector.run.artifact);
    }
    Ok(aggregate)
}

fn run_detector_probe(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
    probe: DetectorProbe,
) -> Result<DetectorRun, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let template = fs::read_to_string(Path::new("specs/tla/raft").join(DETECTOR_CONFIG))?;
    let config_source = render_detector_config(&template, probe)?;
    let config_kind = detector_config_kind(probe).ok_or("unregistered detector probe")?;
    let slug = probe_slug(probe);
    let config_artifact = artifact::write(
        output_dir,
        Path::new(&format!(
            "{profile}-tla/{source_prefix}/detectors/{slug}.cfg"
        )),
        &config_kind,
        config_source.as_bytes(),
    )?;
    let config = fs::canonicalize(&config_artifact.path)?
        .to_string_lossy()
        .into_owned();
    let label = detector_label(probe).ok_or("unregistered detector probe")?;
    let artifact_kind = detector_log_kind(probe).ok_or("unregistered detector probe")?;
    let run = run_tlc(TlcRequest {
        profile,
        source_ref,
        config: &config,
        module: "RafterInvariantDetectorNegative.tla",
        workers: "1",
        seed: required_configuration(configuration, "seed")?,
        timeout: Duration::from_secs(120),
        output_dir,
        label: &label,
        artifact_kind: &artifact_kind,
        state: TlcState::Ephemeral,
    })?;
    Ok(DetectorRun {
        run,
        config_artifact,
    })
}

struct TlcRun {
    output: process::ProcessOutput,
    artifact: crate::ArtifactRef,
}

struct DetectorRun {
    run: TlcRun,
    config_artifact: crate::ArtifactRef,
}

struct DetectorProbes {
    succeeded: bool,
    peak_rss_kib: u64,
    duration_ms: u64,
    artifacts: Vec<crate::ArtifactRef>,
    qualifications: BTreeMap<String, u64>,
}

impl Default for DetectorProbes {
    fn default() -> Self {
        Self {
            succeeded: true,
            peak_rss_kib: 0,
            duration_ms: 0,
            artifacts: Vec::new(),
            qualifications: BTreeMap::new(),
        }
    }
}

fn empty_detector_qualifications() -> BTreeMap<String, u64> {
    REGISTERED_PREDICATES
        .into_iter()
        .map(|predicate| {
            (
                detector_observation(predicate).expect("registered predicate"),
                0,
            )
        })
        .collect()
}

fn detector_qualified(
    exit_code: Option<i32>,
    timed_out: bool,
    summary: Option<&tla_output::TlcSummary>,
    expected_invariant: &str,
) -> bool {
    exit_code == Some(12)
        && !timed_out
        && summary.is_some_and(|summary| {
            !summary.completed_without_error
                && summary.process_finished
                && summary.violated_invariant.as_deref() == Some(expected_invariant)
                && summary.distinct_states >= 2
                && summary.states_left == 0
                && summary.search_depth >= 2
        })
}

#[derive(Clone, Copy)]
enum TlcState<'a> {
    Ephemeral,
    Checkpoint {
        state_dir: &'a Path,
        recover_from: Option<&'a Path>,
        checkpoint_minutes: &'a str,
        max_heap: &'a str,
    },
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
    artifact_kind: &'a str,
    state: TlcState<'a>,
}

fn run_tlc(request: TlcRequest<'_>) -> Result<TlcRun, Box<dyn Error>> {
    let source_prefix = request.source_ref.get(..12).unwrap_or(request.source_ref);
    let state_dir = match request.state {
        TlcState::Ephemeral => {
            let state_dir = Path::new("target/rafter-invariants/tla")
                .join(source_prefix)
                .join(request.profile)
                .join(request.label);
            if state_dir.exists() {
                fs::remove_dir_all(&state_dir)?;
            }
            fs::create_dir_all(&state_dir)?;
            fs::canonicalize(state_dir)?
        }
        TlcState::Checkpoint { state_dir, .. } => {
            fs::create_dir_all(state_dir)?;
            fs::canonicalize(state_dir)?
        }
    };
    let jar = fs::canonicalize(JAR)?;
    let mut arguments = Vec::new();
    if let TlcState::Checkpoint { max_heap, .. } = request.state {
        arguments.push(format!("-Xmx{max_heap}").into());
    }
    arguments.extend([
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
    ]);
    if let TlcState::Checkpoint {
        checkpoint_minutes,
        recover_from,
        ..
    } = request.state
    {
        arguments.extend([
            "-checkpoint".into(),
            checkpoint_minutes.into(),
            "-gzip".into(),
        ]);
        if let Some(recover_from) = recover_from {
            arguments.extend([
                "-recover".into(),
                fs::canonicalize(recover_from)?.into_os_string(),
            ]);
        }
    }
    arguments.extend([
        "-config".into(),
        request.config.into(),
        request.module.into(),
    ]);
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
        request.artifact_kind,
        &process::tla_json_log(request.label, &output)?,
    )?;
    Ok(TlcRun { output, artifact })
}

#[cfg(test)]
#[path = "tla_mutation_tests.rs"]
mod mutation_tests;
