use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
    fs,
    path::Path,
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use std::os::fd::BorrowedFd;

use super::{
    artifact,
    filesystem::{HeldDirectory, OperationDeadline, TREE_LIMITS},
    process, tla_checkpoint,
    tla_contract::required_configuration,
    tla_output,
};
use tla_output::{
    detector_config_kind, detector_invariant, detector_label, detector_log_kind,
    detector_observation, probe_slug, render_detector_config, DetectorProbe, DETECTOR_PROBES,
    MEMBERSHIP_TRACE_MIN_DEPTH, MEMBERSHIP_TRACE_MIN_DISTINCT_STATES, MUTATION_SUITE_ARTIFACT_KIND,
    MUTATION_SUITE_LABEL, REGISTERED_PREDICATES,
};

const TRACE_CONFIG: &str = "RaftMembershipTraceSample.cfg";
const DETECTOR_CONFIG: &str = "RafterInvariantDetectorNegative.cfg";
const JAR: &str = "tools/cache/tla2tools.jar";
const PROBE_TIMEOUT: Duration = Duration::from_secs(120);
const QUALIFICATION_PHASE_COUNT: usize = DETECTOR_PROBES.len() + 2;
const TOTAL_TIMEOUT_KEY: &str = "total_timeout";
const FINALIZATION_RESERVE_KEY: &str = "finalization_reserve";

pub(super) struct TlaExecution {
    pub(super) main: Option<tla_output::TlcSummary>,
    pub(super) main_progress: Option<tla_output::TlcProgress>,
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
            max_heap: required_configuration(configuration, "max_heap")?,
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

fn trace_succeeded(trace: &TlcRun) -> bool {
    trace.output.status.success()
        && !trace.output.timed_out
        && tla_output::parse(&trace.output.stdout)
            .ok()
            .is_some_and(|summary| {
                summary.completed_without_error
                    && summary.process_finished
                    && summary.distinct_states >= MEMBERSHIP_TRACE_MIN_DISTINCT_STATES
                    && summary.states_left == 0
                    && summary.search_depth >= MEMBERSHIP_TRACE_MIN_DEPTH
            })
}

struct MainCompletion<'a> {
    trace: &'a TlcRun,
    detectors: DetectorProbes,
    artifacts: Vec<crate::ArtifactRef>,
    checkpoint: Option<tla_checkpoint::Preparation>,
    checkpoint_report: Option<tla_checkpoint::RecoveryReport>,
    output_dir: &'a Path,
    total_deadline: Instant,
}

pub(super) fn parse_main_summary(
    output: &[u8],
) -> (Option<tla_output::TlcSummary>, Option<String>) {
    match tla_output::parse(output) {
        Ok(summary) => (Some(summary), None),
        Err(error) => match tla_output::parse_complete_prefix(output) {
            Ok(summary) if summary.violated_invariant.is_some() => (Some(summary), Some(error)),
            _ => (None, Some(error)),
        },
    }
}

fn complete_main_execution(
    completion: MainCompletion<'_>,
    main: TlcRun,
) -> Result<TlaExecution, Box<dyn Error>> {
    let MainCompletion {
        trace,
        detectors,
        mut artifacts,
        checkpoint,
        checkpoint_report,
        output_dir,
        total_deadline,
    } = completion;
    let (summary, main_parse_error) = parse_main_summary(&main.output.stdout);
    let main_progress = main
        .output
        .timed_out
        .then(|| {
            tla_output::parse_latest_progress(&main.output.stdout)
                .ok()
                .flatten()
        })
        .flatten();
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
        if summary
            .as_ref()
            .and_then(|summary| summary.violated_invariant.as_ref())
            .is_some()
        {
            artifacts.extend(preparation.abandon());
        } else {
            artifacts.extend(preparation.finish(output_dir, total_deadline)?);
        }
    }
    Ok(TlaExecution {
        main: summary,
        main_progress,
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
    budget: ExecutionBudget,
) -> Result<Option<tla_checkpoint::Preparation>, Box<dyn Error>> {
    tla_checkpoint::enabled(configuration)
        .then(|| {
            tla_checkpoint::prepare(
                profile,
                source_ref,
                configuration,
                artifacts,
                output_dir,
                budget.execution_deadline,
            )
        })
        .transpose()
}

fn trace_failure(trace: &TlcRun, artifacts: Vec<crate::ArtifactRef>) -> TlaExecution {
    TlaExecution {
        main: None,
        main_progress: None,
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

fn trace_budget_failure(artifacts: Vec<crate::ArtifactRef>) -> TlaExecution {
    TlaExecution {
        main: None,
        main_progress: None,
        main_parse_error: None,
        main_status: MainStatus::NotRun,
        trace_status: ProbeStatus::Failed,
        detector_status: ProbeStatus::NotRun,
        detector_qualifications: empty_detector_qualifications(),
        peak_rss_kib: 0,
        duration_ms: 0,
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
        main_progress: None,
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
        main_progress: None,
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

fn main_budget_failure(
    trace: &TlcRun,
    detectors: DetectorProbes,
    artifacts: Vec<crate::ArtifactRef>,
    checkpoint_report: Option<tla_checkpoint::RecoveryReport>,
) -> TlaExecution {
    TlaExecution {
        main: None,
        main_progress: None,
        main_parse_error: None,
        main_status: MainStatus::TimedOut,
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Passed,
        detector_qualifications: detectors.qualifications,
        peak_rss_kib: trace.output.peak_rss_kib.max(detectors.peak_rss_kib),
        duration_ms: process::duration_ms(trace.output.duration)
            .saturating_add(detectors.duration_ms),
        artifacts,
        checkpoint_report,
        checkpoint_error: None,
    }
}

fn run_trace_probe(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
    timeout: Duration,
) -> Result<TlcRun, Box<dyn Error>> {
    run_tlc(TlcRequest {
        profile,
        source_ref,
        config: TRACE_CONFIG,
        module: "RaftMembershipTraceSample.tla",
        workers: "1",
        seed: required_configuration(configuration, "seed")?,
        timeout,
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
    budget: &ExecutionBudget,
) -> Result<DetectorProbes, Box<dyn Error>> {
    let mut aggregate = DetectorProbes::default();
    for probe in DETECTOR_PROBES {
        let Some(timeout) = budget.phase_timeout(PROBE_TIMEOUT) else {
            aggregate.succeeded = false;
            aggregate.qualifications = empty_detector_qualifications();
            break;
        };
        let detector = run_detector_probe(
            profile,
            source_ref,
            configuration,
            output_dir,
            probe,
            timeout,
        )?;
        aggregate.peak_rss_kib = aggregate.peak_rss_kib.max(detector.run.output.peak_rss_kib);
        aggregate.duration_ms = aggregate
            .duration_ms
            .saturating_add(process::duration_ms(detector.run.output.duration));
        let expected_invariant = detector_invariant(probe).ok_or("unregistered detector probe")?;
        let summary = tla_output::parse(&detector.run.output.stdout).ok();
        let qualified = detector_qualified(
            detector.run.output.status.code(),
            detector.run.output.timed_out,
            summary.as_ref(),
            &expected_invariant,
        );
        aggregate.succeeded &= qualified;
        let observation =
            detector_observation(probe.predicate).ok_or("unregistered detector predicate")?;
        let predicate_qualified = aggregate.qualifications.entry(observation).or_insert(1);
        *predicate_qualified &= u64::from(qualified);
        aggregate.artifacts.push(detector.config_artifact);
        aggregate.artifacts.push(detector.run.artifact);
    }
    if aggregate.succeeded {
        let Some(timeout) = budget.phase_timeout(PROBE_TIMEOUT) else {
            aggregate.succeeded = false;
            return Ok(aggregate);
        };
        let mutation = run_mutation_suite(profile, source_ref, output_dir, timeout)?;
        aggregate.peak_rss_kib = aggregate.peak_rss_kib.max(mutation.output.peak_rss_kib);
        aggregate.duration_ms = aggregate
            .duration_ms
            .saturating_add(process::duration_ms(mutation.output.duration));
        aggregate.succeeded &= tla_output::mutation_suite_passed(
            mutation.output.status.code(),
            mutation.output.timed_out,
            &String::from_utf8_lossy(&mutation.output.stdout),
        );
        aggregate.artifacts.push(mutation.artifact);
    }
    Ok(aggregate)
}

fn run_mutation_suite(
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
    timeout: Duration,
) -> Result<TlcRun, Box<dyn Error>> {
    let arguments = [
        "test",
        "--locked",
        "-p",
        "rafter-invariants",
        "producer::tla_exec::mutation_tests",
        "--",
        "--ignored",
        "--test-threads=1",
    ]
    .map(OsString::from);
    let output = process::timed_for_with_cap(
        process::ProcessKind::TlaExecution,
        "cargo",
        &arguments,
        &process::base_environment(),
        Path::new("."),
        Some(timeout),
    )?;
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let artifact = artifact::write(
        output_dir,
        Path::new(&format!(
            "{profile}-tla/{source_prefix}/detector-mutation-suite.json"
        )),
        MUTATION_SUITE_ARTIFACT_KIND,
        &process::tla_json_log(MUTATION_SUITE_LABEL, &output)?,
    )?;
    Ok(TlcRun { output, artifact })
}

fn run_detector_probe(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
    probe: DetectorProbe,
    timeout: Duration,
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
    let config_guard = HeldDirectory::workspace()?.hold_file(Path::new(&config_artifact.path))?;
    config_guard.verify_path_binding()?;
    let config = config_guard.external_path().to_string_lossy().into_owned();
    let label = detector_label(probe).ok_or("unregistered detector probe")?;
    let artifact_kind = detector_log_kind(probe).ok_or("unregistered detector probe")?;
    let run = run_tlc(TlcRequest {
        profile,
        source_ref,
        config: &config,
        module: "RafterInvariantDetectorNegative.tla",
        workers: "1",
        seed: required_configuration(configuration, "seed")?,
        timeout,
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
        .filter_map(|predicate| detector_observation(predicate).map(|observation| (observation, 0)))
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

#[derive(Clone, Copy, Debug)]
struct ExecutionBudget {
    execution_deadline: Instant,
    total_deadline: Instant,
}

impl ExecutionBudget {
    fn from_configuration(
        profile: &str,
        configuration: &BTreeMap<String, String>,
    ) -> Result<Self, Box<dyn Error>> {
        Self::at(profile, configuration, Instant::now())?;
        let (execution_deadline, total_deadline) = process::active_layer_deadlines(profile, "tla")?;
        Ok(Self {
            execution_deadline,
            total_deadline,
        })
    }

    fn at(
        profile: &str,
        configuration: &BTreeMap<String, String>,
        started: Instant,
    ) -> Result<Self, Box<dyn Error>> {
        let total = configured_budget_duration(configuration, TOTAL_TIMEOUT_KEY)?;
        let reserve = configured_budget_duration(configuration, FINALIZATION_RESERVE_KEY)?;
        match (total, reserve) {
            (Some(total), Some(reserve)) => {
                let execution_window = total
                    .checked_sub(reserve)
                    .filter(|window| !window.is_zero())
                    .ok_or("TLA total_timeout must exceed finalization_reserve")?;
                let maximum_probe_time = PROBE_TIMEOUT
                    .checked_mul(u32::try_from(QUALIFICATION_PHASE_COUNT)?)
                    .ok_or("TLA probe budget overflow")?;
                if execution_window <= maximum_probe_time {
                    return Err(
                        "TLA shared execution budget must leave time for the main model check"
                            .into(),
                    );
                }
                let execution_deadline = started
                    .checked_add(execution_window)
                    .ok_or("TLA shared execution deadline overflow")?;
                let total_deadline = started
                    .checked_add(total)
                    .ok_or("TLA total deadline overflow")?;
                Ok(Self {
                    execution_deadline,
                    total_deadline,
                })
            }
            (None, None) => Err(format!(
                "{profile} TLA runner requires total_timeout and finalization_reserve"
            )
            .into()),
            _ => {
                Err("TLA total_timeout and finalization_reserve must be configured together".into())
            }
        }
    }

    fn phase_timeout(self, cap: Duration) -> Option<Duration> {
        self.phase_timeout_at(Instant::now(), cap)
    }

    fn phase_timeout_at(self, now: Instant, cap: Duration) -> Option<Duration> {
        let remaining = self.execution_deadline.checked_duration_since(now)?;
        (!remaining.is_zero()).then_some(cap.min(remaining))
    }
}

fn configured_budget_duration(
    configuration: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<Duration>, Box<dyn Error>> {
    configuration
        .get(key)
        .map(|value| {
            let minutes = value
                .strip_suffix('m')
                .ok_or_else(|| format!("TLA {key} must use whole minutes"))?
                .parse::<u64>()?;
            let seconds = minutes
                .checked_mul(60)
                .ok_or_else(|| format!("TLA {key} is too large"))?;
            Ok(Duration::from_secs(seconds))
        })
        .transpose()
}

#[derive(Clone, Copy)]
enum TlcState<'a> {
    Ephemeral,
    Checkpoint {
        state_dir: &'a HeldDirectory,
        recover_from: Option<&'a HeldDirectory>,
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
    #[cfg(not(target_os = "linux"))]
    require_sound_tlc_state_binding()?;
    let source_prefix = request.source_ref.get(..12).unwrap_or(request.source_ref);
    process::ensure_execution_deadline(
        request.profile,
        "tla",
        &format!("{} TLC state preparation", request.label),
    )?;
    let ephemeral_state = prepare_ephemeral_state(request, source_prefix)?;
    let (state_handle, recover_handle) = state_handles(request.state, ephemeral_state.as_ref())?;
    process::ensure_execution_deadline(
        request.profile,
        "tla",
        &format!("{} TLC process launch", request.label),
    )?;
    verify_tlc_state_binding(state_handle, recover_handle)?;
    let state_binding = state_handle.bind_for_child()?;
    let recover_binding = recover_handle
        .map(HeldDirectory::bind_for_child)
        .transpose()?;
    let arguments = tlc_arguments(
        request,
        state_binding.path(),
        recover_binding
            .as_ref()
            .map(super::filesystem::ChildDirectory::path),
    )?;
    verify_tlc_state_binding(state_handle, recover_handle)?;
    let environment = process::base_environment();
    #[cfg(target_os = "linux")]
    let descriptors = tlc_directory_descriptors(&state_binding, recover_binding.as_ref());
    #[cfg(target_os = "linux")]
    let output = process::timed_for_with_cap_and_descriptors(
        process::ProcessKind::TlaExecution,
        "java",
        &arguments,
        &environment,
        Path::new("specs/tla/raft"),
        Some(request.timeout),
        &descriptors,
    )?;
    #[cfg(not(target_os = "linux"))]
    let output = process::timed_for_with_cap(
        process::ProcessKind::TlaExecution,
        "java",
        &arguments,
        &environment,
        Path::new("specs/tla/raft"),
        Some(request.timeout),
    )?;
    verify_tlc_state_binding(state_handle, recover_handle)?;
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

#[cfg(not(target_os = "linux"))]
fn require_sound_tlc_state_binding() -> Result<(), Box<dyn Error>> {
    Err("TLC execution requires Linux descriptor-relative state directories; this host cannot soundly expose a held directory tree to Java".into())
}

fn prepare_ephemeral_state(
    request: TlcRequest<'_>,
    source_prefix: &str,
) -> Result<Option<HeldDirectory>, Box<dyn Error>> {
    if !matches!(request.state, TlcState::Ephemeral) {
        return Ok(None);
    }
    let state_dir = Path::new("target/rafter-invariants/tla")
        .join(source_prefix)
        .join(request.profile)
        .join(request.label);
    let (execution_deadline, _) = process::active_layer_deadlines(request.profile, "tla")?;
    Ok(Some(HeldDirectory::replace_tree(
        &state_dir,
        TREE_LIMITS,
        OperationDeadline::at(execution_deadline, "stale TLC state cleanup"),
    )?))
}

fn state_handles<'a>(
    state: TlcState<'a>,
    ephemeral_state: Option<&'a HeldDirectory>,
) -> Result<(&'a HeldDirectory, Option<&'a HeldDirectory>), Box<dyn Error>> {
    match state {
        TlcState::Ephemeral => Ok((
            ephemeral_state.ok_or("ephemeral TLC state handle was not initialized")?,
            None,
        )),
        TlcState::Checkpoint {
            state_dir,
            recover_from,
            ..
        } => Ok((state_dir, recover_from)),
    }
}

fn tlc_arguments(
    request: TlcRequest<'_>,
    state_dir: &Path,
    recover_from: Option<&Path>,
) -> Result<Vec<OsString>, Box<dyn Error>> {
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
        state_dir.as_os_str().to_os_string(),
    ]);
    if let TlcState::Checkpoint {
        checkpoint_minutes, ..
    } = request.state
    {
        arguments.extend([
            "-checkpoint".into(),
            checkpoint_minutes.into(),
            "-gzip".into(),
        ]);
        if let Some(recover_from) = recover_from {
            arguments.extend(["-recover".into(), recover_from.as_os_str().to_os_string()]);
        }
    }
    arguments.extend([
        "-config".into(),
        request.config.into(),
        request.module.into(),
    ]);
    Ok(arguments)
}

#[cfg(target_os = "linux")]
fn tlc_directory_descriptors<'a>(
    state: &'a super::filesystem::ChildDirectory,
    recover: Option<&'a super::filesystem::ChildDirectory>,
) -> Vec<BorrowedFd<'a>> {
    let mut descriptors = vec![state.descriptor()];
    if let Some(recover) = recover {
        descriptors.push(recover.descriptor());
    }
    descriptors
}

fn verify_tlc_state_binding(
    state: &HeldDirectory,
    recover: Option<&HeldDirectory>,
) -> Result<(), Box<dyn Error>> {
    state.verify_path_binding()?;
    if let Some(recover) = recover {
        recover.verify_path_binding()?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tla_exec_budget_tests.rs"]
mod budget_tests;

#[cfg(test)]
#[path = "tla_mutation_tests.rs"]
mod mutation_tests;
