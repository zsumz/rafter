use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
    fs,
    path::Path,
    time::{Duration, Instant},
};

use super::{artifact, process, tla_checkpoint, tla_contract::required_configuration, tla_output};
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
    let trace_summary = tla_output::parse(&trace.output.stdout).ok();
    let trace_succeeded = trace.output.status.success()
        && !trace.output.timed_out
        && trace_summary.as_ref().is_some_and(|summary| {
            summary.completed_without_error
                && summary.process_finished
                && summary.distinct_states >= MEMBERSHIP_TRACE_MIN_DISTINCT_STATES
                && summary.states_left == 0
                && summary.search_depth >= MEMBERSHIP_TRACE_MIN_DEPTH
        });
    if !trace_succeeded {
        return Ok(trace_failure(&trace, artifacts));
    }
    let mut detectors =
        run_detector_probes(profile, source_ref, configuration, output_dir, &budget)?;
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
        let Some(preparation) = checkpoint.take() else {
            return Err("checkpoint error was reported without checkpoint state".into());
        };
        artifacts.extend(preparation.finish(output_dir)?);
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
            artifacts.extend(preparation.finish(output_dir)?);
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
            state_dir: &preparation.state_dir,
            recover_from: preparation.recover_from.as_deref(),
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
        &trace,
        detectors,
        artifacts,
        checkpoint,
        checkpoint_report,
        output_dir,
        main,
    )
}

fn complete_main_execution(
    trace: &TlcRun,
    detectors: DetectorProbes,
    mut artifacts: Vec<crate::ArtifactRef>,
    checkpoint: Option<tla_checkpoint::Preparation>,
    checkpoint_report: Option<tla_checkpoint::RecoveryReport>,
    output_dir: &Path,
    main: TlcRun,
) -> Result<TlaExecution, Box<dyn Error>> {
    let (summary, main_parse_error) = match tla_output::parse(&main.output.stdout) {
        Ok(summary) => (Some(summary), None),
        Err(error) => (None, Some(error)),
    };
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
        artifacts.extend(preparation.finish(output_dir)?);
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
) -> Result<Option<tla_checkpoint::Preparation>, Box<dyn Error>> {
    tla_checkpoint::enabled(configuration)
        .then(|| tla_checkpoint::prepare(profile, source_ref, configuration, artifacts, output_dir))
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
    let output = process::timed_with_timeout(
        "cargo",
        &arguments,
        &process::base_environment(),
        Path::new("."),
        timeout,
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
    execution_deadline: Option<Instant>,
}

impl ExecutionBudget {
    fn from_configuration(
        profile: &str,
        configuration: &BTreeMap<String, String>,
    ) -> Result<Self, Box<dyn Error>> {
        Self::at(profile, configuration, Instant::now())
    }

    fn at(
        profile: &str,
        configuration: &BTreeMap<String, String>,
        started: Instant,
    ) -> Result<Self, Box<dyn Error>> {
        let total = configured_budget_duration(configuration, TOTAL_TIMEOUT_KEY)?;
        let reserve = configured_budget_duration(configuration, FINALIZATION_RESERVE_KEY)?;
        let execution_deadline = match (total, reserve) {
            (None, None) if profile != "pr" => None,
            (Some(total), Some(reserve)) => {
                let execution_window = total
                    .checked_sub(reserve)
                    .filter(|window| !window.is_zero())
                    .ok_or("TLA total_timeout must exceed finalization_reserve")?;
                let maximum_probe_time = PROBE_TIMEOUT
                    .checked_mul(u32::try_from(DETECTOR_PROBES.len() + 1)?)
                    .ok_or("TLA probe budget overflow")?;
                if execution_window <= maximum_probe_time {
                    return Err(
                        "TLA shared execution budget must leave time for the main model check"
                            .into(),
                    );
                }
                Some(
                    started
                        .checked_add(execution_window)
                        .ok_or("TLA shared execution deadline overflow")?,
                )
            }
            (None, None) => {
                return Err("PR TLA runner requires total_timeout and finalization_reserve".into())
            }
            _ => {
                return Err(
                    "TLA total_timeout and finalization_reserve must be configured together".into(),
                )
            }
        };
        Ok(Self { execution_deadline })
    }

    fn phase_timeout(self, cap: Duration) -> Option<Duration> {
        self.phase_timeout_at(Instant::now(), cap)
    }

    fn phase_timeout_at(self, now: Instant, cap: Duration) -> Option<Duration> {
        let Some(deadline) = self.execution_deadline else {
            return Some(cap);
        };
        let remaining = deadline.checked_duration_since(now)?;
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
mod budget_tests {
    use std::{collections::BTreeMap, time::Duration};

    use super::{ExecutionBudget, FINALIZATION_RESERVE_KEY, PROBE_TIMEOUT, TOTAL_TIMEOUT_KEY};

    fn pr_budget() -> BTreeMap<String, String> {
        BTreeMap::from([
            (TOTAL_TIMEOUT_KEY.to_owned(), "120m".to_owned()),
            (FINALIZATION_RESERVE_KEY.to_owned(), "2m".to_owned()),
        ])
    }

    #[test]
    fn shared_pr_budget_reduces_the_main_timeout_and_preserves_the_reserve() {
        let started = std::time::Instant::now();
        let budget = ExecutionBudget::at("pr", &pr_budget(), started).expect("valid PR budget");

        assert_eq!(
            budget.phase_timeout_at(started, PROBE_TIMEOUT),
            Some(PROBE_TIMEOUT)
        );
        assert_eq!(
            budget.phase_timeout_at(
                started + Duration::from_secs(20 * 60),
                Duration::from_secs(115 * 60),
            ),
            Some(Duration::from_secs(98 * 60))
        );
        assert_eq!(
            budget.phase_timeout_at(
                started + Duration::from_secs(118 * 60),
                Duration::from_secs(115 * 60),
            ),
            None
        );
    }

    #[test]
    fn pr_budget_requires_a_paired_reserve_and_time_for_the_main_run() {
        let started = std::time::Instant::now();
        assert!(ExecutionBudget::at("pr", &BTreeMap::new(), started).is_err());
        assert!(ExecutionBudget::at(
            "pr",
            &BTreeMap::from([(TOTAL_TIMEOUT_KEY.to_owned(), "120m".to_owned())]),
            started,
        )
        .is_err());
        assert!(ExecutionBudget::at(
            "pr",
            &BTreeMap::from([
                (TOTAL_TIMEOUT_KEY.to_owned(), "20m".to_owned()),
                (FINALIZATION_RESERVE_KEY.to_owned(), "2m".to_owned()),
            ]),
            started,
        )
        .is_err());
    }

    #[test]
    fn scheduled_profiles_keep_their_existing_per_phase_bounds() {
        let started = std::time::Instant::now();
        let budget = ExecutionBudget::at("weekly", &BTreeMap::new(), started)
            .expect("scheduled profile remains unbounded across phases");
        assert_eq!(
            budget.phase_timeout_at(started + Duration::from_secs(24 * 60 * 60), PROBE_TIMEOUT,),
            Some(PROBE_TIMEOUT)
        );
    }
}

#[cfg(test)]
#[path = "tla_mutation_tests.rs"]
mod mutation_tests;
