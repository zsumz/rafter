//! One bounded Maelstrom trial and its exact evidence assembly.

use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::Path,
    time::{Duration, Instant},
};

use crate::execution::filesystem::{self as producer_fs, HeldDirectory, OperationDeadline};

use super::super::{
    artifact, maelstrom_edn, process,
    scenario::{required_configuration, Scenario},
    tool,
};
use super::{
    artifacts::{capture_binary, capture_tree, discover_store, reset_state_directory},
    cleanup_state_directory,
    lease::read_markers,
    model::{ScenarioMarkers, TrialOutcome},
};

struct TrialEvidence<'a> {
    scenario: Scenario,
    output_dir: &'a Path,
    namespace: &'a Path,
    script: &'a Path,
    state_dir: &'a HeldDirectory,
    durable: &'a HeldDirectory,
    deadline: Instant,
}

pub(in crate::producer::maelstrom) fn run_trial(
    scenario: Scenario,
    trial: u64,
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
) -> Result<TrialOutcome, Box<dyn Error>> {
    let (execution_deadline, total_deadline) =
        process::active_layer_deadlines(profile, "maelstrom")?;
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let state_dir = reset_state_directory(
        &Path::new("target/rafter-invariants/maelstrom")
            .join(source_prefix)
            .join(profile)
            .join(scenario.name())
            .join(format!("trial-{trial}")),
        execution_deadline,
    )?;
    let outcome = (|| {
        let durable = state_dir.create_dir_all(Path::new("durable"))?;
        let script = fs::canonicalize(scenario.script())?;
        let script_handle = producer_fs::hold_file(&script)?;
        let script_dir = script
            .parent()
            .ok_or("Maelstrom scenario script omitted its parent directory")?;
        let durable_path = durable.external_path();
        let environment = trial_environment(configuration, &durable_path, script_dir, scenario)?;
        let timeout = trial_process_timeout(configuration)?;
        state_dir.verify_path_binding()?;
        durable.verify_path_binding()?;
        script_handle.verify_path_binding()?;
        let state_path = state_dir.path().to_path_buf();
        let output = process::timed_for_with_cap(
            process::ProcessKind::MaelstromTrial,
            script
                .to_str()
                .ok_or("Maelstrom script path is not UTF-8")?,
            &["--test-count".into(), "1".into()],
            &environment,
            &state_path,
            Some(timeout),
        )?;
        let namespace = Path::new(&format!(
            "{profile}-maelstrom/{source_prefix}/{}/trial-{trial}",
            scenario.name()
        ))
        .to_path_buf();
        collect_trial_evidence(
            &TrialEvidence {
                scenario,
                output_dir,
                namespace: &namespace,
                script: &script,
                state_dir: &state_dir,
                durable: &durable,
                deadline: total_deadline,
            },
            &output,
        )
    })();
    let cleanup = cleanup_state_directory(state_dir, total_deadline);
    finish_trial(outcome, cleanup)
}

fn finish_trial(
    outcome: Result<TrialOutcome, Box<dyn Error>>,
    cleanup: Result<(), Box<dyn Error>>,
) -> Result<TrialOutcome, Box<dyn Error>> {
    match (outcome, cleanup) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(cleanup_error)) => {
            Err(format!("clean Maelstrom scratch state: {cleanup_error}").into())
        }
        (Err(error), Err(cleanup_error)) => {
            Err(format!("{error}; Maelstrom scratch cleanup failed: {cleanup_error}").into())
        }
    }
}

fn collect_trial_evidence(
    evidence: &TrialEvidence<'_>,
    output: &process::ProcessOutput,
) -> Result<TrialOutcome, Box<dyn Error>> {
    let mut artifacts = vec![artifact::write(
        evidence.output_dir,
        &evidence.namespace.join("process.json"),
        "maelstrom-process-log",
        &process::json_log(evidence.scenario.name(), output)?,
    )?];
    artifacts.push(artifact::capture(
        evidence.output_dir,
        &evidence.namespace.join("inputs"),
        evidence.script,
        "maelstrom-runner",
    )?);
    artifacts.push(tool::capture_jar(evidence.output_dir, evidence.namespace)?);
    capture_binary(
        evidence.output_dir,
        evidence.namespace,
        Path::new("target/debug/rafter-maelstrom"),
        "maelstrom-binary",
        &mut artifacts,
    )?;
    if matches!(
        evidence.scenario,
        Scenario::Restart | Scenario::AppCrash | Scenario::Snapshot | Scenario::LeaseIsolation
    ) {
        capture_binary(
            evidence.output_dir,
            evidence.namespace,
            Path::new("target/debug/rafter-maelstrom-leader-restart-proxy"),
            "maelstrom-proxy-binary",
            &mut artifacts,
        )?;
    }
    let run_store = discover_store(evidence.state_dir, evidence.deadline);
    let (summary, error, markers) = match run_store {
        Ok(store) => {
            capture_tree(
                evidence.output_dir,
                &evidence.namespace.join("store"),
                &store,
                &mut artifacts,
                evidence.deadline,
            )?;
            let results = store.read_to_string_with_deadline(
                Path::new("results.edn"),
                OperationDeadline::at(evidence.deadline, "Maelstrom results read"),
            );
            let parsed = results
                .map_err(|error| format!("read Maelstrom results.edn: {error}"))
                .and_then(|source| maelstrom_edn::parse(&source));
            let markers = read_markers(&store, evidence.deadline)?;
            match parsed {
                Ok(summary) => (Some(summary), None, markers),
                Err(error) => (None, Some(error), markers),
            }
        }
        Err(error) => (None, Some(error), ScenarioMarkers::default()),
    };
    capture_tree(
        evidence.output_dir,
        &evidence.namespace.join("durable"),
        evidence.durable,
        &mut artifacts,
        evidence.deadline,
    )?;
    Ok(TrialOutcome {
        summary,
        error,
        process_succeeded: output.status.success() && !output.timed_out,
        process_timed_out: output.timed_out,
        markers,
        duration_ms: process::duration_ms(output.duration),
        peak_rss_kib: output.peak_rss_kib,
        artifacts,
    })
}

pub(in crate::producer) fn trial_process_timeout(
    configuration: &BTreeMap<String, String>,
) -> Result<Duration, Box<dyn Error>> {
    let workload_seconds = required_configuration(configuration, "duration_seconds")?
        .parse::<u64>()
        .map_err(|error| format!("invalid Maelstrom duration_seconds: {error}"))?;
    if workload_seconds == 0 {
        return Err("Maelstrom duration_seconds must be positive".into());
    }
    Duration::from_secs(workload_seconds)
        .checked_add(Duration::from_secs(2 * 60))
        .ok_or_else(|| "Maelstrom trial timeout overflowed".into())
}

fn trial_environment(
    configuration: &BTreeMap<String, String>,
    durable: &Path,
    script_dir: &Path,
    scenario: Scenario,
) -> Result<BTreeMap<String, String>, String> {
    let mut environment = process::base_environment();
    environment.extend([
        (
            "RAFTER_MAELSTROM_ROOT".to_owned(),
            durable.to_string_lossy().into_owned(),
        ),
        (
            "RAFTER_MAELSTROM_SCRIPT_DIR".to_owned(),
            script_dir.to_string_lossy().into_owned(),
        ),
        (
            "RAFTER_MAELSTROM_TIME_LIMIT".to_owned(),
            required_configuration(configuration, "duration_seconds")?.to_owned(),
        ),
        (
            "RAFTER_MAELSTROM_RATE".to_owned(),
            required_configuration(configuration, "rate")?.to_owned(),
        ),
        (
            "RAFTER_MAELSTROM_CONCURRENCY".to_owned(),
            scenario.concurrency().to_owned(),
        ),
    ]);
    if scenario == Scenario::LeaseIsolation {
        environment.extend([
            (
                "RAFTER_MAELSTROM_RESTART_MODE".to_owned(),
                "lease-isolation".to_owned(),
            ),
            ("RAFTER_MAELSTROM_LEASE_EVIDENCE".to_owned(), "1".to_owned()),
            (
                "RAFTER_MAELSTROM_TICK_INTERVAL_MS".to_owned(),
                required_configuration(configuration, "lease_tick_interval_ms")?.to_owned(),
            ),
            (
                "RAFTER_MAELSTROM_ELECTION_TIMEOUT_TICKS".to_owned(),
                required_configuration(configuration, "lease_election_timeout_ticks")?.to_owned(),
            ),
            (
                "RAFTER_MAELSTROM_HEARTBEAT_INTERVAL_TICKS".to_owned(),
                required_configuration(configuration, "lease_heartbeat_interval_ticks")?.to_owned(),
            ),
        ]);
    }
    Ok(environment)
}
