use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use crate::ArtifactRef;

use super::{artifact, maelstrom_edn, maelstrom_scenario::required_configuration, process};

pub(super) use super::maelstrom_scenario::Scenario;

pub(super) struct TrialOutcome {
    pub summary: Option<maelstrom_edn::MaelstromSummary>,
    pub error: Option<String>,
    pub process_succeeded: bool,
    pub process_timed_out: bool,
    pub markers: ScenarioMarkers,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub artifacts: Vec<ArtifactRef>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum LeaseTranscriptStatus {
    #[default]
    Missing,
    Complete,
    Incomplete,
    Violation,
    ViolationWithHarnessError,
    HarnessError,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ScenarioMarkers {
    pub membership_enter: u64,
    pub membership_leave: u64,
    pub membership_complete: u64,
    pub restarts: u64,
    pub post_restart_progress: u64,
    pub crashpoints: u64,
    pub post_crash_progress: u64,
    pub snapshots_compacted: u64,
    pub snapshots_applied: u64,
    pub post_restart_snapshots_applied: u64,
    pub lease_fast_path_read_ok: u64,
    pub lease_read_buffered: u64,
    pub lease_expired_while_leader: u64,
    pub lease_post_expiry_released: u64,
    pub lease_post_expiry_handler: u64,
    pub lease_post_expiry_unavailable: u64,
    pub lease_post_expiry_read_served: u64,
    pub lease_post_expiry_renewed: u64,
    pub lease_post_expiry_unexpected_error: u64,
    pub lease_duplicate_terminal: u64,
    pub lease_coverage_lost: u64,
    pub lease_history_probe_matches: u64,
    pub lease_history_probe_mismatches: u64,
    pub lease_sequence_complete: u64,
    pub lease_sequence_invalid: u64,
    pub lease_status: LeaseTranscriptStatus,
}

pub(super) fn run_trial(
    scenario: Scenario,
    trial: u64,
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
) -> Result<TrialOutcome, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let state_dir = reset_state_directory(
        Path::new("target/rafter-invariants/maelstrom")
            .join(source_prefix)
            .join(profile)
            .join(scenario.name())
            .join(format!("trial-{trial}")),
    )?;
    let durable = state_dir.join("durable");
    fs::create_dir_all(&durable)?;
    let script = fs::canonicalize(scenario.script())?;
    let environment = trial_environment(configuration, &durable, scenario)?;
    let timeout = trial_process_timeout(configuration)?;
    let output = process::timed_for_with_cap(
        process::ProcessKind::MaelstromTrial,
        script
            .to_str()
            .ok_or("Maelstrom script path is not UTF-8")?,
        &["--test-count".into(), "1".into()],
        &environment,
        &state_dir,
        Some(timeout),
    )?;
    let namespace = Path::new(&format!(
        "{profile}-maelstrom/{source_prefix}/{}/trial-{trial}",
        scenario.name()
    ))
    .to_path_buf();
    let mut artifacts = vec![artifact::write(
        output_dir,
        &namespace.join("process.json"),
        "maelstrom-process-log",
        &process::json_log(scenario.name(), &output)?,
    )?];
    let script_artifact = artifact::capture(
        output_dir,
        &namespace.join("inputs"),
        &script,
        "maelstrom-runner",
    )?;
    artifacts.push(script_artifact);
    artifacts.push(super::maelstrom_tool::capture_jar(output_dir, &namespace)?);
    capture_binary(
        output_dir,
        &namespace,
        Path::new("target/debug/rafter-maelstrom"),
        "maelstrom-binary",
        &mut artifacts,
    )?;
    if matches!(
        scenario,
        Scenario::Restart | Scenario::AppCrash | Scenario::Snapshot | Scenario::LeaseIsolation
    ) {
        capture_binary(
            output_dir,
            &namespace,
            Path::new("target/debug/rafter-maelstrom-leader-restart-proxy"),
            "maelstrom-proxy-binary",
            &mut artifacts,
        )?;
    }
    let run_store = discover_store(&state_dir);
    let (summary, error, markers) = match run_store {
        Ok(store) => {
            capture_tree(output_dir, &namespace.join("store"), &store, &mut artifacts)?;
            let results = fs::read_to_string(store.join("results.edn"));
            let parsed = results
                .map_err(|error| format!("read Maelstrom results.edn: {error}"))
                .and_then(|source| maelstrom_edn::parse(&source));
            let markers = read_markers(&store)?;
            match parsed {
                Ok(summary) => (Some(summary), None, markers),
                Err(error) => (None, Some(error), markers),
            }
        }
        Err(error) => (None, Some(error), ScenarioMarkers::default()),
    };
    capture_tree(
        output_dir,
        &namespace.join("durable"),
        &durable,
        &mut artifacts,
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

fn trial_process_timeout(
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
    scenario: Scenario,
) -> Result<BTreeMap<String, String>, String> {
    let mut environment = process::base_environment();
    environment.extend([
        (
            "RAFTER_MAELSTROM_ROOT".to_owned(),
            durable.to_string_lossy().into_owned(),
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

fn reset_state_directory(path: PathBuf) -> Result<PathBuf, Box<dyn Error>> {
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }
    fs::create_dir_all(&path)?;
    Ok(fs::canonicalize(path)?)
}

fn capture_binary(
    output_dir: &Path,
    namespace: &Path,
    binary: &Path,
    kind: &str,
    artifacts: &mut Vec<ArtifactRef>,
) -> Result<(), Box<dyn Error>> {
    if !binary.is_file() {
        return Err(format!("Maelstrom run did not produce {}", binary.display()).into());
    }
    artifacts.push(artifact::capture(
        output_dir,
        &namespace.join("inputs"),
        binary,
        kind,
    )?);
    Ok(())
}

fn discover_store(state_dir: &Path) -> Result<PathBuf, String> {
    let root = state_dir.join("store/lin-kv");
    let mut stores = fs::read_dir(&root)
        .map_err(|error| format!("read Maelstrom store {}: {error}", root.display()))?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    stores.sort();
    match stores.as_slice() {
        [store] => Ok(store.clone()),
        _ => Err(format!(
            "expected one Maelstrom retained store, found {}",
            stores.len()
        )),
    }
}

fn capture_tree(
    output_dir: &Path,
    namespace: &Path,
    root: &Path,
    artifacts: &mut Vec<ArtifactRef>,
) -> Result<(), Box<dyn Error>> {
    for file in files_below(root)? {
        let relative = file.strip_prefix(root)?;
        let kind = if relative == Path::new("results.edn") {
            "maelstrom-results"
        } else if relative.starts_with("node-logs") {
            "maelstrom-node-log"
        } else if namespace.ends_with("durable") {
            "maelstrom-durable-file"
        } else {
            "maelstrom-store-file"
        };
        artifacts.push(artifact::capture_as(
            output_dir,
            &namespace.join(relative),
            &file,
            kind,
        )?);
    }
    Ok(())
}

fn files_below(root: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    Ok(files)
}

fn read_markers(store: &Path) -> Result<ScenarioMarkers, Box<dyn Error>> {
    let mut markers = ScenarioMarkers::default();
    let mut lease_events = Vec::new();
    let mut lease_parse_errors = 0;
    for file in files_below(&store.join("node-logs"))? {
        let source_node = file
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or("Maelstrom node log has no UTF-8 file stem")?;
        scan_markers(
            &fs::read_to_string(&file)?,
            source_node,
            &mut markers,
            &mut lease_events,
            &mut lease_parse_errors,
        );
    }
    finish_lease_transcript(&mut markers, &lease_events, lease_parse_errors);
    let history = fs::read_to_string(store.join("history.edn")).ok();
    bind_lease_history(&mut markers, &lease_events, history.as_deref());
    Ok(markers)
}

fn scan_markers(
    source: &str,
    source_node: &str,
    markers: &mut ScenarioMarkers,
    lease_events: &mut Vec<LeaseMarker>,
    lease_parse_errors: &mut u64,
) {
    let mut saw_restart = false;
    let mut saw_crash = false;
    for line in source.lines() {
        markers.membership_enter += u64::from(line.contains("action=enter-joint"));
        markers.membership_leave += u64::from(line.contains("action=leave-joint"));
        markers.membership_complete += u64::from(line.contains("complete target="));
        if line.contains("proxy restarting child") {
            markers.restarts += 1;
            saw_restart = true;
        }
        if line.contains("crashpoint=RAFTER_MAELSTROM_CRASH_AFTER_APP_PERSIST_ONCE fired") {
            markers.crashpoints += 1;
            saw_crash = true;
        }
        let progress = line.contains(" role=leader ") || line.contains("compacted snapshot");
        markers.post_restart_progress += u64::from(saw_restart && progress);
        markers.post_crash_progress += u64::from(saw_crash && progress);
        markers.snapshots_compacted += u64::from(line.contains("compacted snapshot"));
        markers.snapshots_applied += u64::from(line.contains("applied snapshot"));
        markers.post_restart_snapshots_applied +=
            u64::from(saw_restart && line.contains("applied snapshot"));
        if line.starts_with("rafter-maelstrom lease-isolation ") {
            match LeaseMarker::parse(line, source_node) {
                Ok(event) => {
                    bump_lease_count(markers, &event.phase);
                    lease_events.push(event);
                }
                Err(()) => *lease_parse_errors += 1,
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LeaseMarker {
    source_node: String,
    seq: u64,
    node: String,
    term: u64,
    phase: String,
    client: String,
    msg_id: u64,
    code: Option<u64>,
    reason: Option<String>,
}

impl LeaseMarker {
    fn parse(line: &str, source_node: &str) -> Result<Self, ()> {
        let fields = line
            .strip_prefix("rafter-maelstrom lease-isolation ")
            .ok_or(())?
            .split_ascii_whitespace()
            .try_fold(BTreeMap::new(), |mut fields, part| {
                let (name, value) = part.split_once('=').ok_or(())?;
                if fields.insert(name, value).is_some() {
                    return Err(());
                }
                Ok(fields)
            })?;
        let allowed = BTreeSet::from([
            "seq", "node", "term", "phase", "client", "msg_id", "code", "reason",
        ]);
        if fields.keys().any(|field| !allowed.contains(field)) {
            return Err(());
        }
        let phase = required(&fields, "phase")?.to_owned();
        let code = fields
            .get("code")
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ())?;
        let reason = fields.get("reason").map(|value| (*value).to_owned());
        let known_phase = matches!(
            phase.as_str(),
            "fast-path-read-ok"
                | "read-buffered"
                | "lease-expired"
                | "post-expiry-released"
                | "post-expiry-handler"
                | "post-expiry-unavailable"
                | "post-expiry-read-served-violation"
                | "post-expiry-renewed-violation"
                | "post-expiry-unexpected-error"
                | "post-expiry-duplicate-terminal"
                | "coverage-lost"
        );
        if !known_phase
            || (phase == "post-expiry-unexpected-error") != code.is_some()
            || (phase == "coverage-lost") != reason.is_some()
        {
            return Err(());
        }
        Ok(Self {
            source_node: source_node.to_owned(),
            seq: required(&fields, "seq")?.parse().map_err(|_| ())?,
            node: required(&fields, "node")?.to_owned(),
            term: required(&fields, "term")?.parse().map_err(|_| ())?,
            phase,
            client: required(&fields, "client")?.to_owned(),
            msg_id: required(&fields, "msg_id")?.parse().map_err(|_| ())?,
            code,
            reason,
        })
    }

    fn request(&self) -> (&str, u64) {
        (&self.client, self.msg_id)
    }
}

fn required<'a>(fields: &'a BTreeMap<&str, &str>, name: &str) -> Result<&'a str, ()> {
    fields.get(name).copied().ok_or(())
}

fn bump_lease_count(markers: &mut ScenarioMarkers, phase: &str) {
    let counter = match phase {
        "fast-path-read-ok" => &mut markers.lease_fast_path_read_ok,
        "read-buffered" => &mut markers.lease_read_buffered,
        "lease-expired" => &mut markers.lease_expired_while_leader,
        "post-expiry-released" => &mut markers.lease_post_expiry_released,
        "post-expiry-handler" => &mut markers.lease_post_expiry_handler,
        "post-expiry-unavailable" => &mut markers.lease_post_expiry_unavailable,
        "post-expiry-read-served-violation" => &mut markers.lease_post_expiry_read_served,
        "post-expiry-renewed-violation" => &mut markers.lease_post_expiry_renewed,
        "post-expiry-unexpected-error" => &mut markers.lease_post_expiry_unexpected_error,
        "post-expiry-duplicate-terminal" => &mut markers.lease_duplicate_terminal,
        "coverage-lost" => &mut markers.lease_coverage_lost,
        _ => return,
    };
    *counter += 1;
}

fn finish_lease_transcript(
    markers: &mut ScenarioMarkers,
    events: &[LeaseMarker],
    parse_errors: u64,
) {
    let derived = match validate_lease_transcript(events) {
        Ok(LeaseTranscriptStatus::Complete) => {
            markers.lease_sequence_complete = 1;
            LeaseTranscriptStatus::Complete
        }
        Ok(LeaseTranscriptStatus::Missing) => LeaseTranscriptStatus::Missing,
        Ok(status) => {
            markers.lease_sequence_invalid = 1;
            status
        }
        Err(()) => {
            markers.lease_sequence_invalid = 1;
            LeaseTranscriptStatus::HarnessError
        }
    };
    if parse_errors > 0 {
        markers.lease_sequence_complete = 0;
        markers.lease_sequence_invalid =
            markers.lease_sequence_invalid.saturating_add(parse_errors);
        markers.lease_status = if matches!(
            derived,
            LeaseTranscriptStatus::Violation | LeaseTranscriptStatus::ViolationWithHarnessError
        ) {
            LeaseTranscriptStatus::ViolationWithHarnessError
        } else {
            LeaseTranscriptStatus::HarnessError
        };
    } else {
        markers.lease_status = derived;
    }
}

fn bind_lease_history(
    markers: &mut ScenarioMarkers,
    events: &[LeaseMarker],
    history: Option<&str>,
) {
    if markers.lease_status != LeaseTranscriptStatus::Complete {
        return;
    }
    let probe = events.iter().find(|event| event.phase == "read-buffered");
    let matches = probe
        .zip(history)
        .and_then(|(probe, history)| {
            maelstrom_edn::lease_probe_completion_count(history, &probe.client, probe.msg_id).ok()
        })
        .unwrap_or_default();
    if matches == 1 {
        markers.lease_history_probe_matches = 1;
    } else {
        markers.lease_history_probe_mismatches = 1;
        markers.lease_sequence_complete = 0;
        markers.lease_sequence_invalid = 1;
        markers.lease_status = LeaseTranscriptStatus::HarnessError;
    }
}

fn validate_lease_transcript(events: &[LeaseMarker]) -> Result<LeaseTranscriptStatus, ()> {
    if events.is_empty() {
        return Ok(LeaseTranscriptStatus::Missing);
    }
    let first = &events[0];
    if first.phase != "fast-path-read-ok" || first.source_node != first.node || first.seq != 1 {
        return Err(());
    }
    let node = first.node.as_str();
    let term = first.term;
    let fast = first.request();
    let mut probe = None;
    let mut expired = false;
    let mut released = false;
    let mut handled = false;
    let mut terminal = None;
    let mut duplicate_terminal = false;
    for (index, event) in events.iter().enumerate().skip(1) {
        if event.seq != (index + 1) as u64
            || event.source_node != node
            || event.node != node
            || event.term != term
        {
            return Err(());
        }
        match event.phase.as_str() {
            "lease-expired"
                if !expired && probe.is_none() && !released && event.request() == fast =>
            {
                expired = true;
            }
            "read-buffered" if expired && probe.is_none() && !released => {
                if event.request() == fast {
                    return Err(());
                }
                probe = Some(event.request());
            }
            "post-expiry-released" if expired && probe == Some(event.request()) && !released => {
                released = true;
            }
            "post-expiry-handler" if released && probe == Some(event.request()) && !handled => {
                handled = true;
            }
            "post-expiry-unavailable"
                if handled && probe == Some(event.request()) && terminal.is_none() =>
            {
                terminal = Some(LeaseTranscriptStatus::Complete);
            }
            "post-expiry-read-served-violation"
                if handled && probe == Some(event.request()) && terminal.is_none() =>
            {
                terminal = Some(LeaseTranscriptStatus::Violation);
            }
            "post-expiry-renewed-violation"
                if expired && event.request() == probe.unwrap_or(fast) && terminal.is_none() =>
            {
                terminal = Some(LeaseTranscriptStatus::Violation);
            }
            "post-expiry-unexpected-error"
                if handled
                    && probe == Some(event.request())
                    && event.code.is_some()
                    && terminal.is_none() =>
            {
                terminal = Some(LeaseTranscriptStatus::HarnessError);
            }
            "post-expiry-duplicate-terminal"
                if released && probe == Some(event.request()) && !duplicate_terminal =>
            {
                duplicate_terminal = true;
            }
            "coverage-lost" if index + 1 == events.len() && event.reason.is_some() => {
                terminal = Some(LeaseTranscriptStatus::Incomplete);
            }
            _ => return Err(()),
        }
    }
    Ok(match (terminal, duplicate_terminal) {
        (Some(LeaseTranscriptStatus::Violation), true) => {
            LeaseTranscriptStatus::ViolationWithHarnessError
        }
        (Some(LeaseTranscriptStatus::Violation), false) => LeaseTranscriptStatus::Violation,
        (Some(LeaseTranscriptStatus::Complete), false) => LeaseTranscriptStatus::Complete,
        (Some(LeaseTranscriptStatus::Incomplete) | None, false) => {
            LeaseTranscriptStatus::Incomplete
        }
        (Some(LeaseTranscriptStatus::HarnessError), _) | (_, true) => {
            LeaseTranscriptStatus::HarnessError
        }
        (
            Some(LeaseTranscriptStatus::Missing | LeaseTranscriptStatus::ViolationWithHarnessError),
            _,
        ) => return Err(()),
    })
}

#[cfg(test)]
mod lease_transcript_tests {
    use std::{collections::BTreeMap, time::Duration};

    use super::{
        bind_lease_history, finish_lease_transcript, trial_process_timeout,
        validate_lease_transcript, LeaseMarker, LeaseTranscriptStatus, ScenarioMarkers,
    };

    fn good() -> Vec<LeaseMarker> {
        [
            "seq=1 node=n1 term=3 phase=fast-path-read-ok client=c0 msg_id=7",
            "seq=2 node=n1 term=3 phase=lease-expired client=c0 msg_id=7",
            "seq=3 node=n1 term=3 phase=read-buffered client=c1 msg_id=11",
            "seq=4 node=n1 term=3 phase=post-expiry-released client=c1 msg_id=11",
            "seq=5 node=n1 term=3 phase=post-expiry-handler client=c1 msg_id=11",
            "seq=6 node=n1 term=3 phase=post-expiry-unavailable client=c1 msg_id=11",
        ]
        .into_iter()
        .map(|fields| {
            LeaseMarker::parse(&format!("rafter-maelstrom lease-isolation {fields}"), "n1")
                .expect("fixture parses")
        })
        .collect()
    }

    #[test]
    fn trial_timeout_is_bound_to_workload_duration_with_teardown_time() {
        let configuration = BTreeMap::from([("duration_seconds".to_owned(), "45".to_owned())]);
        assert_eq!(
            trial_process_timeout(&configuration).expect("valid trial timeout"),
            Duration::from_secs(45 + 2 * 60)
        );
        assert!(trial_process_timeout(&BTreeMap::from([(
            "duration_seconds".to_owned(),
            "0".to_owned()
        )]))
        .is_err());
        assert!(trial_process_timeout(&BTreeMap::from([(
            "duration_seconds".to_owned(),
            "not-a-duration".to_owned()
        )]))
        .is_err());
        assert!(trial_process_timeout(&BTreeMap::new()).is_err());
    }

    #[test]
    fn accepts_only_expiry_before_the_correlated_buffered_read() {
        assert_eq!(
            validate_lease_transcript(&good()),
            Ok(LeaseTranscriptStatus::Complete)
        );
        let mut buffered_first = good();
        buffered_first.swap(1, 2);
        buffered_first[1].seq = 2;
        buffered_first[2].seq = 3;
        assert!(validate_lease_transcript(&buffered_first).is_err());
    }

    #[test]
    fn rejects_cross_node_cross_term_and_uncorrelated_sequences() {
        let mut cross_node = good();
        cross_node[3].node = "n2".to_owned();
        assert!(validate_lease_transcript(&cross_node).is_err());
        let mut cross_term = good();
        cross_term[4].term = 4;
        assert!(validate_lease_transcript(&cross_term).is_err());
        let mut uncorrelated = good();
        uncorrelated[5].msg_id = 99;
        assert!(validate_lease_transcript(&uncorrelated).is_err());
    }

    #[test]
    fn rejects_out_of_order_missing_and_duplicate_events() {
        let mut out_of_order = good();
        out_of_order.swap(3, 4);
        out_of_order[3].seq = 4;
        out_of_order[4].seq = 5;
        assert!(validate_lease_transcript(&out_of_order).is_err());
        assert_eq!(
            validate_lease_transcript(&good()[..5]),
            Ok(LeaseTranscriptStatus::Incomplete)
        );
        let mut duplicate = good();
        duplicate.insert(2, duplicate[1].clone());
        for (index, event) in duplicate.iter_mut().enumerate() {
            event.seq = (index + 1) as u64;
        }
        assert!(validate_lease_transcript(&duplicate).is_err());
    }

    #[test]
    fn classifies_read_ok_and_renewal_as_violations_only() {
        let mut served = good();
        served[5].phase = "post-expiry-read-served-violation".to_owned();
        assert_eq!(
            validate_lease_transcript(&served),
            Ok(LeaseTranscriptStatus::Violation)
        );
        let mut renewed = good();
        renewed.truncate(2);
        renewed.push(LeaseMarker::parse(
            "rafter-maelstrom lease-isolation seq=3 node=n1 term=3 phase=post-expiry-renewed-violation client=c0 msg_id=7",
            "n1",
        ).expect("fixture parses"));
        assert_eq!(
            validate_lease_transcript(&renewed),
            Ok(LeaseTranscriptStatus::Violation)
        );
    }

    #[test]
    fn unexpected_error_is_harness_error_and_malformed_fields_fail_closed() {
        let mut unexpected = good();
        unexpected[5] = LeaseMarker::parse(
            "rafter-maelstrom lease-isolation seq=6 node=n1 term=3 phase=post-expiry-unexpected-error client=c1 msg_id=11 code=20",
            "n1",
        ).expect("fixture parses");
        assert_eq!(
            validate_lease_transcript(&unexpected),
            Ok(LeaseTranscriptStatus::HarnessError)
        );
        assert!(LeaseMarker::parse(
            "rafter-maelstrom lease-isolation seq=1 node=n1 term=3 phase=fast-path-read-ok client=c0 msg_id=7 extra=x",
            "n1",
        ).is_err());
    }

    #[test]
    fn duplicate_terminal_marker_fails_closed_after_either_terminal_kind() {
        for terminal in [
            "post-expiry-unavailable",
            "post-expiry-read-served-violation",
        ] {
            let mut events = good();
            events[5].phase = terminal.to_owned();
            events.push(LeaseMarker::parse(
                "rafter-maelstrom lease-isolation seq=7 node=n1 term=3 phase=post-expiry-duplicate-terminal client=c1 msg_id=11",
                "n1",
            ).expect("fixture parses"));
            let expected = if terminal == "post-expiry-read-served-violation" {
                LeaseTranscriptStatus::ViolationWithHarnessError
            } else {
                LeaseTranscriptStatus::HarnessError
            };
            assert_eq!(validate_lease_transcript(&events), Ok(expected));

            let mut duplicate_before_handler = good();
            duplicate_before_handler[5].phase = terminal.to_owned();
            duplicate_before_handler.insert(4, LeaseMarker::parse(
                "rafter-maelstrom lease-isolation seq=5 node=n1 term=3 phase=post-expiry-duplicate-terminal client=c1 msg_id=11",
                "n1",
            ).expect("fixture parses"));
            for (index, event) in duplicate_before_handler.iter_mut().enumerate() {
                event.seq = (index + 1) as u64;
            }
            assert_eq!(
                validate_lease_transcript(&duplicate_before_handler),
                Ok(expected)
            );
        }
    }

    #[test]
    fn malformed_marker_after_read_served_preserves_violation_and_harness_error() {
        let mut events = good();
        events[5].phase = "post-expiry-read-served-violation".to_owned();
        let mut markers = ScenarioMarkers::default();
        finish_lease_transcript(&mut markers, &events, 1);
        assert_eq!(
            markers.lease_status,
            LeaseTranscriptStatus::ViolationWithHarnessError
        );
        assert_eq!(markers.lease_sequence_invalid, 2);
    }

    #[test]
    fn retained_history_must_match_the_exact_probe_identity_once() {
        let events = good();
        let exact = concat!(
            "{:index 1 :type :invoke :process 0 :f :read :value nil}\n",
            "{:index 2 :type :fail :process 0 :f :read :value nil :error ",
            "[:temporarily-unavailable \"LeadershipLost [rafter-lease-probe client=c1 msg_id=11 code=11]\"]}"
        );
        let swapped = exact.replace("client=c1", "client=c2");

        let mut matched = ScenarioMarkers {
            lease_status: LeaseTranscriptStatus::Complete,
            lease_sequence_complete: 1,
            ..ScenarioMarkers::default()
        };
        bind_lease_history(&mut matched, &events, Some(exact));
        assert_eq!(matched.lease_history_probe_matches, 1);
        assert_eq!(matched.lease_status, LeaseTranscriptStatus::Complete);

        for history in [None, Some(swapped.as_str())] {
            let mut rejected = ScenarioMarkers {
                lease_status: LeaseTranscriptStatus::Complete,
                lease_sequence_complete: 1,
                ..ScenarioMarkers::default()
            };
            bind_lease_history(&mut rejected, &events, history);
            assert_eq!(rejected.lease_history_probe_mismatches, 1);
            assert_eq!(rejected.lease_status, LeaseTranscriptStatus::HarnessError);
        }
    }
}
