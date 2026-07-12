use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use edn_format::{parse_str, Keyword, Value};

use crate::{
    aggregate::AggregateError, producer::maelstrom_edn::MaelstromSummary, ArtifactRef, CheckReceipt,
};

#[rustfmt::skip]
const MARKERS: [&str; 25] = ["membership_enter", "membership_leave", "membership_complete", "restarts", "post_restart_progress", "crashpoints", "post_crash_progress", "snapshots_compacted", "snapshots_applied", "post_restart_snapshots_applied", "lease_fast_path_read_ok", "lease_read_buffered", "lease_expired_while_leader", "lease_post_expiry_released", "lease_post_expiry_handler", "lease_post_expiry_unavailable", "lease_post_expiry_read_served", "lease_post_expiry_renewed", "lease_post_expiry_unexpected_error", "lease_duplicate_terminal", "lease_coverage_lost", "lease_history_probe_matches", "lease_history_probe_mismatches", "lease_sequence_complete", "lease_sequence_invalid"];
#[rustfmt::skip]
const SIMPLE_MARKERS: [(&str, &str); 5] = [("membership_enter", "action=enter-joint"), ("membership_leave", "action=leave-joint"), ("membership_complete", "complete target="), ("snapshots_compacted", "compacted snapshot"), ("snapshots_applied", "applied snapshot")];

pub(super) fn verify_matches_file(
    artifact: &ArtifactRef,
    source: impl AsRef<Path>,
    root: &Path,
) -> Result<(), AggregateError> {
    let captured = fs::read(root.join(&artifact.path))
        .map_err(|read_error| error(format!("read captured {}: {read_error}", artifact.kind)))?;
    let current = fs::read(source.as_ref()).map_err(|read_error| {
        error(format!(
            "read source-bound {} input {}: {read_error}",
            artifact.kind,
            source.as_ref().display()
        ))
    })?;
    if captured == current {
        Ok(())
    } else {
        Err(error(format!(
            "captured {} does not match the source-bound input",
            artifact.kind
        )))
    }
}

pub(super) fn scenario_script(scenario: &str) -> Result<&'static str, AggregateError> {
    match scenario {
        "base" => Ok("scripts/maelstrom-lin-kv"),
        "membership" => Ok("scripts/maelstrom-lin-kv-membership-change"),
        "restart" => Ok("scripts/maelstrom-lin-kv-repeated-restart"),
        "app-crash" => Ok("scripts/maelstrom-lin-kv-app-persist-crash"),
        "snapshot" => Ok("scripts/maelstrom-lin-kv-forced-snapshot"),
        "lease-isolation" => Ok("scripts/maelstrom-lin-kv-lease-isolation"),
        _ => Err(error("unknown Maelstrom scenario")),
    }
}

pub(super) fn group_trials(
    check: &CheckReceipt,
) -> Result<BTreeMap<u64, Vec<&ArtifactRef>>, AggregateError> {
    let mut grouped = BTreeMap::<u64, Vec<&ArtifactRef>>::new();
    for artifact in &check.artifacts {
        let Some(trial) = trial_number(Path::new(&artifact.path))? else {
            return Err(error("Maelstrom artifact lacks a trial path"));
        };
        grouped.entry(trial).or_default().push(artifact);
    }
    Ok(grouped)
}

fn trial_number(path: &Path) -> Result<Option<u64>, AggregateError> {
    let trials = path
        .components()
        .filter_map(|component| component.as_os_str().to_str()?.strip_prefix("trial-"))
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|parse_error| error(format!("parse Maelstrom trial path: {parse_error}")))?;
    match trials.as_slice() {
        [] => Ok(None),
        [trial] => Ok(Some(*trial)),
        _ => Err(error(format!(
            "ambiguous Maelstrom trial path {}",
            path.display()
        ))),
    }
}

pub(super) fn unique<'a>(
    artifacts: &'a [&ArtifactRef],
    kind: &str,
) -> Result<&'a ArtifactRef, AggregateError> {
    let matching = artifacts
        .iter()
        .filter(|artifact| artifact.kind == kind)
        .collect::<Vec<_>>();
    match matching.as_slice() {
        [artifact] => Ok(artifact),
        [] => Err(error(format!("Maelstrom trial artifact {kind} is missing"))),
        _ => Err(error(format!(
            "Maelstrom trial artifact {kind} is ambiguous"
        ))),
    }
}

pub(super) fn parse_results(
    artifact: &ArtifactRef,
    root: &Path,
) -> Result<MaelstromSummary, AggregateError> {
    let source = read(artifact, root)?;
    crate::producer::maelstrom_edn::parse(&source)
        .map_err(|parse_error| error(format!("parse Maelstrom results: {parse_error}")))
}

pub(super) fn parse_process(
    artifact: &ArtifactRef,
    root: &Path,
) -> Result<crate::producer::ProcessLog, AggregateError> {
    serde_json::from_str(&read(artifact, root)?)
        .map_err(|parse_error| error(format!("parse Maelstrom process log: {parse_error}")))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum LeaseArtifactStatus {
    #[default]
    Missing,
    Complete,
    Incomplete,
    Violation,
    ViolationWithHarnessError,
    HarnessError,
}

pub(super) struct MarkerScan {
    pub values: BTreeMap<&'static str, u64>,
    pub lease_status: LeaseArtifactStatus,
    pub lease_probe: Option<LeaseProbe>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LeaseProbe {
    pub client: String,
    pub message: u64,
}

pub(super) fn scan_node_logs(
    artifacts: &[&ArtifactRef],
    root: &Path,
) -> Result<MarkerScan, AggregateError> {
    let mut values = MARKERS.into_iter().map(|name| (name, 0)).collect();
    let mut lease_events = Vec::new();
    let mut lease_parse_errors = 0;
    let logs = artifacts
        .iter()
        .filter(|artifact| artifact.kind == "maelstrom-node-log")
        .collect::<Vec<_>>();
    if logs.is_empty() {
        return Err(error("Maelstrom trial has no captured node logs"));
    }
    for artifact in logs {
        let source_node = Path::new(&artifact.path)
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| error("Maelstrom node log artifact has no UTF-8 file stem"))?;
        scan_markers(
            &read(artifact, root)?,
            source_node,
            &mut values,
            &mut lease_events,
            &mut lease_parse_errors,
        );
    }
    let lease_status = finalize_lease_scan(&mut values, &lease_events, lease_parse_errors);
    let lease_probe = lease_events
        .iter()
        .find(|event| event.phase == "read-buffered")
        .map(|event| LeaseProbe {
            client: event.client.clone(),
            message: event.message,
        });
    Ok(MarkerScan {
        values,
        lease_status,
        lease_probe,
    })
}

pub(super) fn bind_lease_history(
    scan: &mut MarkerScan,
    artifacts: &[&ArtifactRef],
    root: &Path,
) -> Result<(), AggregateError> {
    if scan.lease_status != LeaseArtifactStatus::Complete {
        return Ok(());
    }
    let histories = artifacts
        .iter()
        .filter(|artifact| {
            artifact.kind == "maelstrom-store-file"
                && Path::new(&artifact.path).ends_with("store/history.edn")
        })
        .collect::<Vec<_>>();
    let matches = match (histories.as_slice(), scan.lease_probe.as_ref()) {
        ([history], Some(probe)) => {
            history_completion_count(&read(history, root)?, &probe.client, probe.message)?
        }
        _ => 0,
    };
    if matches == 1 {
        add_raw(&mut scan.values, "lease_history_probe_matches", 1);
    } else {
        add_raw(&mut scan.values, "lease_history_probe_mismatches", 1);
        scan.values.insert("lease_sequence_complete", 0);
        scan.values.insert("lease_sequence_invalid", 1);
        scan.lease_status = LeaseArtifactStatus::HarnessError;
    }
    Ok(())
}

fn history_completion_count(
    source: &str,
    client: &str,
    message: u64,
) -> Result<u64, AggregateError> {
    let expected = format!("[rafter-lease-probe client={client} msg_id={message} code=11]");
    let mut pending = BTreeMap::<Value, (Value, Value)>::new();
    let mut completions = 0;
    let mut last_index = None;
    for line in source.lines().filter(|line| !line.trim().is_empty()) {
        let parsed = parse_str(line)
            .map_err(|parse_error| error(format!("parse Maelstrom history: {parse_error}")))?;
        let Value::Map(operation) = parsed else {
            return Err(error("Maelstrom history operation is not an EDN map"));
        };
        let index = history_unsigned(history_field(&operation, "index")?)?;
        if last_index.is_some_and(|previous| index <= previous) {
            return Err(error("Maelstrom history indices are not strictly ordered"));
        }
        last_index = Some(index);
        let process = history_field(&operation, "process")?.clone();
        let function = history_field(&operation, "f")?.clone();
        let operation_value = history_field(&operation, "value")?.clone();
        let operation_type = history_keyword_name(history_field(&operation, "type")?)?;
        if operation_type == "invoke" {
            if pending
                .insert(process, (function, operation_value))
                .is_some()
            {
                return Err(error("Maelstrom process invoked twice without a terminal"));
            }
            continue;
        }
        if !matches!(operation_type, "ok" | "fail" | "info") {
            return Err(error(format!(
                "unknown Maelstrom history operation type :{operation_type}"
            )));
        }
        let (invoked_function, invoked_value) = pending
            .remove(&process)
            .ok_or_else(|| error("Maelstrom terminal has no preceding invoke"))?;
        if invoked_function != function
            || !history_operation_identity_matches(&function, &invoked_value, &operation_value)
        {
            return Err(error(
                "Maelstrom terminal function does not match its invoke",
            ));
        }
        let tagged = match operation.get(&history_keyword("error")) {
            Some(Value::Vector(error)) if error.len() == 2 => {
                matches!(&error[0], Value::Keyword(value) if value.name() == "temporarily-unavailable")
                    && matches!(&error[1], Value::String(text) if text.ends_with(&expected))
            }
            _ => false,
        };
        if tagged {
            if operation_type != "fail"
                || !matches!(&function, Value::Keyword(value) if value.name() == "read")
                || operation_value != invoked_value
            {
                return Err(error(
                    "lease probe tag appeared outside its exact failed read completion",
                ));
            }
            completions += 1;
        }
    }
    if !pending.is_empty() {
        return Err(error("Maelstrom history ended with an unterminated invoke"));
    }
    Ok(completions)
}

fn history_operation_identity_matches(function: &Value, invoked: &Value, terminal: &Value) -> bool {
    if matches!(function, Value::Keyword(value) if value.name() == "read") {
        match (invoked, terminal) {
            (Value::Vector(invoked), Value::Vector(terminal)) => {
                invoked.first() == terminal.first()
            }
            _ => invoked == terminal,
        }
    } else {
        invoked == terminal
    }
}

fn history_keyword(name: &str) -> Value {
    Value::Keyword(Keyword::from_name(name))
}

fn history_field<'a>(
    operation: &'a BTreeMap<Value, Value>,
    name: &str,
) -> Result<&'a Value, AggregateError> {
    operation
        .get(&history_keyword(name))
        .ok_or_else(|| error(format!("Maelstrom history operation omitted :{name}")))
}

fn history_keyword_name(value: &Value) -> Result<&str, AggregateError> {
    match value {
        Value::Keyword(value) => Ok(value.name()),
        _ => Err(error(format!("expected EDN keyword, got {value}"))),
    }
}

fn history_unsigned(value: &Value) -> Result<u64, AggregateError> {
    match value {
        Value::Integer(value) => u64::try_from(*value)
            .map_err(|_| error(format!("expected nonnegative history index, got {value}"))),
        _ => Err(error(format!(
            "expected integer history index, got {value}"
        ))),
    }
}

fn scan_markers(
    source: &str,
    source_node: &str,
    values: &mut BTreeMap<&'static str, u64>,
    lease_events: &mut Vec<ArtifactLeaseMarker>,
    lease_parse_errors: &mut u64,
) {
    let mut saw_restart = false;
    let mut saw_crash = false;
    for line in source.lines() {
        for (name, needle) in SIMPLE_MARKERS {
            bump(values, name, line.contains(needle));
        }
        if line.contains("proxy restarting child") {
            bump(values, "restarts", true);
            saw_restart = true;
        }
        if line.contains("crashpoint=RAFTER_MAELSTROM_CRASH_AFTER_APP_PERSIST_ONCE fired") {
            bump(values, "crashpoints", true);
            saw_crash = true;
        }
        let progress = line.contains(" role=leader ") || line.contains("compacted snapshot");
        bump(values, "post_restart_progress", saw_restart && progress);
        bump(values, "post_crash_progress", saw_crash && progress);
        bump(
            values,
            "post_restart_snapshots_applied",
            saw_restart && line.contains("applied snapshot"),
        );
        if line.starts_with("rafter-maelstrom lease-isolation ") {
            match ArtifactLeaseMarker::parse(line, source_node) {
                Ok(event) => {
                    bump_lease_value(values, &event.phase);
                    lease_events.push(event);
                }
                Err(()) => *lease_parse_errors += 1,
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ArtifactLeaseMarker {
    source_node: String,
    sequence: u64,
    node: String,
    term: u64,
    phase: String,
    client: String,
    message: u64,
    code: Option<u64>,
    reason: Option<String>,
}

impl ArtifactLeaseMarker {
    fn parse(line: &str, source_node: &str) -> Result<Self, ()> {
        let body = line
            .strip_prefix("rafter-maelstrom lease-isolation ")
            .ok_or(())?;
        let mut fields = BTreeMap::new();
        for component in body.split_ascii_whitespace() {
            let (key, value) = component.split_once('=').ok_or(())?;
            if fields.insert(key, value).is_some() {
                return Err(());
            }
        }
        let allowed = BTreeSet::from([
            "seq", "node", "term", "phase", "client", "msg_id", "code", "reason",
        ]);
        if fields.keys().any(|key| !allowed.contains(key)) {
            return Err(());
        }
        let phase = take(&fields, "phase")?.to_owned();
        let code = fields
            .get("code")
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| ())?;
        let reason = fields.get("reason").map(|value| (*value).to_owned());
        if !known_lease_phase(&phase)
            || (phase == "post-expiry-unexpected-error") != code.is_some()
            || (phase == "coverage-lost") != reason.is_some()
        {
            return Err(());
        }
        Ok(Self {
            source_node: source_node.to_owned(),
            sequence: take(&fields, "seq")?.parse().map_err(|_| ())?,
            node: take(&fields, "node")?.to_owned(),
            term: take(&fields, "term")?.parse().map_err(|_| ())?,
            phase,
            client: take(&fields, "client")?.to_owned(),
            message: take(&fields, "msg_id")?.parse().map_err(|_| ())?,
            code,
            reason,
        })
    }

    fn identity(&self) -> (&str, u64) {
        (&self.client, self.message)
    }
}

fn take<'a>(fields: &'a BTreeMap<&str, &str>, key: &str) -> Result<&'a str, ()> {
    fields.get(key).copied().ok_or(())
}

fn known_lease_phase(phase: &str) -> bool {
    matches!(
        phase,
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
    )
}

fn bump_lease_value(values: &mut BTreeMap<&'static str, u64>, phase: &str) {
    let name = match phase {
        "fast-path-read-ok" => "lease_fast_path_read_ok",
        "read-buffered" => "lease_read_buffered",
        "lease-expired" => "lease_expired_while_leader",
        "post-expiry-released" => "lease_post_expiry_released",
        "post-expiry-handler" => "lease_post_expiry_handler",
        "post-expiry-unavailable" => "lease_post_expiry_unavailable",
        "post-expiry-read-served-violation" => "lease_post_expiry_read_served",
        "post-expiry-renewed-violation" => "lease_post_expiry_renewed",
        "post-expiry-unexpected-error" => "lease_post_expiry_unexpected_error",
        "post-expiry-duplicate-terminal" => "lease_duplicate_terminal",
        "coverage-lost" => "lease_coverage_lost",
        _ => return,
    };
    add_raw(values, name, 1);
}

fn finalize_lease_scan(
    values: &mut BTreeMap<&'static str, u64>,
    events: &[ArtifactLeaseMarker],
    parse_errors: u64,
) -> LeaseArtifactStatus {
    let derived = match rederive_lease_status(events) {
        Ok(LeaseArtifactStatus::Complete) => {
            add_raw(values, "lease_sequence_complete", 1);
            LeaseArtifactStatus::Complete
        }
        Ok(LeaseArtifactStatus::Missing) => LeaseArtifactStatus::Missing,
        Ok(status) => {
            add_raw(values, "lease_sequence_invalid", 1);
            status
        }
        Err(()) => {
            add_raw(values, "lease_sequence_invalid", 1);
            LeaseArtifactStatus::HarnessError
        }
    };
    if parse_errors > 0 {
        values.insert("lease_sequence_complete", 0);
        add_raw(values, "lease_sequence_invalid", parse_errors);
        if matches!(
            derived,
            LeaseArtifactStatus::Violation | LeaseArtifactStatus::ViolationWithHarnessError
        ) {
            LeaseArtifactStatus::ViolationWithHarnessError
        } else {
            LeaseArtifactStatus::HarnessError
        }
    } else {
        derived
    }
}

fn rederive_lease_status(events: &[ArtifactLeaseMarker]) -> Result<LeaseArtifactStatus, ()> {
    if events.is_empty() {
        return Ok(LeaseArtifactStatus::Missing);
    }
    let initial = &events[0];
    if initial.phase != "fast-path-read-ok"
        || initial.source_node != initial.node
        || initial.sequence != 1
    {
        return Err(());
    }
    let node = initial.node.as_str();
    let term = initial.term;
    let fast = initial.identity();
    let mut buffered = None;
    let mut expired = false;
    let mut released = false;
    let mut handled = false;
    let mut terminal = None;
    let mut duplicate_terminal = false;
    for (offset, event) in events.iter().enumerate().skip(1) {
        if event.sequence != (offset + 1) as u64
            || event.source_node != node
            || event.node != node
            || event.term != term
        {
            return Err(());
        }
        match event.phase.as_str() {
            "lease-expired"
                if !expired && buffered.is_none() && !released && event.identity() == fast =>
            {
                expired = true;
            }
            "read-buffered" if expired && buffered.is_none() && !released => {
                if event.identity() == fast {
                    return Err(());
                }
                buffered = Some(event.identity());
            }
            "post-expiry-released"
                if expired && buffered == Some(event.identity()) && !released =>
            {
                released = true;
            }
            "post-expiry-handler" if released && buffered == Some(event.identity()) && !handled => {
                handled = true;
            }
            "post-expiry-unavailable"
                if handled && buffered == Some(event.identity()) && terminal.is_none() =>
            {
                terminal = Some(LeaseArtifactStatus::Complete);
            }
            "post-expiry-read-served-violation"
                if handled && buffered == Some(event.identity()) && terminal.is_none() =>
            {
                terminal = Some(LeaseArtifactStatus::Violation);
            }
            "post-expiry-renewed-violation"
                if expired
                    && event.identity() == buffered.unwrap_or(fast)
                    && terminal.is_none() =>
            {
                terminal = Some(LeaseArtifactStatus::Violation);
            }
            "post-expiry-unexpected-error"
                if handled
                    && buffered == Some(event.identity())
                    && event.code.is_some()
                    && terminal.is_none() =>
            {
                terminal = Some(LeaseArtifactStatus::HarnessError);
            }
            "post-expiry-duplicate-terminal"
                if released && buffered == Some(event.identity()) && !duplicate_terminal =>
            {
                duplicate_terminal = true;
            }
            "coverage-lost" if event.reason.is_some() && offset + 1 == events.len() => {
                terminal = Some(LeaseArtifactStatus::Incomplete);
            }
            _ => return Err(()),
        }
    }
    finalize_rederived_lease_status(terminal, duplicate_terminal)
}

fn finalize_rederived_lease_status(
    terminal: Option<LeaseArtifactStatus>,
    duplicate_terminal: bool,
) -> Result<LeaseArtifactStatus, ()> {
    Ok(match (terminal, duplicate_terminal) {
        (Some(LeaseArtifactStatus::Violation), true) => {
            LeaseArtifactStatus::ViolationWithHarnessError
        }
        (Some(LeaseArtifactStatus::Violation), false) => LeaseArtifactStatus::Violation,
        (Some(LeaseArtifactStatus::Complete), false) => LeaseArtifactStatus::Complete,
        (Some(LeaseArtifactStatus::Incomplete) | None, false) => LeaseArtifactStatus::Incomplete,
        (Some(LeaseArtifactStatus::HarnessError), _) | (_, true) => {
            LeaseArtifactStatus::HarnessError
        }
        (
            Some(LeaseArtifactStatus::Missing | LeaseArtifactStatus::ViolationWithHarnessError),
            _,
        ) => return Err(()),
    })
}

fn add_raw(values: &mut BTreeMap<&'static str, u64>, name: &'static str, amount: u64) {
    *values.entry(name).or_default() += amount;
}

pub(super) fn trial_floors_met(
    scenario: &str,
    summary: &MaelstromSummary,
    markers: &BTreeMap<&str, u64>,
    durable: bool,
) -> bool {
    let operations = summary.read_ok > 0 && summary.write_ok > 0 && summary.cas_ok > 0;
    let covered = match scenario {
        "base" => true,
        "membership" => {
            markers["membership_enter"] > 0
                && markers["membership_leave"] > 0
                && markers["membership_complete"] > 0
        }
        "restart" => markers["restarts"] >= 3 && markers["post_restart_progress"] > 0,
        "app-crash" => markers["crashpoints"] > 0 && markers["post_crash_progress"] > 0,
        "snapshot" => {
            markers["restarts"] > 0
                && markers["snapshots_compacted"] > 0
                && markers["snapshots_applied"] > 0
                && markers["post_restart_snapshots_applied"] > 0
        }
        "lease-isolation" => {
            markers["lease_sequence_complete"] == 1
                && markers["lease_sequence_invalid"] == 0
                && markers["lease_fast_path_read_ok"] == 1
                && markers["lease_read_buffered"] == 1
                && markers["lease_expired_while_leader"] == 1
                && markers["lease_post_expiry_released"] == 1
                && markers["lease_post_expiry_handler"] == 1
                && markers["lease_post_expiry_unavailable"] == 1
                && markers["lease_post_expiry_read_served"] == 0
                && markers["lease_post_expiry_renewed"] == 0
                && markers["lease_post_expiry_unexpected_error"] == 0
                && markers["lease_duplicate_terminal"] == 0
                && markers["lease_coverage_lost"] == 0
                && markers["lease_history_probe_matches"] == 1
                && markers["lease_history_probe_mismatches"] == 0
        }
        _ => false,
    };
    operations && covered && (!requires_durable(scenario) || durable)
}

pub(super) fn requires_proxy(scenario: &str) -> bool {
    matches!(
        scenario,
        "restart" | "app-crash" | "snapshot" | "lease-isolation"
    )
}

fn requires_durable(scenario: &str) -> bool {
    matches!(scenario, "restart" | "app-crash" | "snapshot")
}

pub(super) fn empty_observations(trials: u64) -> BTreeMap<String, u64> {
    let mut values = BTreeMap::from([
        ("trials".to_owned(), trials),
        ("valid_trials".to_owned(), 0),
        ("invalid_trials".to_owned(), 0),
        ("operation_count".to_owned(), 0),
        ("ok_count".to_owned(), 0),
        ("read_ok".to_owned(), 0),
        ("write_ok".to_owned(), 0),
        ("cas_ok".to_owned(), 0),
    ]);
    values.extend(MARKERS.into_iter().map(|name| (name.to_owned(), 0)));
    values
}

pub(super) fn add_summary(values: &mut BTreeMap<String, u64>, summary: &MaelstromSummary) {
    add(
        values,
        "valid_trials",
        u64::from(summary.validity == crate::producer::maelstrom_edn::Validity::Valid),
    );
    add(
        values,
        "invalid_trials",
        u64::from(summary.linearizability == crate::producer::maelstrom_edn::Validity::Invalid),
    );
    add(values, "operation_count", summary.operation_count);
    add(values, "ok_count", summary.ok_count);
    add(values, "read_ok", summary.read_ok);
    add(values, "write_ok", summary.write_ok);
    add(values, "cas_ok", summary.cas_ok);
}

pub(super) fn add(values: &mut BTreeMap<String, u64>, name: &str, value: u64) {
    *values.entry(name.to_owned()).or_default() += value;
}

fn bump(values: &mut BTreeMap<&'static str, u64>, name: &'static str, matched: bool) {
    *values.entry(name).or_default() += u64::from(matched);
}

fn read(artifact: &ArtifactRef, root: &Path) -> Result<String, AggregateError> {
    fs::read_to_string(root.join(&artifact.path)).map_err(|read_error| {
        error(format!(
            "read Maelstrom artifact {}: {read_error}",
            artifact.path
        ))
    })
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}

#[cfg(test)]
mod tests {
    use super::{
        finalize_lease_scan, history_completion_count, scan_markers, trial_floors_met,
        ArtifactLeaseMarker, LeaseArtifactStatus, MARKERS,
    };
    use crate::producer::maelstrom_edn::{MaelstromSummary, Validity};

    fn summary() -> MaelstromSummary {
        MaelstromSummary {
            validity: Validity::Valid,
            linearizability: Validity::Valid,
            operation_count: 3,
            ok_count: 3,
            read_ok: 1,
            write_ok: 1,
            cas_ok: 1,
        }
    }

    fn good() -> String {
        [
            "seq=1 node=n1 term=3 phase=fast-path-read-ok client=c0 msg_id=7",
            "seq=2 node=n1 term=3 phase=lease-expired client=c0 msg_id=7",
            "seq=3 node=n1 term=3 phase=read-buffered client=c1 msg_id=11",
            "seq=4 node=n1 term=3 phase=post-expiry-released client=c1 msg_id=11",
            "seq=5 node=n1 term=3 phase=post-expiry-handler client=c1 msg_id=11",
            "seq=6 node=n1 term=3 phase=post-expiry-unavailable client=c1 msg_id=11",
        ]
        .into_iter()
        .map(|fields| format!("rafter-maelstrom lease-isolation {fields}"))
        .collect::<Vec<_>>()
        .join("\n")
    }

    fn scan(
        source: &str,
    ) -> (
        LeaseArtifactStatus,
        std::collections::BTreeMap<&'static str, u64>,
    ) {
        let mut markers = MARKERS.into_iter().map(|name| (name, 0)).collect();
        let mut events = Vec::<ArtifactLeaseMarker>::new();
        let mut errors = 0;
        scan_markers(source, "n1", &mut markers, &mut events, &mut errors);
        let status = finalize_lease_scan(&mut markers, &events, errors);
        (status, markers)
    }

    #[test]
    fn lease_isolation_artifacts_require_the_complete_safe_sequence() {
        let (status, mut markers) = scan(&good());
        markers.insert("lease_history_probe_matches", 1);
        assert_eq!(status, LeaseArtifactStatus::Complete);
        assert!(trial_floors_met(
            "lease-isolation",
            &summary(),
            &markers,
            false
        ));
    }

    #[test]
    fn detector_rejects_cross_node_cross_term_and_uncorrelated_events() {
        let cross_node = good().replacen("seq=4 node=n1", "seq=4 node=n2", 1);
        assert_eq!(scan(&cross_node).0, LeaseArtifactStatus::HarnessError);
        let cross_term = good().replacen("seq=5 node=n1 term=3", "seq=5 node=n1 term=4", 1);
        assert_eq!(scan(&cross_term).0, LeaseArtifactStatus::HarnessError);
        let uncorrelated = good().replacen(
            "phase=post-expiry-unavailable client=c1 msg_id=11",
            "phase=post-expiry-unavailable client=c1 msg_id=99",
            1,
        );
        assert_eq!(scan(&uncorrelated).0, LeaseArtifactStatus::HarnessError);
    }

    #[test]
    fn detector_rejects_out_of_order_missing_and_duplicate_events() {
        let mut lines = good().lines().map(str::to_owned).collect::<Vec<_>>();
        lines.swap(1, 2);
        assert_eq!(scan(&lines.join("\n")).0, LeaseArtifactStatus::HarnessError);
        lines = good().lines().map(str::to_owned).collect::<Vec<_>>();
        lines.swap(3, 4);
        assert_eq!(scan(&lines.join("\n")).0, LeaseArtifactStatus::HarnessError);
        lines = good().lines().map(str::to_owned).collect();
        lines.pop();
        assert_eq!(scan(&lines.join("\n")).0, LeaseArtifactStatus::Incomplete);
        lines = good().lines().map(str::to_owned).collect();
        lines.insert(2, lines[1].clone());
        assert_eq!(scan(&lines.join("\n")).0, LeaseArtifactStatus::HarnessError);
    }

    #[test]
    fn detector_classifies_read_ok_and_renewal_as_rd05_violations() {
        let served = good().replace(
            "phase=post-expiry-unavailable",
            "phase=post-expiry-read-served-violation",
        );
        let (status, markers) = scan(&served);
        assert_eq!(status, LeaseArtifactStatus::Violation);
        assert!(!trial_floors_met(
            "lease-isolation",
            &summary(),
            &markers,
            false
        ));
        let mut lines = good()
            .lines()
            .take(2)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        lines.push("rafter-maelstrom lease-isolation seq=3 node=n1 term=3 phase=post-expiry-renewed-violation client=c0 msg_id=7".to_owned());
        assert_eq!(scan(&lines.join("\n")).0, LeaseArtifactStatus::Violation);
    }

    #[test]
    fn detector_treats_unexpected_error_and_malformed_marker_as_harness_errors() {
        let unexpected = good().replace(
            "phase=post-expiry-unavailable client=c1 msg_id=11",
            "phase=post-expiry-unexpected-error client=c1 msg_id=11 code=20",
        );
        assert_eq!(scan(&unexpected).0, LeaseArtifactStatus::HarnessError);
        let malformed = good().replacen("seq=1", "seq=1 extra=x", 1);
        assert_eq!(scan(&malformed).0, LeaseArtifactStatus::HarnessError);
    }

    #[test]
    fn detector_rejects_second_correlated_terminal_after_either_result() {
        for phase in [
            "post-expiry-unavailable",
            "post-expiry-read-served-violation",
        ] {
            let mut source = good().replace("post-expiry-unavailable", phase);
            source.push_str("\nrafter-maelstrom lease-isolation seq=7 node=n1 term=3 phase=post-expiry-duplicate-terminal client=c1 msg_id=11");
            let (status, markers) = scan(&source);
            let expected = if phase == "post-expiry-read-served-violation" {
                LeaseArtifactStatus::ViolationWithHarnessError
            } else {
                LeaseArtifactStatus::HarnessError
            };
            assert_eq!(status, expected);
            assert_eq!(markers["lease_duplicate_terminal"], 1);

            let mut lines = good()
                .replace("post-expiry-unavailable", phase)
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            lines.insert(4, "rafter-maelstrom lease-isolation seq=5 node=n1 term=3 phase=post-expiry-duplicate-terminal client=c1 msg_id=11".to_owned());
            for (index, line) in lines.iter_mut().enumerate() {
                let (_, rest) = line.split_once(" node=").expect("marker has node");
                *line = format!(
                    "rafter-maelstrom lease-isolation seq={} node={rest}",
                    index + 1
                );
            }
            assert_eq!(scan(&lines.join("\n")).0, expected);
        }
    }

    #[test]
    fn malformed_marker_after_read_served_preserves_violation_and_harness_error() {
        let mut source = good().replace(
            "post-expiry-unavailable",
            "post-expiry-read-served-violation",
        );
        source.push_str("\nrafter-maelstrom lease-isolation malformed");
        let (status, markers) = scan(&source);
        assert_eq!(status, LeaseArtifactStatus::ViolationWithHarnessError);
        assert!(markers["lease_sequence_invalid"] > 0);
    }

    #[test]
    fn independent_history_parser_rejects_missing_and_swapped_probe_identity() {
        let completion = "{:index 2 :type :fail :process 0 :f :read :value nil :error [:temporarily-unavailable \"LeadershipLost [rafter-lease-probe client=c1 msg_id=11 code=11]\"]}";
        let exact =
            format!("{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{completion}");
        assert_eq!(
            history_completion_count(&exact, "c1", 11).expect("history parses"),
            1
        );
        assert_eq!(
            history_completion_count("", "c1", 11).expect("empty history parses"),
            0
        );
        assert_eq!(
            history_completion_count(&exact.replace("client=c1", "client=c2"), "c1", 11)
                .expect("swapped history parses"),
            0
        );
        assert!(history_completion_count(completion, "c1", 11).is_err());
        assert!(history_completion_count(
            "{:index 1 :type :invoke :process 0 :f :read :value nil}",
            "c1",
            11
        )
        .is_err());
        let swapped = format!(
            "{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{{:index 2 :type :invoke :process 1 :f :write :value 1}}\n{}",
            completion
                .replace(":index 2", ":index 3")
                .replace(":process 0", ":process 1")
        );
        assert!(history_completion_count(&swapped, "c1", 11).is_err());
        let mismatched_value = format!(
            "{{:index 1 :type :invoke :process 0 :f :read :value [0 nil]}}\n{}",
            completion.replace(":value nil", ":value [1 nil]")
        );
        assert!(history_completion_count(&mismatched_value, "c1", 11).is_err());
        let missing_value = format!(
            "{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{}",
            completion.replace(" :value nil", "")
        );
        assert!(history_completion_count(&missing_value, "c1", 11).is_err());
    }
}
