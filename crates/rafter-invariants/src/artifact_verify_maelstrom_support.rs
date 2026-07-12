use std::{collections::BTreeMap, fs, path::Path};

use crate::{
    aggregate::AggregateError, producer::maelstrom_edn::MaelstromSummary, ArtifactRef, CheckReceipt,
};

#[rustfmt::skip]
const MARKERS: [&str; 10] = ["membership_enter", "membership_leave", "membership_complete", "restarts", "post_restart_progress", "crashpoints", "post_crash_progress", "snapshots_compacted", "snapshots_applied", "post_restart_snapshots_applied"];
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

pub(super) fn scan_node_logs(
    artifacts: &[&ArtifactRef],
    root: &Path,
) -> Result<BTreeMap<&'static str, u64>, AggregateError> {
    let mut values = MARKERS.into_iter().map(|name| (name, 0)).collect();
    let logs = artifacts
        .iter()
        .filter(|artifact| artifact.kind == "maelstrom-node-log")
        .collect::<Vec<_>>();
    if logs.is_empty() {
        return Err(error("Maelstrom trial has no captured node logs"));
    }
    for artifact in logs {
        scan_markers(&read(artifact, root)?, &mut values);
    }
    Ok(values)
}

fn scan_markers(source: &str, values: &mut BTreeMap<&'static str, u64>) {
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
    }
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
        _ => false,
    };
    operations && covered && (!requires_durable(scenario) || durable)
}

pub(super) fn requires_proxy(scenario: &str) -> bool {
    matches!(scenario, "restart" | "app-crash" | "snapshot")
}

fn requires_durable(scenario: &str) -> bool {
    matches!(scenario, "restart" | "app-crash" | "snapshot")
}

pub(super) fn empty_observations(trials: u64) -> BTreeMap<String, u64> {
    let mut values = BTreeMap::from([
        ("trials".to_owned(), trials),
        ("valid_trials".to_owned(), 0),
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
