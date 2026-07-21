//! Trial artifact identity, parsing, and lease observation vocabulary.

use std::{collections::BTreeMap, fs, path::Path};

use crate::{
    evidence::format::maelstrom::{parse as parse_edn_summary, MaelstromSummary},
    verification::{AggregateError, AuthenticatedArtifacts},
    ArtifactRef, CheckReceipt,
};

use super::{error, read};

pub(crate) fn verify_matches_file(
    artifact: &ArtifactRef,
    source: impl AsRef<Path>,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    let captured = authenticated.bytes(artifact)?;
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

pub(crate) fn scenario_script(scenario: &str) -> Result<&'static str, AggregateError> {
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

pub(crate) fn group_trials(
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

pub(crate) fn unique<'a>(
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

pub(crate) fn parse_results(
    artifact: &ArtifactRef,
    authenticated: &AuthenticatedArtifacts,
) -> Result<MaelstromSummary, AggregateError> {
    let source = read(artifact, authenticated)?;
    parse_edn_summary(source)
        .map_err(|parse_error| error(format!("parse Maelstrom results: {parse_error}")))
}

pub(crate) fn parse_process(
    artifact: &ArtifactRef,
    authenticated: &AuthenticatedArtifacts,
) -> Result<crate::evidence::format::process::ProcessLog, AggregateError> {
    crate::evidence::format::process::parse_maelstrom_v3(read(artifact, authenticated)?)
        .map_err(|parse_error| error(format!("parse Maelstrom process log: {parse_error}")))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum LeaseArtifactStatus {
    #[default]
    Missing,
    Complete,
    Incomplete,
    Violation,
    ViolationWithHarnessError,
    HarnessError,
}

pub(crate) struct MarkerScan {
    pub(crate) values: BTreeMap<&'static str, u64>,
    pub(crate) lease_status: LeaseArtifactStatus,
    pub(crate) lease_probe: Option<LeaseProbe>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeaseProbe {
    pub(crate) client: String,
    pub(crate) message: u64,
}
