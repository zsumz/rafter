//! Trial artifact identity, source binding, and neutral-format decoding.

use std::{collections::BTreeMap, fs, path::Path};

use crate::{
    evidence::{format::maelstrom::MaelstromSummary, ArtifactRef, CheckReceipt},
    verification::{AggregateError, AuthenticatedArtifacts},
};

pub(super) fn verify_matches_file(
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
    authenticated: &AuthenticatedArtifacts,
) -> Result<MaelstromSummary, AggregateError> {
    crate::evidence::format::maelstrom::parse(authenticated.text(artifact)?)
        .map_err(|parse_error| error(format!("parse Maelstrom results: {parse_error}")))
}

pub(super) fn parse_process(
    artifact: &ArtifactRef,
    authenticated: &AuthenticatedArtifacts,
) -> Result<crate::evidence::format::process::ProcessLog, AggregateError> {
    crate::evidence::format::process::parse_maelstrom_v3(authenticated.text(artifact)?)
        .map_err(|parse_error| error(format!("parse Maelstrom process log: {parse_error}")))
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

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}
