//! Typed access to authenticated TLA+ proof artifacts.

use crate::{
    evidence::{ArtifactRef, CheckReceipt},
    verification::{AggregateError, AuthenticatedArtifacts},
};

pub(super) fn read_kind(
    check: &CheckReceipt,
    kind: &str,
    authenticated: &AuthenticatedArtifacts,
) -> Result<String, AggregateError> {
    let artifact = unique_artifact(check, kind)?;
    authenticated.text(artifact).map(str::to_owned)
}

pub(super) fn read_json_kind<T: for<'de> serde::Deserialize<'de>>(
    check: &CheckReceipt,
    kind: &str,
    authenticated: &AuthenticatedArtifacts,
) -> Result<T, AggregateError> {
    let source = read_kind(check, kind, authenticated)?;
    serde_json::from_str(&source)
        .map_err(|error| AggregateError::new(format!("parse TLA artifact {kind}: {error}")))
}

pub(super) fn has_kind(check: &CheckReceipt, kind: &str) -> Result<bool, AggregateError> {
    let count = check
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == kind)
        .count();
    match count {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(AggregateError::new(format!(
            "TLA artifact {kind} is ambiguous"
        ))),
    }
}

pub(super) fn unique_artifact<'a>(
    check: &'a CheckReceipt,
    kind: &str,
) -> Result<&'a ArtifactRef, AggregateError> {
    let matching = check
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == kind)
        .collect::<Vec<_>>();
    match matching.as_slice() {
        [artifact] => Ok(artifact),
        [] => Err(AggregateError::new(format!(
            "TLA artifact {kind} is missing"
        ))),
        _ => Err(AggregateError::new(format!(
            "TLA artifact {kind} is ambiguous"
        ))),
    }
}
