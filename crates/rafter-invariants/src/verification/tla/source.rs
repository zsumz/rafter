//! Source, tool-pin, and profile-contract binding for TLA+ evidence.

use std::{fs, path::Path};

use crate::{
    evidence::{CheckReceipt, ResultBundle},
    verification::{AggregateError, AuthenticatedArtifacts},
};

use super::artifact::{read_kind, unique_artifact};

pub(super) fn verify_source_binding(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    root: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    let config = configuration(bundle, "config")?;
    // Every obligation binds the exact configuration bytes TLC read back to
    // the reviewed file in the source checkout, on the same footing as the
    // primary configuration.
    let obligations = super::obligation::contracted(bundle)?
        .iter()
        .map(|obligation| {
            (
                crate::evidence::format::tla::obligation_config_kind(&obligation.id),
                format!("specs/tla/raft/{}", obligation.config),
            )
        })
        .collect::<Vec<_>>();
    for (kind, source) in [
        ("tla-spec", "specs/tla/raft/Raft.tla".to_owned()),
        (
            "tla-trace-spec",
            "specs/tla/raft/RaftMembershipTraceSample.tla".to_owned(),
        ),
        (
            "tla-detector-spec",
            "specs/tla/raft/RafterInvariantDetectorNegative.tla".to_owned(),
        ),
        (
            "tla-detector-config",
            "specs/tla/raft/RafterInvariantDetectorNegative.cfg".to_owned(),
        ),
        ("tla-runner", "scripts/tla-model-check".to_owned()),
        ("tla-tool-asset-id", "tools/tla/ASSET_ID".to_owned()),
        ("tla-tool-checksums", "tools/tla/SHA256SUMS".to_owned()),
        ("tla-config", format!("specs/tla/raft/{config}")),
        (
            "tla-trace-config",
            "specs/tla/raft/RaftMembershipTraceSample.cfg".to_owned(),
        ),
    ]
    .into_iter()
    .map(|(kind, source)| (kind.to_owned(), source))
    .chain(obligations)
    {
        let artifact = read_kind(check, &kind, authenticated)?;
        let source = fs::read_to_string(root.join(&source)).map_err(|error| {
            AggregateError::new(format!("read TLA source binding {source}: {error}"))
        })?;
        if artifact != source {
            return Err(AggregateError::new(format!(
                "TLA artifact {kind} does not match its bound source"
            )));
        }
    }
    Ok(())
}

pub(super) fn verify_tool_pin(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    let expected_sha = configuration(bundle, "tool_sha256")?;
    let tool = unique_artifact(check, "tla-tool")?;
    if tool.sha256 != expected_sha {
        return Err(AggregateError::new(
            "TLA tool artifact does not match the profile digest".to_owned(),
        ));
    }
    let asset_id = read_kind(check, "tla-tool-asset-id", authenticated)?;
    if asset_id.trim() != configuration(bundle, "tool_asset_id")? {
        return Err(AggregateError::new(
            "TLA tool asset ID does not match the profile contract".to_owned(),
        ));
    }
    let checksums = read_kind(check, "tla-tool-checksums", authenticated)?;
    if !checksum_matches(&checksums, expected_sha) {
        return Err(AggregateError::new(
            "TLA checksum manifest does not contain the exact profile digest".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn checksum_matches(checksums: &str, expected_sha: &str) -> bool {
    let declared = checksums
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let sha = fields.next()?;
            let file = fields.next()?;
            (file == "tla2tools.jar" && fields.next().is_none()).then_some(sha)
        })
        .collect::<Vec<_>>();
    declared.as_slice() == [expected_sha]
}

pub(super) fn configuration<'a>(
    bundle: &'a ResultBundle,
    name: &str,
) -> Result<&'a str, AggregateError> {
    bundle
        .execution
        .plan
        .contract
        .runners
        .get(&bundle.runner)
        .ok_or_else(|| {
            AggregateError::new(format!("execution plan omitted runner {}", bundle.runner))
        })?
        .configuration
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| AggregateError::new(format!("TLA configuration omitted {name}")))
}
