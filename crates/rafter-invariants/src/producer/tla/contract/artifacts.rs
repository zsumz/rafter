//! Source-bound capture of the exact TLA+ tool and specification inputs.

use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use crate::{
    contract::profile::ProofObligationContract,
    evidence::{format::tla::obligation_config_kind, ArtifactRef},
};

use super::{
    super::artifact,
    spec::{DETECTOR_CONFIG, DETECTOR_SPEC, SPEC, TRACE_CONFIG, TRACE_SPEC},
    tool::{required_configuration, JAR},
};

pub(in crate::producer::tla) fn source_artifacts(
    configuration: &BTreeMap<String, String>,
    obligations: &[ProofObligationContract],
    output_dir: &Path,
    profile: &str,
    source_ref: &str,
) -> Result<Vec<ArtifactRef>, Box<dyn Error>> {
    let namespace = input_namespace(profile, source_ref);
    let jar = artifact::capture(output_dir, &namespace, Path::new(JAR), "tla-tool")?;
    if jar.sha256 != required_configuration(configuration, "tool_sha256")? {
        return Err("pinned TLC jar digest does not match the profile contract".into());
    }
    if fs::read_to_string("tools/tla/ASSET_ID")?.trim()
        != required_configuration(configuration, "tool_asset_id")?
    {
        return Err("TLC asset ID does not match the profile contract".into());
    }
    // Every obligation configuration is captured beside the primary inputs so
    // the verifier can bind the exact bytes TLC read. They are deliberately
    // absent from the checkpoint contract's input set: obligations never write
    // or recover checkpoint state, so their bytes must not be able to
    // invalidate the primary configuration's accumulated lineage.
    let mut artifacts = vec![
        jar,
        artifact::capture(output_dir, &namespace, Path::new(SPEC), "tla-spec")?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new(TRACE_SPEC),
            "tla-trace-spec",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new(DETECTOR_SPEC),
            "tla-detector-spec",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new("scripts/tla-model-check"),
            "tla-runner",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new("tools/tla/ASSET_ID"),
            "tla-tool-asset-id",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new("tools/tla/SHA256SUMS"),
            "tla-tool-checksums",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            &Path::new("specs/tla/raft").join(required_configuration(configuration, "config")?),
            "tla-config",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new(TRACE_CONFIG),
            "tla-trace-config",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new(DETECTOR_CONFIG),
            "tla-detector-config",
        )?,
    ];
    for obligation in obligations {
        artifacts.push(artifact::capture(
            output_dir,
            &namespace,
            &Path::new("specs/tla/raft").join(&obligation.config),
            &obligation_config_kind(&obligation.id),
        )?);
    }
    Ok(artifacts)
}

fn input_namespace(profile: &str, source_ref: &str) -> PathBuf {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    PathBuf::from(format!("{profile}-tla"))
        .join(source_prefix)
        .join("inputs")
}

#[cfg(test)]
#[path = "artifacts_tests.rs"]
mod tests;
