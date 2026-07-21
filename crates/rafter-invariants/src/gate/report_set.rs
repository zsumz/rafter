//! Semantic readback of one published JSON, `JUnit`, and Markdown verdict set.

use std::{error::Error, fs, path::Path};

use crate::{
    contract::{catalog::Catalog, profile::ProfileManifest},
    verdict::{
        report::{render_junit, render_markdown},
        VerdictReport,
    },
};

const MAX_REPORT_BYTES: u64 = 16 * 1024 * 1024;

/// Verify that one profile's three report formats are exact projections of the same verdict.
///
/// # Errors
///
/// Returns an error when a report is missing, nonregular, excessive, malformed, semantically
/// inconsistent with the registry/profile contract, or differs from the canonical rendering.
pub fn verify_report_set(
    report_dir: &Path,
    profile: &str,
    catalog: &Catalog,
    manifest: &ProfileManifest,
) -> Result<(), Box<dyn Error>> {
    if profile.is_empty()
        || !profile
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err("report profile name is noncanonical".into());
    }
    let json_path = report_dir.join(format!("{profile}.json"));
    let json = read_bounded_regular(&json_path)?;
    let report: VerdictReport = serde_json::from_slice(&json)?;
    if report.profile != profile {
        return Err("verdict report profile does not match the requested report set".into());
    }
    crate::verdict::validate_verdict_report(&report, catalog, manifest)?;

    let expected_json = format!("{}\n", serde_json::to_string_pretty(&report)?).into_bytes();
    require_exact(&json_path, &json, &expected_json)?;
    require_exact_path(
        &report_dir.join(format!("{profile}.xml")),
        render_junit(&report).as_bytes(),
    )?;
    require_exact_path(
        &report_dir.join(format!("{profile}.md")),
        render_markdown(&report).as_bytes(),
    )?;
    Ok(())
}

fn require_exact_path(path: &Path, expected: &[u8]) -> Result<(), Box<dyn Error>> {
    let observed = read_bounded_regular(path)?;
    require_exact(path, &observed, expected)
}

fn require_exact(path: &Path, observed: &[u8], expected: &[u8]) -> Result<(), Box<dyn Error>> {
    if observed == expected {
        Ok(())
    } else {
        Err(format!(
            "published report {} is not its canonical verdict projection",
            path.display()
        )
        .into())
    }
}

fn read_bounded_regular(path: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(format!("published report {} is not a regular file", path.display()).into());
    }
    if metadata.len() > MAX_REPORT_BYTES {
        return Err(format!("published report {} exceeds its byte limit", path.display()).into());
    }
    let bytes = fs::read(path)?;
    if u64::try_from(bytes.len())? != metadata.len() {
        return Err(format!("published report {} changed while read", path.display()).into());
    }
    Ok(bytes)
}

#[cfg(test)]
#[path = "report_set/tests.rs"]
mod tests;
