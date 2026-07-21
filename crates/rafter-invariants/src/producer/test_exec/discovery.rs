//! Exact test discovery across ordinary and ignored libtest inventories.

use std::{error::Error, path::Path};

use super::artifact_log;
use crate::evidence::format::libtest::listed_tests;
use crate::producer::process;

pub(super) struct DiscoveryOutput {
    pub listed: process::ProcessOutput,
    pub ignored: process::ProcessOutput,
    pub log: Vec<u8>,
}

pub(super) fn discover(
    program: &str,
    profile: &str,
    source_ref: &str,
    execution_id: &str,
    output_dir: &Path,
) -> Result<DiscoveryOutput, Box<dyn Error>> {
    let environment = process::base_environment();
    let listed = process::timed_for(
        process::ProcessKind::TestDiscovery,
        program,
        &["--list".into(), "--format".into(), "terse".into()],
        &environment,
        Path::new("."),
    )?;
    let mut log = process::combined_log("libtest discovery", &listed)?;
    artifact_log::persist(output_dir, profile, source_ref, execution_id, &log)?;

    let ignored = process::timed_for(
        process::ProcessKind::TestDiscovery,
        program,
        &[
            "--ignored".into(),
            "--list".into(),
            "--format".into(),
            "terse".into(),
        ],
        &environment,
        Path::new("."),
    )?;
    log.extend(process::combined_log(
        "libtest ignored discovery",
        &ignored,
    )?);
    artifact_log::persist(output_dir, profile, source_ref, execution_id, &log)?;

    Ok(DiscoveryOutput {
        listed,
        ignored,
        log,
    })
}

pub(super) fn failure(
    listed: &process::ProcessOutput,
    ignored: &process::ProcessOutput,
) -> Option<&'static str> {
    if listed.timed_out || ignored.timed_out {
        Some("libtest discovery process timed out")
    } else if !listed.status.success() || !ignored.status.success() {
        Some("libtest discovery process failed")
    } else {
        None
    }
}

pub(super) fn exact_matches(output: &[u8], test_name: &str) -> usize {
    listed_tests(output)
        .iter()
        .filter(|test| test.as_str() == test_name)
        .count()
}

#[cfg(test)]
mod tests;
