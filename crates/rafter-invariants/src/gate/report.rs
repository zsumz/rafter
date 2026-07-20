//! Atomic rendering, publication, and byte-for-byte readback of verdict reports.

use std::{error::Error, fs, path::Path, path::PathBuf};

use crate::{
    contract::{catalog::Catalog, profile::ProfileManifest},
    verdict::{
        report::{render_junit, render_markdown},
        VerdictReport,
    },
};

pub(super) fn write(
    report: &VerdictReport,
    catalog: &Catalog,
    manifest: &ProfileManifest,
    output_dir: &Path,
) -> Result<(), Box<dyn Error>> {
    crate::verdict::validate_verdict_report(report, catalog, manifest)?;
    fs::create_dir_all(output_dir)?;
    let outputs = [
        (
            output_dir.join(format!("{}.json", report.profile)),
            format!("{}\n", serde_json::to_string_pretty(report)?).into_bytes(),
        ),
        (
            output_dir.join(format!("{}.xml", report.profile)),
            render_junit(report).into_bytes(),
        ),
        (
            output_dir.join(format!("{}.md", report.profile)),
            render_markdown(report).into_bytes(),
        ),
    ];
    for (path, contents) in &outputs {
        atomic_write(path.clone(), contents)?;
    }
    for (path, contents) in &outputs {
        verify_written(path, contents)?;
    }
    Ok(())
}

fn verify_written(path: &Path, expected: &[u8]) -> Result<(), Box<dyn Error>> {
    let actual = fs::read(path)?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "written report {} does not match the rendered output",
            path.display()
        )
        .into())
    }
}

fn atomic_write(path: PathBuf, contents: &[u8]) -> Result<(), Box<dyn Error>> {
    let temporary = path.with_extension(format!(
        "{}.tmp-{}",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("report"),
        std::process::id()
    ));
    fs::write(&temporary, contents)?;
    fs::rename(temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests;
