//! Cargo invocation and capture of one compiled invariant target.

use std::{collections::BTreeMap, error::Error, path::Path, path::PathBuf};

use crate::{
    evidence::ArtifactRef,
    execution::filesystem::{self as producer_fs, HeldFile},
};

use super::{cargo_output::executable_from_messages, target::Target};
use crate::producer::{artifact, process};

pub(in crate::producer) struct CompiledTarget {
    pub executable: Option<PathBuf>,
    pub executable_handle: Option<HeldFile>,
    pub binary_artifact: Option<ArtifactRef>,
    pub artifact: ArtifactRef,
    pub error: Option<String>,
    pub peak_rss_kib: u64,
    pub duration_ms: u64,
}

pub(in crate::producer) fn compile(
    target: &Target,
    profile: &str,
    source_ref: &str,
    environment: &BTreeMap<String, String>,
    output_dir: &Path,
) -> Result<CompiledTarget, Box<dyn Error>> {
    let mut arguments = vec![
        "test".into(),
        "--locked".into(),
        "--no-default-features".into(),
        "-p".into(),
        target.package.clone().into(),
    ];
    arguments.extend(target.selector()?);
    arguments.extend([
        "--no-run".into(),
        "--message-format=json-render-diagnostics".into(),
    ]);
    let output = process::timed_for(
        process::ProcessKind::Compile,
        "cargo",
        &arguments,
        environment,
        Path::new("."),
    )?;
    let artifact_id = artifact::stable_id(
        "compile",
        &format!("{profile}\0{source_ref}\0{}", target.key()),
    );
    let log = artifact::write(
        output_dir,
        Path::new(&format!("{profile}-tests/compile/{artifact_id}.log")),
        "compile-log",
        &process::combined_log(&target.key(), &output)?,
    )?;
    let (executable, error) = compile_result(&output, target);
    let executable_handle = executable
        .as_deref()
        .map(producer_fs::hold_file)
        .transpose()?;
    let binary_artifact = executable
        .as_deref()
        .map(|path| {
            artifact::capture(
                output_dir,
                Path::new(&format!("{profile}-tests/inputs")),
                path,
                "test-binary",
            )
        })
        .transpose()?;
    Ok(CompiledTarget {
        executable,
        executable_handle,
        binary_artifact,
        artifact: log,
        error,
        peak_rss_kib: output.peak_rss_kib,
        duration_ms: process::duration_ms(output.duration),
    })
}

fn compile_result(
    output: &process::ProcessOutput,
    target: &Target,
) -> (Option<PathBuf>, Option<String>) {
    if output.timed_out {
        return (
            None,
            Some(format!(
                "cargo test --no-run timed out for {}",
                target.key()
            )),
        );
    }
    if output.status.success() {
        match executable_from_messages(&output.stdout, target) {
            Ok(executable) => (Some(executable), None),
            Err(error) => (None, Some(error)),
        }
    } else {
        (
            None,
            Some(format!("cargo test --no-run failed for {}", target.key())),
        )
    }
}
