//! Fresh simulator release compilation and executable admission.

use std::{
    error::Error,
    path::{Path, PathBuf},
    time::Instant,
};

use serde_json::Value;

use crate::{
    execution::filesystem::{self as producer_fs, HeldDirectory, OperationDeadline, TREE_LIMITS},
    producer::{artifact, process},
};

use super::{runner::completed_successfully, types::SimulatorBuild};

pub(super) fn build(
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<SimulatorBuild, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let target_dir = Path::new("target/rafter-invariants/simulator-build")
        .join(source_prefix)
        .join(profile);
    let (execution_deadline, _) = process::active_layer_deadlines(profile, "simulator")?;
    let target_guard = reset_simulator_build_scratch(&target_dir, execution_deadline)?;
    target_guard.verify_path_binding()?;
    let mut environment = process::base_environment();
    environment.insert(
        "CARGO_TARGET_DIR".to_owned(),
        target_guard.external_path().to_string_lossy().into_owned(),
    );
    let arguments = [
        "build".into(),
        "--release".into(),
        "--locked".into(),
        "-p".into(),
        "rafter-sim".into(),
        "--bin".into(),
        "rafter-model-check-fast".into(),
        "--message-format=json-render-diagnostics".into(),
    ];
    target_guard.verify_path_binding()?;
    let output = process::timed_for(
        process::ProcessKind::Compile,
        "cargo",
        &arguments,
        &environment,
        Path::new("."),
    )?;
    let log = artifact::write(
        output_dir,
        Path::new(&format!("{profile}-simulator/{source_prefix}/compile.log")),
        "compile-log",
        &process::combined_log("simulator compile", &output)?,
    )?;
    if !completed_successfully(&output) {
        return Err("simulator release build failed".into());
    }
    let binary = executable_from_messages(&output.stdout)?;
    let binary_handle = producer_fs::hold_file(&binary)?;
    binary_handle.verify_path_binding()?;
    Ok(SimulatorBuild {
        binary,
        binary_handle,
        target_dir: target_guard,
        artifacts: vec![log],
        peak_rss_kib: output.peak_rss_kib,
        duration_ms: process::duration_ms(output.duration),
    })
}

pub(in crate::producer) fn reset_simulator_build_scratch(
    path: &Path,
    deadline: Instant,
) -> Result<HeldDirectory, Box<dyn Error>> {
    HeldDirectory::replace_tree(
        path,
        TREE_LIMITS,
        OperationDeadline::at(deadline, "simulator build scratch cleanup"),
    )
}

fn executable_from_messages(bytes: &[u8]) -> Result<PathBuf, Box<dyn Error>> {
    let mut executables = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if message["reason"] == "compiler-artifact"
            && message["target"]["name"] == "rafter-model-check-fast"
            && message["target"]["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind == "bin"))
        {
            if message["fresh"] == true {
                return Err("fresh cached simulator binary is forbidden".into());
            }
            if let Some(executable) = message["executable"].as_str() {
                executables.push(PathBuf::from(executable));
            }
        }
    }
    if executables.len() != 1 {
        return Err(format!(
            "expected one simulator executable, found {}",
            executables.len()
        )
        .into());
    }
    let executable = executables.remove(0);
    if !executable.is_absolute() {
        return Err("Cargo emitted a non-absolute simulator executable".into());
    }
    Ok(executable)
}
