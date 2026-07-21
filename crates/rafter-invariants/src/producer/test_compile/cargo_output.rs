//! Qualification of Cargo compiler messages for one requested test target.

use std::path::PathBuf;

use serde::Deserialize;

use super::{executable, protected, target::Target};

#[derive(Debug, Deserialize)]
pub(super) struct CargoCompilerMessage {
    pub(super) reason: String,
    pub(super) package_id: Option<String>,
    pub(super) target: Option<CargoMessageTarget>,
    pub(super) fresh: Option<bool>,
    pub(super) executable: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub(super) struct CargoMessageTarget {
    pub(super) kind: Vec<String>,
    pub(super) name: String,
    pub(super) src_path: PathBuf,
}

pub(super) fn executable_from_messages(bytes: &[u8], target: &Target) -> Result<PathBuf, String> {
    let workspace = executable::producer_workspace_root()?;
    protected::verify_protected_compiler_artifacts(bytes, &workspace)?;
    let mut executables = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<CargoCompilerMessage>(line) else {
            continue;
        };
        if message.reason != "compiler-artifact" {
            continue;
        }
        let Some(message_target) = message.target else {
            continue;
        };
        if message_target.name != target.name || message_target.kind != [target.kind.as_str()] {
            continue;
        }
        if message.fresh == Some(true) {
            return Err(format!(
                "fresh cached executable is forbidden for {}",
                target.key()
            ));
        }
        let package_id = message.package_id.ok_or_else(|| {
            format!(
                "compiler-artifact omitted Cargo package_id for {}",
                target.key()
            )
        })?;
        executable::verify_package_identity(&package_id, &message_target.src_path, target)?;
        let executable = message
            .executable
            .ok_or_else(|| format!("compiler-artifact omitted executable for {}", target.key()))?;
        executables.push(executable::canonical_test_executable(&executable, target)?);
    }
    if executables.len() == 1 {
        Ok(executables.remove(0))
    } else {
        Err(format!(
            "expected one executable for {}, found {}",
            target.key(),
            executables.len()
        ))
    }
}
