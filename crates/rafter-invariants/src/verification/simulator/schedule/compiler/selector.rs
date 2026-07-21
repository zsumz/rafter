//! Independent selection of one exact executable from Cargo JSON messages.

use std::path::PathBuf;

use serde_json::Value;

use crate::verification::AggregateError;

pub(super) fn compiler_artifact_executable(
    bytes: &[u8],
    target_name: &str,
    target_kind: &str,
    target_label: &str,
) -> Result<PathBuf, AggregateError> {
    let mut executables = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if message["reason"] == "compiler-artifact"
            && message["target"]["name"] == target_name
            && message["target"]["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind.as_str() == Some(target_kind)))
        {
            if message["fresh"] == true {
                return Err(AggregateError::new(format!(
                    "fresh cached executable is forbidden for {target_label}"
                )));
            }
            if let Some(executable) = message["executable"].as_str() {
                executables.push(PathBuf::from(executable));
            }
        }
    }
    let [executable] = executables.as_slice() else {
        return Err(AggregateError::new(format!(
            "compile log does not preserve exactly one emitted executable for {target_label}; found {}",
            executables.len()
        )));
    };
    if !executable.is_absolute() {
        return Err(AggregateError::new(format!(
            "Cargo emitted a non-absolute executable for {target_label}"
        )));
    }
    Ok(executable.clone())
}
