//! Pinned TLC tool preparation, Java binding, and duration parsing.

use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path, time::Duration};

use crate::evidence::SourceReceipt;

use super::super::process;

pub(super) const JAR: &str = "tools/cache/tla2tools.jar";
const TOOL_FETCH_TIMEOUT: Duration = Duration::from_secs(5 * 60);

pub(in crate::producer::tla) fn fetch_tool() -> Result<(), Box<dyn Error>> {
    fetch_tool_at(Path::new("."))
}

pub(crate) fn fetch_tool_at(repo_root: &Path) -> Result<(), Box<dyn Error>> {
    let runner = repo_root.join("scripts/tla-model-check");
    let program = runner.as_os_str().to_string_lossy();
    fetch_tool_with(
        repo_root,
        &program,
        &[OsString::from("--fetch-tool")],
        TOOL_FETCH_TIMEOUT,
    )
}

pub(super) fn fetch_tool_with(
    repo_root: &Path,
    program: &str,
    arguments: &[OsString],
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let environment = tool_fetch_environment(repo_root);
    let output = process::timed_with_optional_layer_budget(
        process::ProcessKind::TlaExecution,
        program,
        arguments,
        &environment,
        Path::new("."),
        timeout,
    )?;
    if output.timed_out || !output.status.success() {
        return Err(format!(
            "fetch pinned TLC tool failed with {:?} (timed_out={}): stdout: {}; stderr: {}",
            output.status.code(),
            output.timed_out,
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}

pub(super) fn tool_fetch_environment(repo_root: &Path) -> BTreeMap<String, String> {
    let mut environment = process::base_environment();
    environment.insert(
        "RAFTER_TLA_REPO_ROOT".to_owned(),
        repo_root.as_os_str().to_string_lossy().into_owned(),
    );
    environment
}

pub(in crate::producer::tla) fn validate_java(
    source: &SourceReceipt,
    configuration: &BTreeMap<String, String>,
) -> Result<(), Box<dyn Error>> {
    let java = source
        .tools
        .get("java")
        .ok_or("Java tool identity missing")?;
    let required = required_configuration(configuration, "java_major")?.parse::<u32>()?;
    if java_major(&java.version) != Some(required) {
        return Err(format!("Java version does not satisfy required major {required}").into());
    }
    Ok(())
}

pub(crate) fn java_major(version: &str) -> Option<u32> {
    crate::evidence::format::java::major(version)
}

pub(in crate::producer::tla) fn required_configuration<'a>(
    configuration: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, Box<dyn Error>> {
    configuration
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("TLA runner configuration omitted {name}").into())
}

pub(in crate::producer::tla) fn parse_timeout(value: &str) -> Result<Duration, Box<dyn Error>> {
    let minutes = value
        .strip_suffix('m')
        .ok_or("TLA soft_timeout must use whole minutes")?
        .parse::<u64>()?;
    Ok(Duration::from_secs(minutes.saturating_mul(60)))
}
