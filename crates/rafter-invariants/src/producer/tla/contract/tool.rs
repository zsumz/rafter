//! Pinned TLC tool path, Java binding, and duration parsing.

use std::{collections::BTreeMap, error::Error, time::Duration};

use crate::evidence::SourceReceipt;

pub(super) const JAR: &str = "tools/cache/tla2tools.jar";

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
