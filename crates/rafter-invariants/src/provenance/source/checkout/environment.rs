//! Portable source-environment identity distinct from command execution identity.

use std::{collections::BTreeMap, error::Error};

const SOURCE_ENVIRONMENT_NAMES: &[&str] = &["DEVELOPER_DIR", "SDKROOT", "SYSTEMROOT"];

pub(crate) fn source_environment_sha256() -> Result<String, Box<dyn Error>> {
    source_environment_sha256_from(&current_environment())
}

pub(crate) fn source_environment_sha256_from(
    environment: &BTreeMap<String, String>,
) -> Result<String, Box<dyn Error>> {
    Ok(crate::provenance::invocation::digest_environment(
        &source_environment(environment),
    )?)
}

#[cfg(test)]
pub(crate) fn source_environment_matches_digest(
    environment: &BTreeMap<String, String>,
    expected: &str,
) -> bool {
    source_environment_sha256_from(environment).is_ok_and(|actual| actual == expected)
}

fn current_environment() -> BTreeMap<String, String> {
    SOURCE_ENVIRONMENT_NAMES
        .iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| ((*name).to_owned(), value))
        })
        .collect()
}

fn source_environment(environment: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    SOURCE_ENVIRONMENT_NAMES
        .iter()
        .filter_map(|name| {
            environment
                .get(*name)
                .map(|value| ((*name).to_owned(), value.clone()))
        })
        .collect()
}

#[cfg(test)]
#[path = "environment_tests.rs"]
mod tests;
