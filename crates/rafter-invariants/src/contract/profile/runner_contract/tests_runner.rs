//! Canonical configuration for the Rust test evidence runner.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::validate::decode_configuration;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    build: String,
    compile_timeout: String,
    discovery: String,
    discovery_timeout: String,
    execution: String,
    execution_timeout: String,
    failure_contract: String,
    finalization_reserve: String,
    kill_confirmation_timeout: String,
    layer_timeout: String,
    receipt_finalization_allowance: String,
    scale: String,
    termination_grace: String,
}

pub(super) fn validate(
    profile: &str,
    configuration: &BTreeMap<String, String>,
) -> Result<(), String> {
    let contract: Configuration = decode_configuration(configuration)?;
    if contract.build == "locked-no-default-features"
        && contract.compile_timeout == "10m"
        && contract.discovery == "libtest-executable"
        && contract.discovery_timeout == "2m"
        && contract.execution == "exact-single-threaded"
        && contract.execution_timeout == "5m"
        && contract.failure_contract == "typed-oracle-libtest-v4"
        && contract.finalization_reserve == "3m"
        && contract.kill_confirmation_timeout == "5s"
        && contract.layer_timeout == "40m"
        && contract.receipt_finalization_allowance == "5s"
        && contract.scale == profile
        && contract.termination_grace == "30s"
    {
        Ok(())
    } else {
        Err("tests runner configuration is not canonical".to_owned())
    }
}
