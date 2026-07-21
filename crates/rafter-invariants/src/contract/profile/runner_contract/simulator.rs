//! Canonical detector-proof configuration for the simulator runner.

use std::collections::BTreeMap;

pub(super) fn validate(configuration: &BTreeMap<String, String>) -> Result<(), String> {
    let detector_proof = configuration.get("detector_proof").map(String::as_str);
    if detector_proof == Some("inherited-descriptor-pre-body-secret-v3") {
        Ok(())
    } else {
        Err("simulator detector proof contract is not canonical".to_owned())
    }
}
