//! Canonical detector-proof configuration for the simulator runner.

use std::collections::BTreeMap;

pub(super) fn validate(configuration: &BTreeMap<String, String>) -> Result<(), String> {
    let detector_proof = configuration.get("detector_proof").map(String::as_str);
    let detector_source_preflight = configuration
        .get("detector_source_preflight")
        .map(String::as_str);
    if detector_proof == Some("post-invocation-parent-challenge-v1")
        && detector_source_preflight == Some("exact-module-call-graph-v1")
    {
        Ok(())
    } else {
        Err("simulator detector proof or source-preflight contract is not canonical".to_owned())
    }
}
