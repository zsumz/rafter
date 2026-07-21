//! Independent schema, semantic, and canonical-byte validation for replay reports.

mod artifacts;
mod expectation;
mod inventory;
mod process;
mod runtime;
mod semantics;
mod value;

use super::model::ReplayReport;

pub(in crate::verification) use expectation::ReplayReportExpectation;

pub(in crate::verification) fn validate_report_bytes(bytes: &[u8]) -> Result<(), String> {
    parse_canonical(bytes).map(|_| ())
}

pub(in crate::verification) fn validate_report_bundle(
    bytes: &[u8],
    expectation: &ReplayReportExpectation,
    files: &std::collections::BTreeMap<String, Vec<u8>>,
) -> Result<(), String> {
    let report = parse_canonical(bytes)?;
    expectation.validate(&report)?;
    artifacts::validate(&report, files)
}

#[cfg(test)]
pub(in crate::verification) fn canonical_report_value(
    value: serde_json::Value,
) -> Result<Vec<u8>, String> {
    let report: ReplayReport = serde_json::from_value(value)
        .map_err(|error| format!("decode verifier replay report fixture: {error}"))?;
    let mut bytes = serde_json::to_vec_pretty(&report)
        .map_err(|error| format!("render verifier replay report fixture: {error}"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn parse_canonical(bytes: &[u8]) -> Result<ReplayReport, String> {
    let report = serde_json::from_slice(bytes)
        .map_err(|error| format!("decode verifier replay report: {error}"))?;
    semantics::validate(&report)?;
    let mut canonical = serde_json::to_vec_pretty(&report)
        .map_err(|error| format!("render verifier replay report: {error}"))?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err("verifier replay report bytes are not canonical".to_owned());
    }
    Ok(report)
}
