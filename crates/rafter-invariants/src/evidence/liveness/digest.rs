//! Canonical SHA-256 identities for liveness contracts and reports.

use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::{
    contract::profile::{SimulatorExecutionContract, SimulatorLivenessContract},
    evidence::SimulatorLivenessReportBinding,
};

pub(super) fn serialized_digest(value: &impl Serialize) -> String {
    let bytes = serde_json::to_vec(value)
        .unwrap_or_else(|error| format!("liveness-serialization-error:{error}").into_bytes());
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn canonical_value_digest(value: &Value) -> String {
    let canonical = canonical_value(value);
    let bytes = serde_json::to_vec(&canonical)
        .unwrap_or_else(|error| format!("report-serialization-error:{error}").into_bytes());
    format!("{:x}", Sha256::digest(bytes))
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_value).collect()),
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            let mut canonical = Map::new();
            for key in keys {
                canonical.insert(key.clone(), canonical_value(&values[key]));
            }
            Value::Object(canonical)
        }
        value => value.clone(),
    }
}

pub(crate) fn liveness_contract_digest(contract: &SimulatorLivenessContract) -> String {
    serialized_digest(contract)
}

pub(crate) fn execution_contract_digest(contract: &SimulatorExecutionContract) -> String {
    serialized_digest(contract)
}

pub(crate) fn liveness_reports_digest(reports: &[SimulatorLivenessReportBinding]) -> String {
    serialized_digest(&reports)
}
