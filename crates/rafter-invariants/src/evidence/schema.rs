//! Shape validation for one producer's result bundle.

use serde_json::Value;

use crate::contract::schema::{validate, RESULT_SCHEMA};

pub(crate) fn validate_result_bundle(bundle: &crate::ResultBundle) -> Result<(), String> {
    let value = serde_json::to_value(bundle)
        .map_err(|error| format!("serialize invariant result bundle: {error}"))?;
    validate_result_value(&value)
}

pub(crate) fn validate_result_value(value: &Value) -> Result<(), String> {
    validate(value, RESULT_SCHEMA, "invariant result bundle")
}
