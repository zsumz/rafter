//! Shared checked-in JSON Schema compilation and validation.

use serde_json::Value;

pub(crate) fn validate(instance: &Value, source: &str, label: &str) -> Result<(), String> {
    let schema: Value = serde_json::from_str(source)
        .map_err(|error| format!("parse checked-in {label} schema: {error}"))?;
    let validator = jsonschema::validator_for(&schema)
        .map_err(|error| format!("compile checked-in {label} schema: {error}"))?;
    let errors = validator
        .iter_errors(instance)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{label} violates its checked-in schema: {}",
            errors.join("; ")
        ))
    }
}
