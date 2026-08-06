//! Shared checked-in JSON Schema compilation and validation.

use serde_json::Value;

const MAX_SCHEMA_DIAGNOSTIC_BYTES: usize = 4 * 1024;

pub(crate) fn validate(instance: &Value, source: &str, label: &str) -> Result<(), String> {
    let schema: Value = serde_json::from_str(source)
        .map_err(|error| format!("parse checked-in {label} schema: {error}"))?;
    let validator = jsonschema::validator_for(&schema)
        .map_err(|error| format!("compile checked-in {label} schema: {error}"))?;
    let Some(error) = validator.iter_errors(instance).next() else {
        return Ok(());
    };
    let instance_path = error.instance_path.to_string();
    let instance_path = if instance_path.is_empty() {
        "/"
    } else {
        &instance_path
    };
    let mut diagnostic = format!("at {instance_path}: {error}");
    if diagnostic.len() > MAX_SCHEMA_DIAGNOSTIC_BYTES {
        let mut boundary = MAX_SCHEMA_DIAGNOSTIC_BYTES;
        while !diagnostic.is_char_boundary(boundary) {
            boundary -= 1;
        }
        diagnostic.truncate(boundary);
        diagnostic.push_str("...");
    }
    Err(format!(
        "{label} violates its checked-in schema: {diagnostic}"
    ))
}
