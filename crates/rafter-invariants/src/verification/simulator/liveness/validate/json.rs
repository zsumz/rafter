//! Strict JSON object and scalar accessors used by evidence validators.

use std::collections::BTreeSet;

use serde_json::{Map, Value};

pub(super) fn exact_string(value: &Value, field: &str, expected: &str) -> Result<(), String> {
    let actual = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("field `{field}` is missing or not a string"))?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "field `{field}` expected `{expected}`, found `{actual}`"
        ))
    }
}

pub(super) fn exact_string_array(
    value: &Value,
    field: &str,
    expected: &[String],
) -> Result<(), String> {
    let observed = value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("field `{field}` is missing or not an array"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("field `{field}` contains a non-string"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if observed == expected {
        Ok(())
    } else {
        Err(format!("field `{field}` does not match the registry"))
    }
}

pub(super) fn require_exact_fields(
    value: &Value,
    expected: &[&str],
    context: &str,
) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{context} is not an object"))?;
    require_exact_object_fields(object, expected, context)
}

pub(super) fn require_exact_object_fields(
    object: &Map<String, Value>,
    expected: &[&str],
    context: &str,
) -> Result<(), String> {
    let observed = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    if observed == expected {
        Ok(())
    } else {
        let missing = expected.difference(&observed).copied().collect::<Vec<_>>();
        let unknown = observed.difference(&expected).copied().collect::<Vec<_>>();
        Err(format!(
            "{context} has missing fields {missing:?} or unknown fields {unknown:?}"
        ))
    }
}

pub(super) fn required_object<'a>(
    value: &'a Value,
    field: &str,
) -> Result<&'a Map<String, Value>, String> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("field `{field}` is missing or not an object"))
}

pub(super) fn required_u64(value: &Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("field `{field}` is missing or not an integer"))
}

pub(super) fn required_map_str<'a>(
    value: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("field `{field}` is missing or not a string"))
}

pub(super) fn required_map_u64(value: &Map<String, Value>, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("field `{field}` is missing or not an integer"))
}

pub(super) fn required_map_bool(value: &Map<String, Value>, field: &str) -> Result<bool, String> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("field `{field}` is missing or not a boolean"))
}

pub(super) fn required_map_u64_array(
    value: &Map<String, Value>,
    field: &str,
) -> Result<Vec<u64>, String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("field `{field}` is missing or not an array"))?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| format!("field `{field}` contains a non-integer"))
        })
        .collect()
}
