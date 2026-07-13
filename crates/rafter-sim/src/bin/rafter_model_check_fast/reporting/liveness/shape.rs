use std::collections::BTreeSet;

pub(super) fn validate_required_evidence(
    report: &serde_json::Value,
    preconditions: &serde_json::Map<String, serde_json::Value>,
    evidence_field: &str,
    required_field: &str,
    satisfied_field: &str,
    expected: bool,
) -> Result<(), String> {
    let required = preconditions
        .get(required_field)
        .and_then(serde_json::Value::as_bool);
    let satisfied = preconditions
        .get(satisfied_field)
        .and_then(serde_json::Value::as_bool);
    let present = report
        .get(evidence_field)
        .is_some_and(|value| !value.is_null());
    if required != Some(expected) || satisfied != Some(expected) || present != expected {
        return Err(format!("`{evidence_field}` evidence is inconsistent"));
    }
    let status_field = required_field.replace("_required", "_status");
    let expected_status = if expected {
        "satisfied"
    } else {
        "not-required"
    };
    if preconditions
        .get(&status_field)
        .and_then(serde_json::Value::as_str)
        != Some(expected_status)
    {
        return Err(format!("`{evidence_field}` status is inconsistent"));
    }
    if expected {
        let evidence = report[evidence_field]
            .as_object()
            .ok_or_else(|| format!("`{evidence_field}` evidence is not an object"))?;
        match evidence_field {
            "stable_leader" => {
                require_exact_object_fields(
                    evidence,
                    &["node_id", "stable_rounds", "remained_leader_through_probe"],
                    "stable-leader evidence",
                )?;
                if evidence
                    .get("node_id")
                    .and_then(serde_json::Value::as_u64)
                    .is_none()
                    || evidence
                        .get("stable_rounds")
                        .and_then(serde_json::Value::as_u64)
                        .is_none_or(|rounds| rounds == 0)
                    || evidence
                        .get("remained_leader_through_probe")
                        .and_then(serde_json::Value::as_bool)
                        .is_none()
                {
                    return Err("`stable_leader` evidence is malformed".to_owned());
                }
            }
            "proposal" => {
                require_exact_object_fields(
                    evidence,
                    &["proposal_id", "terminal_outcome"],
                    "proposal evidence",
                )?;
                if evidence
                    .get("proposal_id")
                    .and_then(serde_json::Value::as_u64)
                    .is_none_or(|proposal_id| proposal_id == 0)
                    || evidence
                        .get("terminal_outcome")
                        .and_then(serde_json::Value::as_str)
                        .is_none()
                {
                    return Err("`proposal` evidence is malformed".to_owned());
                }
            }
            _ => {
                return Err(format!(
                    "unknown required evidence field `{evidence_field}`"
                ))
            }
        }
    }
    Ok(())
}

pub(super) fn require_exact_fields(
    value: &serde_json::Value,
    expected: &[&str],
    context: &str,
) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{context} is not an object"))?;
    require_exact_object_fields(object, expected, context)
}

pub(super) fn require_exact_object_fields(
    object: &serde_json::Map<String, serde_json::Value>,
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

pub(super) fn require_exact_strings(
    value: &serde_json::Value,
    field: &str,
    expected: &[&str],
) -> Result<(), String> {
    let observed = value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("liveness report field `{field}` is missing or not an array"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| format!("liveness report field `{field}` contains a non-string"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if observed == expected {
        Ok(())
    } else {
        Err(format!("liveness report field `{field}` is inconsistent"))
    }
}

pub(super) fn required_u64_array(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Vec<u64>, String> {
    object
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("liveness precondition `{field}` is missing or not an array"))?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| format!("liveness precondition `{field}` contains a non-integer"))
        })
        .collect()
}

pub(super) fn required_object_u64(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<u64, String> {
    object
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("liveness precondition `{field}` is missing or not an integer"))
}

pub(super) fn required_str<'a>(
    report: &'a serde_json::Value,
    field: &str,
) -> Result<&'a str, String> {
    report
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("liveness report field `{field}` is missing or not a string"))
}

pub(super) fn required_u64(report: &serde_json::Value, field: &str) -> Result<u64, String> {
    report
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("liveness report field `{field}` is missing or not an integer"))
}

pub(super) fn require_exact(
    report: &serde_json::Value,
    field: &str,
    expected: &str,
) -> Result<(), String> {
    let actual = required_str(report, field)?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "liveness report field `{field}` expected `{expected}`, found `{actual}`"
        ))
    }
}
