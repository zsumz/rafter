use serde_json::Value;

const RESULT_SCHEMA: &str = include_str!("../../../verification/invariant-result-schema.json");
const VERDICT_SCHEMA: &str = include_str!("../../../verification/invariant-verdict-schema.json");

pub(crate) fn validate_result_bundle(bundle: &crate::ResultBundle) -> Result<(), String> {
    let value = serde_json::to_value(bundle)
        .map_err(|error| format!("serialize invariant result bundle: {error}"))?;
    validate(&value, RESULT_SCHEMA, "invariant result bundle")
}

pub(crate) fn validate_result_value(value: &Value) -> Result<(), String> {
    validate(value, RESULT_SCHEMA, "invariant result bundle")
}

pub(crate) fn validate_verdict_report(report: &crate::VerdictReport) -> Result<(), String> {
    let value = serde_json::to_value(report)
        .map_err(|error| format!("serialize invariant verdict report: {error}"))?;
    validate(&value, VERDICT_SCHEMA, "invariant verdict report")
}

fn validate(instance: &Value, source: &str, label: &str) -> Result<(), String> {
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

#[cfg(test)]
mod tests {
    use super::{validate_result_bundle, validate_result_value, validate_verdict_report};

    #[test]
    fn rust_receipts_and_reports_conform_to_distinct_checked_in_schemas() {
        let (catalog, manifest) = crate::tests::loaded();
        let bundles = crate::tests::passing_bundles(&catalog, &manifest);
        for bundle in &bundles {
            validate_result_bundle(bundle).expect("synthetic bundle conforms");
        }
        let report = crate::aggregate(&catalog, &manifest, "pr", "abc", &bundles)
            .expect("synthetic report aggregates");
        validate_verdict_report(&report).expect("aggregate report conforms");
        assert_ne!(
            crate::types::RESULT_SCHEMA_VERSION,
            crate::types::VERDICT_SCHEMA_VERSION
        );
    }

    #[test]
    fn schema_validation_rejects_version_and_shape_tampering() {
        let (catalog, manifest) = crate::tests::loaded();
        let bundle = crate::tests::passing_bundles(&catalog, &manifest)
            .into_iter()
            .next()
            .expect("bundle");
        let mut value = serde_json::to_value(bundle).expect("bundle serializes");
        value["schema_version"] = serde_json::json!(u64::MAX);
        assert!(validate_result_value(&value).is_err());
        value["schema_version"] = serde_json::json!(crate::types::RESULT_SCHEMA_VERSION);
        value["execution"]["unreviewed"] = serde_json::json!(true);
        assert!(validate_result_value(&value).is_err());
    }
}
