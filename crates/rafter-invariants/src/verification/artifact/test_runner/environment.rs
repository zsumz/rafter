//! Deterministic libtest execution environments.

use std::{collections::BTreeMap, path::Path};

use crate::{
    evidence::{CheckReceipt, ResultBundle},
    verification::AggregateError,
};

pub(crate) fn verify_exact_environment(
    exact: &crate::evidence::format::process::LabeledProcess,
    expected: &BTreeMap<String, String>,
    expected_digest: &str,
) -> Result<(), AggregateError> {
    if exact.invocation.environment != *expected
        || exact.invocation.environment_sha256 != expected_digest
        || !crate::provenance::invocation::environment_matches_digest(
            &exact.invocation.environment,
            &exact.invocation.environment_sha256,
        )
    {
        return Err(AggregateError::new(
            "test log does not contain the exact execution environment".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn exact_test_environment(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    invocations: &[crate::evidence::format::process::LabeledProcess],
    test_name: &str,
    oracle_check_id: &str,
    root: &Path,
) -> Result<BTreeMap<String, String>, AggregateError> {
    let execution_id = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "test-log")
        .and_then(|artifact| Path::new(&artifact.path).file_stem())
        .and_then(|value| value.to_str())
        .ok_or_else(|| AggregateError::new("test log path has no execution ID".to_owned()))?;
    let execution_profile = super::super::test_execution::profile(bundle);
    let executed_test_name = invocations
        .get(2)
        .and_then(|invocation| invocation.invocation.arguments.first())
        .map_or(test_name, String::as_str);
    let seed = crate::provenance::invocation::deterministic_u64(
        "rafter-tests/v1",
        &format!(
            "{execution_profile}\0{}\0{executed_test_name}",
            bundle.source_ref
        ),
    );
    let mut environment = invocations
        .first()
        .map(|invocation| invocation.invocation.environment.clone())
        .unwrap_or_default();
    environment.extend([
        ("PROPTEST_RNG_SEED".to_owned(), seed.to_string()),
        (
            "PROPTEST_DISABLE_FAILURE_PERSISTENCE".to_owned(),
            "1".to_owned(),
        ),
        (
            "TMPDIR".to_owned(),
            root.join("target/rafter-invariants/tmp")
                .join(execution_id)
                .to_string_lossy()
                .into_owned(),
        ),
        ("RUST_BACKTRACE".to_owned(), "1".to_owned()),
        (
            crate::evidence::format::libtest::ORACLE_TOKEN_ENV.to_owned(),
            crate::evidence::format::libtest::oracle_token(&bundle.source_ref, oracle_check_id),
        ),
    ]);
    if bundle.runner == "simulator" {
        let detector_environment = invocations
            .get(2)
            .map(|invocation| &invocation.invocation.environment)
            .ok_or_else(|| {
                AggregateError::new("detector log omitted its exact invocation".to_owned())
            })?;
        let descriptor = detector_environment
            .get(crate::evidence::detector_proof::PROOF_DESCRIPTOR_ENV)
            .ok_or_else(|| {
                AggregateError::new(
                    "detector execution environment omitted its inherited proof descriptor"
                        .to_owned(),
                )
            })?;
        if !crate::evidence::detector_proof::canonical_descriptor(descriptor) {
            return Err(AggregateError::new(
                "detector proof descriptor is not canonical".to_owned(),
            ));
        }
        environment.insert(
            crate::evidence::detector_proof::PROOF_DESCRIPTOR_ENV.to_owned(),
            descriptor.clone(),
        );
    }
    Ok(environment)
}
