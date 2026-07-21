//! Source-bound qualification of simulator negative-detector evidence.

use std::{collections::BTreeMap, fs, path::Path};

use crate::{
    contract::{catalog::EvidenceDescriptor, SimulatorIdentity},
    evidence::{ArtifactRef, CheckReceipt, ResultBundle},
    verification::{AggregateError, AuthenticatedArtifacts, DetectorFixtureSourceBinding},
};
pub(crate) trait DetectorLogVerifier: Sync {
    fn verify_harness_error(
        &self,
        bundle: &ResultBundle,
        check: &CheckReceipt,
        source: &str,
        test_name: &str,
        oracle_check_id: &str,
        root: &Path,
    ) -> Result<(), AggregateError>;

    fn verify_passing_invocations(
        &self,
        bundle: &ResultBundle,
        check: &CheckReceipt,
        source: &str,
        test_name: &str,
        oracle_check_id: &str,
        root: &Path,
    ) -> Result<(), AggregateError>;

    fn verify_witness_contract(
        &self,
        bundle: &ResultBundle,
        source: &str,
        check_id: &str,
        detector: &str,
        witnesses: &BTreeMap<String, usize>,
    ) -> Result<(), AggregateError>;

    fn require_exact_pass(
        &self,
        source: &str,
        test_name: &str,
        check_id: &str,
    ) -> Result<(), AggregateError>;
}

pub(crate) struct NegativeDetectorContext<'a> {
    pub(crate) bundle: &'a ResultBundle,
    pub(crate) root: &'a Path,
    pub(crate) source_root: &'a Path,
    pub(crate) authenticated: &'a AuthenticatedArtifacts,
    pub(crate) detector_sources: &'a mut crate::verification::DetectorFixtureAnalysis,
    pub(crate) test_logs: &'a mut BTreeMap<String, String>,
    pub(crate) log_verifier: &'a dyn DetectorLogVerifier,
}

pub(crate) fn verify_negative_detector_evidence_authenticated(
    context: &mut NegativeDetectorContext<'_>,
    check: &CheckReceipt,
    descriptor: &EvidenceDescriptor,
    identity: &SimulatorIdentity,
) -> Result<(), AggregateError> {
    let Some(negative_test) = identity.negative_test.as_ref() else {
        return Ok(());
    };
    let fixture = descriptor.negative_fixture.as_deref().ok_or_else(|| {
        AggregateError::new(format!(
            "simulator check {} has a registered negative test without a fixture",
            check.check_id
        ))
    })?;
    if negative_test.test_name.rsplit("::").next() != Some(fixture) {
        return Err(AggregateError::new(format!(
            "simulator check {} fixture does not match registered test identity {}",
            check.check_id, negative_test.test_name
        )));
    }
    let invocation_contract = verify_negative_fixture_binding_cached(
        context.source_root,
        descriptor,
        fixture,
        &check.check_id,
        context.detector_sources,
    )?;
    let qualified = check
        .observations
        .get("detector_qualified")
        .copied()
        .ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {} omits detector qualification status",
                check.check_id
            ))
        })?;
    if qualified > 1 {
        return Err(AggregateError::new(format!(
            "simulator check {} has invalid detector qualification count {qualified}",
            check.check_id
        )));
    }
    if qualified == 0 && super::event::execution_is_passing(context.bundle, &check.execution_id) {
        return Err(AggregateError::new(format!(
            "passing simulator check {} did not qualify its detector",
            check.check_id
        )));
    }
    let Some(artifact) = detector_test_log(check, qualified)? else {
        return Ok(());
    };
    let source = if let Some(source) = context.test_logs.get(&artifact.path) {
        source.clone()
    } else {
        let source = context.authenticated.text(artifact)?.to_owned();
        context
            .test_logs
            .insert(artifact.path.clone(), source.clone());
        source
    };
    if qualified == 0 {
        return context.log_verifier.verify_harness_error(
            context.bundle,
            check,
            &source,
            &negative_test.test_name,
            &negative_test.check_id(),
            context.root,
        );
    }
    context.log_verifier.verify_passing_invocations(
        context.bundle,
        check,
        &source,
        &negative_test.test_name,
        &negative_test.check_id(),
        context.root,
    )?;
    context.log_verifier.verify_witness_contract(
        context.bundle,
        &source,
        &negative_test.check_id(),
        invocation_contract.registered_identity(),
        invocation_contract.witnesses(),
    )?;
    context
        .log_verifier
        .require_exact_pass(&source, &negative_test.test_name, &check.check_id)
}

fn detector_test_log(
    check: &CheckReceipt,
    qualified: u64,
) -> Result<Option<&ArtifactRef>, AggregateError> {
    let test_logs = check
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "test-log")
        .collect::<Vec<_>>();
    match test_logs.as_slice() {
        [artifact] => Ok(Some(*artifact)),
        [] if qualified == 0
            && check
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == "compile-log") =>
        {
            Ok(None)
        }
        [] => Err(AggregateError::new(format!(
            "detector log missing for {}",
            check.check_id
        ))),
        _ => Err(AggregateError::new(format!(
            "simulator check {} must bind exactly one detector test-log, found {}",
            check.check_id,
            test_logs.len()
        ))),
    }
}

#[cfg(test)]
pub(crate) fn verify_negative_fixture_binding(
    root: &Path,
    descriptor: &EvidenceDescriptor,
    fixture: &str,
    check_id: &str,
) -> Result<crate::verification::DetectorFixtureContract, AggregateError> {
    verify_negative_fixture_binding_cached(
        root,
        descriptor,
        fixture,
        check_id,
        &mut crate::verification::DetectorFixtureAnalysis::default(),
    )
}

fn verify_negative_fixture_binding_cached(
    root: &Path,
    descriptor: &EvidenceDescriptor,
    fixture: &str,
    check_id: &str,
    analysis: &mut crate::verification::DetectorFixtureAnalysis,
) -> Result<crate::verification::DetectorFixtureContract, AggregateError> {
    let fixture_path = descriptor.negative_fixture_path.as_deref().ok_or_else(|| {
        AggregateError::new(format!(
            "simulator check {check_id} has no registered negative fixture path"
        ))
    })?;
    let detector = descriptor
        .negative_fixture_detector
        .as_deref()
        .ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {check_id} has no registered detector identity"
            ))
        })?;
    let test_identity = descriptor
        .simulator
        .as_ref()
        .and_then(|identity| identity.negative_test.as_ref())
        .ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {check_id} has no registered detector test identity"
            ))
        })?;
    let canonical_root = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize source root: {error}")))?;
    let canonical_fixture = fs::canonicalize(root.join(fixture_path)).map_err(|error| {
        AggregateError::new(format!(
            "read simulator fixture source {fixture_path}: {error}"
        ))
    })?;
    if !canonical_fixture.starts_with(&canonical_root) {
        return Err(AggregateError::new(format!(
            "simulator fixture path escapes the source root: {fixture_path}"
        )));
    }
    let detector_path = descriptor.negative_detector_path();
    let canonical_detector = fs::canonicalize(root.join(detector_path)).map_err(|error| {
        AggregateError::new(format!(
            "read simulator detector source {detector_path}: {error}"
        ))
    })?;
    if !canonical_detector.starts_with(&canonical_root) {
        return Err(AggregateError::new(format!(
            "simulator detector path escapes the source root: {detector_path}"
        )));
    }
    let fixture_source = fs::read_to_string(&canonical_fixture).map_err(|error| {
        AggregateError::new(format!(
            "read simulator fixture source {fixture_path}: {error}"
        ))
    })?;
    let detector_source = fs::read_to_string(&canonical_detector).map_err(|error| {
        AggregateError::new(format!(
            "read simulator detector source {detector_path}: {error}"
        ))
    })?;
    analysis
        .analyze(&DetectorFixtureSourceBinding {
            fixture_source: &fixture_source,
            detector_source: &detector_source,
            source_root: &canonical_root,
            fixture_path: &canonical_fixture,
            detector_path: &canonical_detector,
            test_identity,
            fixture,
            detector,
        })
        .map_err(|error| {
            AggregateError::new(format!(
                "simulator check {check_id} does not bind fixture {fixture} to detector {detector}: {error}"
            ))
        })
}
