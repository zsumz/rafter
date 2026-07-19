//! Detector proof and harness-error acceptance.

use std::{collections::BTreeMap, path::Path};

use crate::{
    evidence::{detector_proof::TranscriptRecord, CheckReceipt, ResultBundle},
    verification::AggregateError,
};

#[cfg(test)]
pub(in crate::artifact_verify) fn require_detector_witness(
    bundle: &ResultBundle,
    source: &str,
    oracle_check_id: &str,
    registered_identity: &str,
) -> Result<(), AggregateError> {
    valid_witness_identity(registered_identity).ok_or_else(|| {
        AggregateError::new(format!(
            "registered detector identity is malformed: {registered_identity}"
        ))
    })?;
    require_detector_witness_contract(
        bundle,
        source,
        oracle_check_id,
        registered_identity,
        &BTreeMap::from([(format!("expect-err:{registered_identity}"), 1)]),
    )
}

pub(in crate::artifact_verify) fn require_detector_witness_contract(
    bundle: &ResultBundle,
    source: &str,
    oracle_check_id: &str,
    registered_identity: &str,
    expected_witnesses: &BTreeMap<String, usize>,
) -> Result<(), AggregateError> {
    if !expected_witnesses.keys().any(|witness| {
        witness
            .split_once(':')
            .is_some_and(|(_, identity)| identity == registered_identity)
    }) {
        return Err(AggregateError::new(format!(
            "source invocation contract omits registered detector {registered_identity}"
        )));
    }
    let processes = crate::evidence::format::process::parse_combined_processes(source)
        .map_err(|error| AggregateError::new(format!("parse detector invocation: {error}")))?;
    let exact = processes
        .iter()
        .find(|process| process.label == "exact libtest execution")
        .ok_or_else(|| {
            AggregateError::new("detector log omitted its exact invocation".to_owned())
        })?;
    if exact.schema_version != crate::evidence::format::process::DETECTOR_PROCESS_SCHEMA_VERSION {
        return Err(AggregateError::new(
            "detector exact invocation does not use the detector process schema".to_owned(),
        ));
    }
    let token = crate::evidence::format::libtest::oracle_token(&bundle.source_ref, oracle_check_id);
    let challenge = exact.detector_challenge.as_deref().ok_or_else(|| {
        AggregateError::new("detector log omitted its parent-issued challenge".to_owned())
    })?;
    require_detector_witness_contract_in_streams(
        &exact.stdout,
        &exact.stderr,
        &token,
        challenge,
        expected_witnesses,
    )
}

#[cfg(test)]
pub(super) fn require_detector_witness_in_streams(
    stdout: &str,
    stderr: &str,
    token: &str,
    challenge: &str,
    registered_identity: &str,
) -> Result<(), AggregateError> {
    valid_witness_identity(registered_identity).ok_or_else(|| {
        AggregateError::new(format!(
            "registered detector identity is malformed: {registered_identity}"
        ))
    })?;
    require_detector_witness_contract_in_streams(
        stdout,
        stderr,
        token,
        challenge,
        &BTreeMap::from([(format!("expect-err:{registered_identity}"), 1)]),
    )
}

pub(super) fn require_detector_witness_contract_in_streams(
    stdout: &str,
    stderr: &str,
    token: &str,
    challenge: &str,
    expected_witnesses: &BTreeMap<String, usize>,
) -> Result<(), AggregateError> {
    let witnesses = verify_transcript_policy(stdout, stderr, token, challenge)?;
    if &witnesses != expected_witnesses {
        return Err(AggregateError::new(format!(
            "detector log witness contract mismatch: expected {expected_witnesses:?}, observed {witnesses:?}"
        )));
    }
    Ok(())
}

fn verify_transcript_policy(
    stdout: &str,
    stderr: &str,
    expected_token: &str,
    expected_challenge: &str,
) -> Result<BTreeMap<String, usize>, AggregateError> {
    crate::evidence::detector_proof::validate_challenge(expected_challenge)
        .map_err(|error| AggregateError::new(format!("detector proof failed: {error}")))?;
    let records = crate::evidence::detector_proof::decode_transcript(stdout, stderr)
        .map_err(|error| AggregateError::new(format!("detector proof failed: {error}")))?;
    let mut witnesses = BTreeMap::<String, usize>::new();
    let mut proofs = BTreeMap::<String, usize>::new();
    for record in records {
        match record {
            TranscriptRecord::Witness { token, witness } => {
                if token != expected_token {
                    return Err(AggregateError::new(
                        "detector witness is bound to another execution token".to_owned(),
                    ));
                }
                *witnesses.entry(witness).or_default() += 1;
            }
            TranscriptRecord::Proof {
                token,
                witness,
                challenge,
            } => {
                if token != expected_token {
                    return Err(AggregateError::new(
                        "detector proof is bound to another execution token".to_owned(),
                    ));
                }
                if challenge != expected_challenge {
                    return Err(AggregateError::new(
                        "detector proof used the wrong post-invocation challenge".to_owned(),
                    ));
                }
                *proofs.entry(witness).or_default() += 1;
            }
        }
    }
    if witnesses.is_empty() {
        return Err(AggregateError::new(
            "detector transcript contains no runtime witnesses".to_owned(),
        ));
    }
    if witnesses != proofs {
        return Err(AggregateError::new(format!(
            "detector witness and proof inventories differ: witnesses={witnesses:?}, proofs={proofs:?}"
        )));
    }
    Ok(witnesses)
}

#[cfg(test)]
fn valid_witness_identity(identity: &str) -> Option<()> {
    let mut segments = identity.split("::");
    valid_identifier(segments.next()?)?;
    for segment in segments {
        valid_identifier(segment)?;
    }
    Some(())
}

#[cfg(test)]
fn valid_identifier(identifier: &str) -> Option<()> {
    (!identifier.is_empty()
        && identifier.chars().enumerate().all(|(index, character)| {
            character == '_'
                || character.is_ascii_alphanumeric() && (index > 0 || !character.is_ascii_digit())
        }))
    .then_some(())
}

pub(in crate::artifact_verify) fn verify_detector_harness_error_invocations(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    source: &str,
    test_name: &str,
    oracle_check_id: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    super::outcome::verify_harness_error_test_invocations(
        bundle,
        check,
        source,
        test_name,
        oracle_check_id,
        root,
    )?;
    let processes = crate::evidence::format::process::parse_combined_processes(source)
        .map_err(|error| AggregateError::new(format!("parse detector invocation: {error}")))?;
    if let Some(exact) = processes
        .iter()
        .find(|process| process.label == "exact libtest execution")
    {
        if exact.schema_version != crate::evidence::format::process::DETECTOR_PROCESS_SCHEMA_VERSION
        {
            return Err(AggregateError::new(
                "detector harness-error exact invocation uses the wrong process schema".to_owned(),
            ));
        }
        verify_detector_harness_challenge(exact.detector_challenge.as_deref())?;
    }
    Ok(())
}

pub(super) fn verify_detector_harness_challenge(
    challenge: Option<&str>,
) -> Result<(), AggregateError> {
    let challenge = challenge.ok_or_else(|| {
        AggregateError::new("detector harness-error log omitted its challenge".to_owned())
    })?;
    crate::evidence::detector_proof::validate_challenge(challenge).map_err(|error| {
        AggregateError::new(format!(
            "detector harness-error challenge is invalid: {error}"
        ))
    })
}
