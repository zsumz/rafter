//! Verifier-owned qualification of one detector execution transcript.

use std::collections::BTreeMap;

use crate::evidence::{
    detector_proof::{self, TranscriptRecord},
    format::libtest,
};

pub(crate) fn qualify_detector_execution(
    stdout: &[u8],
    stderr: &[u8],
    test_name: &str,
    token: &str,
    challenge: &str,
    expected_witnesses: &BTreeMap<String, usize>,
) -> Result<(), String> {
    if !libtest::exact_pass(stdout, test_name) {
        return Err(
            "detector replay did not contain one exact passing libtest execution".to_owned(),
        );
    }
    let markers = libtest::oracle_markers(stdout, stderr, token)
        .ok_or_else(|| "detector replay contains an oracle marker for another token".to_owned())?;
    if markers.observed != 1 || markers.violations != 0 {
        return Err(format!(
            "detector replay requires one observation and no violation markers, observed={} violations={}",
            markers.observed, markers.violations
        ));
    }
    verify_detector_transcript_bytes(stdout, stderr, token, challenge, expected_witnesses)
}

pub(crate) fn verify_detector_transcript(
    stdout: &str,
    stderr: &str,
    expected_token: &str,
    expected_challenge: &str,
    expected_witnesses: &BTreeMap<String, usize>,
) -> Result<(), String> {
    verify_detector_transcript_bytes(
        stdout.as_bytes(),
        stderr.as_bytes(),
        expected_token,
        expected_challenge,
        expected_witnesses,
    )
}

fn verify_detector_transcript_bytes(
    stdout: &[u8],
    stderr: &[u8],
    expected_token: &str,
    expected_challenge: &str,
    expected_witnesses: &BTreeMap<String, usize>,
) -> Result<(), String> {
    let witnesses = transcript_witnesses(stdout, stderr, expected_token, expected_challenge)?;
    if &witnesses != expected_witnesses {
        return Err(format!(
            "detector log witness contract mismatch: expected {expected_witnesses:?}, observed {witnesses:?}"
        ));
    }
    Ok(())
}

fn transcript_witnesses(
    stdout: &[u8],
    stderr: &[u8],
    expected_token: &str,
    expected_challenge: &str,
) -> Result<BTreeMap<String, usize>, String> {
    detector_proof::validate_challenge(expected_challenge)
        .map_err(|error| format!("detector proof failed: {error}"))?;
    let records = detector_proof::decode_transcript_bytes(stdout, stderr)
        .map_err(|error| format!("detector proof failed: {error}"))?;
    let mut witnesses = BTreeMap::<String, usize>::new();
    let mut proofs = BTreeMap::<String, usize>::new();
    for record in records {
        match record {
            TranscriptRecord::Witness { token, witness } => {
                if token != expected_token {
                    return Err("detector witness is bound to another execution token".to_owned());
                }
                *witnesses.entry(witness).or_default() += 1;
            }
            TranscriptRecord::Proof {
                token,
                witness,
                challenge,
            } => {
                if token != expected_token {
                    return Err("detector proof is bound to another execution token".to_owned());
                }
                if challenge != expected_challenge {
                    return Err("detector proof used the wrong pre-body challenge".to_owned());
                }
                *proofs.entry(witness).or_default() += 1;
            }
        }
    }
    if witnesses.is_empty() {
        return Err("detector transcript contains no runtime witnesses".to_owned());
    }
    if witnesses != proofs {
        return Err(format!(
            "detector witness and proof inventories differ: witnesses={witnesses:?}, proofs={proofs:?}"
        ));
    }
    Ok(witnesses)
}

#[cfg(test)]
#[path = "transcript/tests.rs"]
mod tests;
