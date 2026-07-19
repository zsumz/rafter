//! Producer-owned acceptance policy for detector proof transcripts.

use std::collections::BTreeMap;

use crate::evidence::detector_proof::{self, TranscriptRecord};

pub(super) fn classify_transcript(
    stdout: &str,
    stderr: &str,
    expected_token: &str,
    expected_challenge: &str,
) -> Result<BTreeMap<String, usize>, String> {
    detector_proof::validate_challenge(expected_challenge)?;
    let records = detector_proof::decode_transcript(stdout, stderr)?;
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
                    return Err(
                        "detector proof used the wrong post-invocation challenge".to_owned()
                    );
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
#[path = "detector_policy_tests.rs"]
mod tests;
