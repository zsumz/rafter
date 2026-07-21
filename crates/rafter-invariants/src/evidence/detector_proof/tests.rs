//! Detector proof wire-decoding scenarios.

use super::*;

#[test]
fn transcript_decoding_preserves_markers_without_deciding_acceptance() {
    let challenge = encode_challenge(&[0x5a; CHALLENGE_BYTES]);
    let token = "token";
    let witness = "expect-err:crate_name::detector";
    let stderr = format!(
        "{WITNESS_PREFIX}{token}:{witness}()\n{PROOF_PREFIX}other-token:{witness}():{challenge}\n"
    );
    assert_eq!(
        decode_transcript("", &stderr).expect("valid wire transcript"),
        vec![
            TranscriptRecord::Witness {
                token: token.to_owned(),
                witness: witness.to_owned(),
            },
            TranscriptRecord::Proof {
                token: "other-token".to_owned(),
                witness: witness.to_owned(),
                challenge: challenge.clone(),
            },
        ]
    );
    assert!(decode_transcript(
        "",
        &format!("{PROOF_PREFIX}{token}:{witness}():not-a-challenge")
    )
    .is_err());
}

#[test]
fn proof_descriptor_has_one_canonical_shape() {
    assert!(canonical_descriptor("3"));
    assert!(canonical_descriptor("2147483647"));
    for descriptor in ["", "0", "2", "03", "+3", "-1", "3 ", "2147483648"] {
        assert!(!canonical_descriptor(descriptor), "accepted {descriptor}");
    }
}
