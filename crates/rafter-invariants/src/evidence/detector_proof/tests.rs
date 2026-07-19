//! Detector proof wire-decoding scenarios.

use std::path::Path;

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
fn proof_socket_path_has_one_exact_managed_shape() {
    assert!(managed_socket_path(Path::new(
        "target/rafter-invariants/tmp/detector-proof/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.sock"
    )));
    for path in [
        "/target/rafter-invariants/tmp/detector-proof/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.sock",
        "target/rafter-invariants/tmp/detector-proof/nested/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.sock",
        "target/rafter-invariants/tmp/detector-proof/12-3.sock",
        "target/rafter-invariants/tmp/detector-proof/12-3-5A5A5A5A5A5A5A5A5A5A5A5A5A5A5A5A.sock",
        "target/rafter-invariants/tmp/detector-proof/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.txt",
    ] {
        assert!(!managed_socket_path(Path::new(path)), "accepted {path}");
    }
}
