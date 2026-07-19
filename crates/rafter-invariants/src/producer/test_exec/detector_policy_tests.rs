//! Producer detector-proof acceptance scenarios.

use super::*;

const TOKEN: &str = "source-bound-token";
const WITNESS: &str = "expect-err:fixture::detect";

fn transcript(token: &str, challenge: &str) -> String {
    format!(
        "{}{token}:{WITNESS}()\n{}{token}:{WITNESS}():{challenge}\n",
        detector_proof::WITNESS_PREFIX,
        detector_proof::PROOF_PREFIX,
    )
}

#[test]
fn producer_policy_requires_its_exact_token_challenge_and_inventory() {
    let challenge = detector_proof::encode_challenge(&[0x5a; detector_proof::CHALLENGE_BYTES]);
    assert_eq!(
        classify_transcript("", &transcript(TOKEN, &challenge), TOKEN, &challenge)
            .expect("matching producer proof"),
        BTreeMap::from([(WITNESS.to_owned(), 1)])
    );
    assert!(
        classify_transcript("", &transcript("foreign", &challenge), TOKEN, &challenge).is_err()
    );
    assert!(
        classify_transcript("", &transcript(TOKEN, &"0".repeat(64)), TOKEN, &challenge).is_err()
    );
    assert!(classify_transcript(
        "",
        &format!("{}{TOKEN}:{WITNESS}()", detector_proof::WITNESS_PREFIX),
        TOKEN,
        &challenge
    )
    .is_err());
}
