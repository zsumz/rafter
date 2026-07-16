//! Public symbol, error-propagation, and operator-diagnostic contracts.

use rafter_codec::{
    decode_message, DecodePeerMessageError, EncodePeerMessageError, MAGIC, VERSION,
};

#[test]
fn public_constants_pin_the_current_frame_identity() {
    assert_eq!(MAGIC, *b"RFPM");
    assert_eq!(VERSION, 1);
}

#[test]
fn errors_propagate_with_question_mark() {
    fn decode_garbage() -> Result<(), Box<dyn std::error::Error>> {
        decode_message(b"XXXX not a peer message")?;
        Ok(())
    }

    let error = decode_garbage().expect_err("garbage bytes must not decode");
    assert_eq!(
        error.to_string(),
        "peer message magic [58, 58, 58, 58] is not RFPM"
    );
}

#[test]
fn public_error_diagnostics_include_actionable_limits() {
    let error = EncodePeerMessageError::FieldTooLarge {
        field: "membership_voter_count",
        len: 65_536,
        max: 65_535,
    };
    assert_eq!(
        error.to_string(),
        "peer message field membership_voter_count has length 65536, exceeding the wire maximum 65535"
    );

    let decode = DecodePeerMessageError::NonCanonicalMembershipOrder {
        field: "membership_voters",
    };
    assert!(decode.to_string().contains("strictly increasing"));
}
