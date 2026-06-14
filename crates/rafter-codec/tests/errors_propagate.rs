use rafter_codec::decode_message;

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
