//! Adversarial duplicate-property scenarios at each receipt object layer.

use super::super::json::decode_unique_value;

#[test]
fn nested_duplicate_properties_are_rejected_before_normalization() {
    for source in [
        br#"{"schema_version":14,"schema_version":14}"#.as_slice(),
        br#"{"results":[{"status":"error","status":"pass"}]}"#.as_slice(),
        br#"{"execution":{"source":{"commit":"a","commit":"b"}}}"#.as_slice(),
        br#"{"execution":{"invocation":{"program":"a","program":"b"}}}"#.as_slice(),
        br#"{"execution":{"invocation":{"environment":{"PATH":"a","PATH":"b"}}}}"#.as_slice(),
    ] {
        let error = decode_unique_value(source).expect_err("duplicate property must be malformed");
        assert!(
            error.to_string().contains("duplicate JSON object property"),
            "unexpected decoder error: {error}"
        );
    }
}

#[test]
fn distinct_properties_retain_their_json_shape() {
    let source = br#"{"schema_version":14,"results":[{"status":"pass"}],"execution":{"invocation":{"environment":{"PATH":"a"}}}}"#;
    let decoded = decode_unique_value(source).expect("unique properties decode");

    assert_eq!(decoded["schema_version"], 14);
    assert_eq!(decoded["results"][0]["status"], "pass");
    assert_eq!(
        decoded["execution"]["invocation"]["environment"]["PATH"],
        "a"
    );
}
