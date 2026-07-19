//! Wire-compatibility and fail-closed decoding scenarios for process evidence.

use std::collections::BTreeMap;

use super::{
    encode_combined_v3, encode_detector_v4, encode_maelstrom_v2, encode_tla_v3,
    parse_combined_processes, parse_combined_v3, parse_maelstrom_v2, parse_tla_v3,
    ProcessFormatError, ProcessObservation, TerminationReceipt,
};
use crate::evidence::InvocationReceipt;

const PROGRAM_SHA256: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ENVIRONMENT_SHA256: &str = "1111111111111111111111111111111111111111111111111111111111111111";

fn invocation() -> InvocationReceipt {
    InvocationReceipt {
        program: "cargo".to_owned(),
        program_sha256: PROGRAM_SHA256.to_owned(),
        arguments: vec!["test".to_owned()],
        current_dir: "/workspace/rafter".to_owned(),
        environment: BTreeMap::new(),
        environment_sha256: ENVIRONMENT_SHA256.to_owned(),
    }
}

fn observation<'a>(
    invocation: &'a InvocationReceipt,
    termination: Option<&'a TerminationReceipt>,
    stdout: &'a [u8],
    stderr: &'a [u8],
) -> ProcessObservation<'a> {
    ProcessObservation {
        invocation,
        exit_code: Some(0),
        timed_out: false,
        termination,
        duration_ms: 17,
        peak_rss_kib: 4096,
        stdout,
        stderr,
    }
}

#[test]
fn tla_v3_wire_is_byte_stable_and_strictly_versioned() {
    let invocation = invocation();
    let termination = TerminationReceipt {
        process_group: true,
        term_signal_sent: false,
        grace_ms: 30_000,
        kill_signal_sent: false,
    };
    let encoded = String::from_utf8(
        encode_tla_v3(
            "model-check",
            observation(&invocation, Some(&termination), b"states=44\n", b""),
        )
        .expect("encode TLA process receipt"),
    )
    .expect("TLA process receipt is UTF-8");
    let expected = format!(
        concat!(
            "{{\n",
            "  \"schema_version\": 3,\n",
            "  \"label\": \"model-check\",\n",
            "  \"invocation\": {{\n",
            "    \"program\": \"cargo\",\n",
            "    \"program_sha256\": \"{}\",\n",
            "    \"arguments\": [\n",
            "      \"test\"\n",
            "    ],\n",
            "    \"current_dir\": \"/workspace/rafter\",\n",
            "    \"environment\": {{}},\n",
            "    \"environment_sha256\": \"{}\"\n",
            "  }},\n",
            "  \"exit_code\": 0,\n",
            "  \"timed_out\": false,\n",
            "  \"termination\": {{\n",
            "    \"process_group\": true,\n",
            "    \"term_signal_sent\": false,\n",
            "    \"grace_ms\": 30000,\n",
            "    \"kill_signal_sent\": false\n",
            "  }},\n",
            "  \"duration_ms\": 17,\n",
            "  \"peak_rss_kib\": 4096,\n",
            "  \"stdout\": \"states=44\\n\",\n",
            "  \"stderr\": \"\"\n",
            "}}"
        ),
        PROGRAM_SHA256, ENVIRONMENT_SHA256
    );
    assert_eq!(encoded, expected);
    let log = parse_tla_v3(&encoded).expect("parse stable TLA process receipt");
    assert_eq!(log.termination, Some(termination));
    assert!(parse_maelstrom_v2(&encoded).is_err());

    let mut value = serde_json::to_value(&log).expect("convert process receipt");
    value["trusted"] = serde_json::json!(true);
    assert!(parse_tla_v3(&serde_json::to_string(&value).unwrap()).is_err());
}

#[test]
fn maelstrom_v2_wire_is_byte_stable_and_forbids_termination() {
    let invocation = invocation();
    let encoded = String::from_utf8(
        encode_maelstrom_v2("base", observation(&invocation, None, b"nemesis=ok\n", b""))
            .expect("encode Maelstrom process receipt"),
    )
    .expect("Maelstrom process receipt is UTF-8");
    let expected = format!(
        concat!(
            "{{\n",
            "  \"schema_version\": 2,\n",
            "  \"label\": \"base\",\n",
            "  \"invocation\": {{\n",
            "    \"program\": \"cargo\",\n",
            "    \"program_sha256\": \"{}\",\n",
            "    \"arguments\": [\n",
            "      \"test\"\n",
            "    ],\n",
            "    \"current_dir\": \"/workspace/rafter\",\n",
            "    \"environment\": {{}},\n",
            "    \"environment_sha256\": \"{}\"\n",
            "  }},\n",
            "  \"exit_code\": 0,\n",
            "  \"timed_out\": false,\n",
            "  \"duration_ms\": 17,\n",
            "  \"peak_rss_kib\": 4096,\n",
            "  \"stdout\": \"nemesis=ok\\n\",\n",
            "  \"stderr\": \"\"\n",
            "}}"
        ),
        PROGRAM_SHA256, ENVIRONMENT_SHA256
    );
    assert_eq!(encoded, expected);
    assert!(parse_maelstrom_v2(&encoded).is_ok());
    assert!(parse_tla_v3(&encoded).is_err());

    let with_termination = encoded.replace(
        "  \"duration_ms\": 17,",
        "  \"termination\": {\"process_group\":true,\"term_signal_sent\":false,\"grace_ms\":1,\"kill_signal_sent\":false},\n  \"duration_ms\": 17,",
    );
    assert!(parse_maelstrom_v2(&with_termination).is_err());
}

#[test]
fn combined_framing_preserves_payload_boundaries_and_detector_challenge() {
    let invocation_receipt = invocation();
    let invocation = serde_json::to_string(&invocation_receipt).expect("serialize invocation");
    let plain = format!(
        "schema_version: 3\nlabel: compile\ninvocation: {invocation}\nexit_code: Some(0)\ntimed_out: false\nduration_ms: 7\npeak_rss_kib: 9\nstdout_bytes: 3\nstderr_bytes: 3\n\nok\nerr"
    );
    let plain_observation = ProcessObservation {
        invocation: &invocation_receipt,
        exit_code: Some(0),
        timed_out: false,
        termination: None,
        duration_ms: 7,
        peak_rss_kib: 9,
        stdout: b"ok\n",
        stderr: b"err",
    };
    assert_eq!(
        encode_combined_v3("compile", plain_observation).expect("encode combined receipt"),
        plain.as_bytes()
    );
    let challenge = "5a".repeat(32);
    let detector = format!(
        "schema_version: 4\nlabel: detector\ninvocation: {invocation}\ndetector_challenge: {challenge}\nexit_code: None\ntimed_out: true\nduration_ms: 11\npeak_rss_kib: 13\nstdout_bytes: 4\nstderr_bytes: 0\n\npass"
    );

    let adjacent = plain + &detector;
    let parsed = parse_combined_processes(&adjacent).expect("parse adjacent frames");
    assert!(parse_combined_v3(&adjacent).is_err());
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].schema_version, 3);
    assert_eq!(parsed[0].stdout, "ok\n");
    assert_eq!(parsed[0].stderr, "err");
    assert_eq!(parsed[0].metrics.duration_ms, 7);
    assert_eq!(parsed[0].detector_challenge, None);
    assert_eq!(parsed[1].stdout, "pass");
    assert_eq!(parsed[1].exit_code, None);
    assert!(parsed[1].timed_out);
    assert_eq!(parsed[1].schema_version, 4);
    assert_eq!(
        parsed[1].detector_challenge.as_deref(),
        Some(challenge.as_str())
    );
}

#[test]
fn encoders_reject_ambiguous_labels_challenges_and_non_utf8_output() {
    let invocation = invocation();
    let invalid_utf8 = observation(&invocation, None, &[0xff], b"");
    assert!(encode_combined_v3("compile", invalid_utf8).is_err());
    assert!(encode_maelstrom_v2("base", invalid_utf8).is_err());
    assert!(
        encode_combined_v3("compile\nforged", observation(&invocation, None, b"", b"")).is_err()
    );
    assert!(encode_detector_v4(
        "detector",
        observation(&invocation, None, b"", b""),
        &"A".repeat(64),
    )
    .is_err());
}

#[test]
fn formats_cannot_silently_discard_termination_evidence() {
    let invocation = invocation();
    let termination = TerminationReceipt {
        process_group: true,
        term_signal_sent: false,
        grace_ms: 30_000,
        kill_signal_sent: false,
    };
    let observed = observation(&invocation, Some(&termination), b"ok", b"");
    assert!(encode_combined_v3("compile", observed).is_err());
    assert!(encode_maelstrom_v2("base", observed).is_err());
    assert!(encode_tla_v3("model-check", observed).is_ok());
}

#[test]
fn combined_parser_rejects_incomplete_and_malformed_frames() {
    let invocation = serde_json::to_string(&invocation()).expect("serialize invocation");
    let valid = format!(
        "schema_version: 3\nlabel: compile\ninvocation: {invocation}\nexit_code: Some(0)\ntimed_out: false\nduration_ms: 7\npeak_rss_kib: 9\nstdout_bytes: 2\nstderr_bytes: 0\n\nok"
    );
    assert!(matches!(
        parse_combined_processes(""),
        Err(ProcessFormatError::EmptyTranscript)
    ));
    assert!(matches!(
        parse_combined_processes(&valid.replace("schema_version: 3", "schema_version: 2")),
        Err(ProcessFormatError::UnsupportedCombinedSchema(2))
    ));
    assert!(
        parse_combined_processes(&valid.replace("peak_rss_kib: 9", "peak_rss_kib: 0")).is_err()
    );
    assert!(
        parse_combined_processes(&valid.replace("stdout_bytes: 2", "stdout_bytes: 3")).is_err()
    );
    assert!(parse_combined_processes(&format!(
        "schema_version: 4\nlabel: detector\ninvocation: {invocation}\ndetector_challenge: ABC\nexit_code: Some(0)\ntimed_out: false\nduration_ms: 1\npeak_rss_kib: 1\nstdout_bytes: 0\nstderr_bytes: 0\n\n"
    ))
    .is_err());
}
