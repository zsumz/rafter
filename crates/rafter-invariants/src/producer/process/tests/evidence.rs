//! Process receipt framing, timeout classification, and schema scenarios.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};

use super::{
    super::{combined_detector_log, combined_log, json_log, timed_with_timeout},
    support::unique_test_path,
};
use crate::{
    evidence::format::process::{parse_combined_processes, ProcessLog},
    provenance::invocation::digest_environment,
};

#[test]
fn timed_child_is_killed_at_its_soft_timeout() {
    let environment = super::super::base_environment();
    let output = timed_with_timeout(
        "sleep",
        &[OsString::from("5")],
        &environment,
        Path::new("."),
        Duration::from_millis(10),
    )
    .expect("timed child produces telemetry");

    assert!(output.timed_out);
    assert!(!output.status.success());
    assert!(output.duration < Duration::from_secs(2));
    assert!(output.peak_rss_kib > 0);
    assert_eq!(output.invocation.program, "sleep");
    assert_eq!(output.invocation.arguments, ["5"]);
    assert_eq!(
        output.invocation.current_dir,
        std::fs::canonicalize(".")
            .expect("working directory canonicalizes")
            .to_string_lossy()
            .into_owned()
    );
    assert_eq!(
        output.invocation.environment_sha256,
        digest_environment(&environment).expect("valid environment")
    );

    let plain = String::from_utf8(combined_log("timeout", &output).expect("log serializes"))
        .expect("plain process log is UTF-8");
    assert!(plain.starts_with("schema_version: 4\nlabel: timeout\ninvocation: {"));
    assert!(plain.contains("\"program\":\"sleep\""));
    let parsed = parse_combined_processes(&plain).expect("combined metrics parse");
    assert_eq!(parsed.len(), 1);
    assert!(parsed[0].timed_out);
    assert_ne!(parsed[0].exit_code, Some(0));
    assert_eq!(parsed[0].metrics.peak_rss_kib, output.peak_rss_kib);
    assert!(parsed[0].detector_challenge.is_none());
    let challenge = "5a".repeat(32);
    let detector = String::from_utf8(
        combined_detector_log("timeout", &output, &challenge).expect("detector log serializes"),
    )
    .expect("detector process log is UTF-8");
    let detector = parse_combined_processes(&detector).expect("detector process log parses");
    assert_eq!(
        detector[0].detector_challenge.as_deref(),
        Some(challenge.as_str())
    );
    let structured: ProcessLog = serde_json::from_slice(
        &json_log("timeout", &output).expect("structured process log serializes"),
    )
    .expect("structured process log parses");
    assert_eq!(structured.schema_version, 3);
    assert!(structured.termination.is_none());
    assert_eq!(structured.invocation, output.invocation);
}

#[test]
fn timed_out_process_transcript_retains_output_and_timeout_classification() {
    let output = timed_with_timeout(
        "sh",
        &[
            OsString::from("-c"),
            OsString::from("printf retained-before-timeout; sleep 5"),
        ],
        &super::super::base_environment(),
        Path::new("."),
        Duration::from_millis(10),
    )
    .expect("timed-out process returns a replayable receipt");
    assert!(output.timed_out);
    assert_eq!(output.stdout, b"retained-before-timeout");

    let unique = unique_test_path("timeout-artifact");
    let directory = PathBuf::from("target/rafter-invariants/test-artifacts").join(
        unique
            .file_name()
            .expect("timeout artifact path has a file name"),
    );
    let bytes = combined_log("timeout-retention", &output).expect("frame timeout transcript");
    let artifact =
        crate::producer::artifact::write(&directory, Path::new("timeout.log"), "test-log", &bytes)
            .expect("persist timeout transcript");
    let retained = std::fs::read_to_string(&artifact.path).expect("read timeout transcript");
    let [parsed] = parse_combined_processes(&retained)
        .expect("parse retained timeout transcript")
        .try_into()
        .expect("one retained process");
    assert!(parsed.timed_out);
    assert_eq!(parsed.stdout, "retained-before-timeout");
    assert_eq!(artifact.size_bytes, bytes.len() as u64);
    std::fs::remove_dir_all(directory).expect("remove timeout artifact directory");
}

#[test]
fn combined_processes_preserve_failed_and_timed_out_semantic_statuses() {
    let source = concat!(
        "schema_version: 4\n",
        "label: test\n",
        "invocation: {\"program\":\"cargo\",\"program_sha256\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"arguments\":[\"test\"],\"current_dir\":\"/workspace\",\"environment\":{},\"environment_sha256\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\",\"launchers\":[]}\n",
        "exit_code: Some(0)\n",
        "timed_out: false\n",
        "duration_ms: 1\n",
        "peak_rss_kib: 1\n",
        "stdout_bytes: 2\n",
        "stderr_bytes: 0\n",
        "\n",
        "ok",
    );
    let parsed = parse_combined_processes(source).expect("successful receipt parses");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].stdout, "ok");
    assert_eq!(parsed[0].stderr, "");
    assert!(parsed[0].detector_challenge.is_none());
    let challenge = "5a".repeat(32);
    let detector_source = source
        .replacen("schema_version: 4", "schema_version: 5", 1)
        .replacen(
            "\nexit_code:",
            &format!("\ndetector_challenge: {challenge}\nexit_code:"),
            1,
        );
    let detector =
        parse_combined_processes(&detector_source).expect("detector process receipt parses");
    assert_eq!(
        detector[0].detector_challenge.as_deref(),
        Some(challenge.as_str())
    );
    assert!(
        parse_combined_processes(&detector_source.replace(&challenge, &"A".repeat(64))).is_err()
    );
    let failed = parse_combined_processes(&source.replace("Some(0)", "Some(1)"))
        .expect("failed semantic receipt remains parseable");
    assert_eq!(failed[0].exit_code, Some(1));
    let timed_out = parse_combined_processes(&source.replace("false", "true"))
        .expect("timed-out semantic receipt remains parseable");
    assert!(timed_out[0].timed_out);
    assert!(parse_combined_processes(&source.replace("Some(0)", "0")).is_err());
    assert!(
        parse_combined_processes(&source.replace("stdout_bytes: 2", "stdout_bytes: 20")).is_err()
    );
    assert!(parse_combined_processes(&format!("{source}trailing junk")).is_err());
}

#[test]
fn length_framing_preserves_process_log_tokens_inside_stdout() {
    let payload = "schema_version: 3\n\nstdout_bytes: 999\n--- stderr ---";
    let output = timed_with_timeout(
        "printf",
        &[OsString::from("%s"), OsString::from(payload)],
        &super::super::base_environment(),
        Path::new("."),
        Duration::from_secs(2),
    )
    .expect("capture adversarial stdout");
    let log = String::from_utf8(combined_log("framing", &output).expect("serialize log"))
        .expect("combined log is UTF-8");
    let [parsed] = parse_combined_processes(&log)
        .expect("length framing ignores payload tokens")
        .try_into()
        .expect("one process receipt");
    assert_eq!(parsed.stdout, payload);
    assert_eq!(parsed.stderr, "");
}

#[test]
fn structured_process_log_rejects_unknown_fields() {
    let source = r#"{
            "schema_version": 4,
            "label": "model-check",
            "invocation": {
                "program": "java",
                "program_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "arguments": ["-jar", "tla2tools.jar"],
                "current_dir": "/workspace/rafter",
                "environment": {},
                "environment_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "launchers": []
            },
            "exit_code": 0,
            "timed_out": false,
            "termination": null,
            "duration_ms": 1,
            "peak_rss_kib": 1,
            "stdout": "",
            "stderr": "",
            "trusted": true
        }"#;
    assert!(serde_json::from_str::<ProcessLog>(source).is_err());
}
