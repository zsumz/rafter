//! Tests for neutral TLC evidence parsing.

use super::{parse, parse_complete_prefix, parse_latest_progress, TlcProgress, TlcSummary};

#[test]
fn parses_framed_success_with_grouped_counts() {
    let summary = parse(
        b"@!@!@STARTMSG 2193:0 @!@!@\nNo error.\n@!@!@ENDMSG 2193 @!@!@\n\
          @!@!@STARTMSG 2199:0 @!@!@\n130,123,456 states generated, 120,000,001 distinct states found, 0 states left on queue.\n@!@!@ENDMSG 2199 @!@!@\n\
          @!@!@STARTMSG 2194:0 @!@!@\nThe depth of the complete state graph search is 19.\n@!@!@ENDMSG 2194 @!@!@\n\
          @!@!@STARTMSG 2186:0 @!@!@\nFinished.\n@!@!@ENDMSG 2186 @!@!@\n",
    )
    .expect("framed output parses");
    assert_eq!(
        summary,
        TlcSummary {
            generated_states: 130_123_456,
            distinct_states: 120_000_001,
            states_left: 0,
            search_depth: 19,
            completed_without_error: true,
            process_finished: true,
            violated_invariant: None,
        }
    );
}

#[test]
fn extracts_named_invariant_counterexample() {
    let summary = parse(
        b"@!@!@STARTMSG 2107:1 @!@!@\nInvariant ElectionSafety is violated.\n@!@!@ENDMSG 2107 @!@!@\n",
    )
    .expect("violation frame parses");
    assert_eq!(
        summary.violated_invariant.as_deref(),
        Some("ElectionSafety")
    );
    assert!(!summary.completed_without_error);
}

#[test]
fn complete_violation_survives_a_truncated_trailing_frame() {
    let output = b"@!@!@STARTMSG 2110:1 @!@!@\nInvariant ElectionSafety is violated.\n@!@!@ENDMSG 2110 @!@!@\n\
@!@!@STARTMSG 2200:0 @!@!@\nProgress(3) at";
    assert!(parse(output).is_err());
    assert_eq!(
        parse_complete_prefix(output)
            .expect("parse complete frame prefix")
            .violated_invariant
            .as_deref(),
        Some("ElectionSafety")
    );
}

#[test]
fn complete_violation_survives_later_malformed_terminal_frames() {
    let violation = "@!@!@STARTMSG 2110:1 @!@!@\nInvariant ElectionSafety is violated.\n@!@!@ENDMSG 2110 @!@!@\n";
    let cases = [
        (
            format!(
                "{violation}@!@!@STARTMSG 2199:0 @!@!@\nmalformed statistics\n@!@!@ENDMSG 2199 @!@!@\n"
            ),
            "TLC 2199 frame has malformed state statistics",
            0,
        ),
        (
            format!(
                "{violation}@!@!@STARTMSG 2199:0 @!@!@\n2 states generated, 2 distinct states found, 0 states left on queue.\n@!@!@ENDMSG 2199 @!@!@\n\
                 @!@!@STARTMSG 2194:0 @!@!@\nmalformed depth\n@!@!@ENDMSG 2194 @!@!@\n"
            ),
            "TLC 2194 frame has malformed search depth",
            2,
        ),
    ];

    for (output, expected_error, expected_states) in cases {
        assert_eq!(parse(output.as_bytes()).unwrap_err(), expected_error);
        let summary = parse_complete_prefix(output.as_bytes()).expect("recover named violation");
        assert_eq!(
            summary.violated_invariant.as_deref(),
            Some("ElectionSafety")
        );
        assert_eq!(summary.generated_states, expected_states);
    }

    assert!(parse_complete_prefix(
        b"@!@!@STARTMSG 2199:0 @!@!@\nmalformed statistics\n@!@!@ENDMSG 2199 @!@!@\n"
    )
    .is_err());
}

#[test]
fn complete_violation_survives_later_framing_and_duplicate_defects() {
    let violation = "@!@!@STARTMSG 2110:1 @!@!@\nInvariant ElectionSafety is violated.\n@!@!@ENDMSG 2110 @!@!@\n";
    let statistics = "@!@!@STARTMSG 2199:0 @!@!@\n2 states generated, 2 distinct states found, 0 states left on queue.\n@!@!@ENDMSG 2199 @!@!@\n";
    let cases = [
        format!("{violation}{statistics}{statistics}"),
        format!(
            "{violation}@!@!@STARTMSG 2194:0 @!@!@\nThe depth of the complete state graph search is 2.\n@!@!@ENDMSG 2199 @!@!@\n"
        ),
        format!(
            "{violation}@!@!@STARTMSG 2194:0 @!@!@\n@!@!@STARTMSG 2199:0 @!@!@\n"
        ),
    ];

    for output in cases {
        assert!(parse(output.as_bytes()).is_err());
        assert_eq!(
            parse_complete_prefix(output.as_bytes())
                .expect("recover named violation before parser defect")
                .violated_invariant
                .as_deref(),
            Some("ElectionSafety")
        );
    }
}

#[test]
fn rejects_truncated_or_duplicate_terminal_frames() {
    assert!(parse(b"@!@!@STARTMSG 2193:0 @!@!@\n").is_err());
    assert!(parse(
        b"@!@!@STARTMSG 2193:0 @!@!@\nA\n@!@!@ENDMSG 2193 @!@!@\n\
          @!@!@STARTMSG 2193:0 @!@!@\nB\n@!@!@ENDMSG 2193 @!@!@\n"
    )
    .is_err());
}

#[test]
fn rejects_unframed_success_prose() {
    assert!(parse(
        b"Model checking completed. No error has been found. 120,000,001 distinct states found."
    )
    .is_err());
}

#[test]
fn parses_the_latest_complete_progress_frame() {
    let progress = parse_latest_progress(
        b"@!@!@STARTMSG 2200:0 @!@!@\nProgress(21) at 2026-07-13 19:18:31: 23,784,130 states generated (4,670,725 s/min), 6,246,309 distinct states found (1,150,848 ds/min), 3,294,097 states left on queue.\n@!@!@ENDMSG 2200 @!@!@\n\
          @!@!@STARTMSG 2200:0 @!@!@\nProgress(23) at 2026-07-13 19:52:32: 181,490,601 states generated (4,966,137 s/min), 40,062,465 distinct states found (1,000,915 ds/min), 19,012,042 states left on queue.\n@!@!@ENDMSG 2200 @!@!@\n\
          @!@!@STARTMSG 2200:0 @!@!@\nProgress(24) at",
    )
    .expect("complete progress frames parse")
    .expect("latest progress exists");
    assert_eq!(
        progress,
        TlcProgress {
            generated_states: 181_490_601,
            distinct_states: 40_062_465,
            states_left: 19_012_042,
            depth: 23,
        }
    );
}

#[test]
fn rejects_a_malformed_complete_progress_frame() {
    assert!(parse_latest_progress(
        b"@!@!@STARTMSG 2200:0 @!@!@\nProgress without statistics.\n@!@!@ENDMSG 2200 @!@!@\n"
    )
    .is_err());
}
