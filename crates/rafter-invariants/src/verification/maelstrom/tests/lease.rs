//! Scenarios for independent Maelstrom artifact verification.

use super::{
    finalize_lease_scan, history_completion_count, history_completion_count_with_limits,
    scan_markers, scan_markers_with_limits, trial_floors_met, ArtifactLeaseMarker, HistoryLimits,
    LeaseArtifactStatus, MarkerLimits, Scenario, MARKERS,
};
use crate::evidence::format::maelstrom::{MaelstromSummary, Validity};

fn summary() -> MaelstromSummary {
    MaelstromSummary {
        validity: Validity::Valid,
        linearizability: Validity::Valid,
        operation_count: 3,
        ok_count: 3,
        read_ok: 1,
        write_ok: 1,
        cas_ok: 1,
    }
}

fn good() -> String {
    [
        "seq=1 node=n1 term=3 phase=fast-path-read-ok client=c0 msg_id=7",
        "seq=2 node=n1 term=3 phase=lease-expired client=c0 msg_id=7",
        "seq=3 node=n1 term=3 phase=read-buffered client=c1 msg_id=11",
        "seq=4 node=n1 term=3 phase=post-expiry-released client=c1 msg_id=11",
        "seq=5 node=n1 term=3 phase=post-expiry-handler client=c1 msg_id=11",
        "seq=6 node=n1 term=3 phase=post-expiry-unavailable client=c1 msg_id=11",
    ]
    .into_iter()
    .map(|fields| format!("rafter-maelstrom lease-isolation {fields}"))
    .collect::<Vec<_>>()
    .join("\n")
}

fn scan(
    source: &str,
) -> (
    LeaseArtifactStatus,
    std::collections::BTreeMap<&'static str, u64>,
) {
    let mut markers = MARKERS.into_iter().map(|name| (name, 0)).collect();
    let mut events = Vec::<ArtifactLeaseMarker>::new();
    let mut errors = 0;
    scan_markers(source, "n1", &mut markers, &mut events, &mut errors)
        .expect("fixture markers stay within production limits");
    let status = finalize_lease_scan(&mut markers, &events, errors);
    (status, markers)
}

#[test]
fn lease_isolation_artifacts_require_the_complete_safe_sequence() {
    let (status, mut markers) = scan(&good());
    markers.insert("lease_history_probe_matches", 1);
    assert_eq!(status, LeaseArtifactStatus::Complete);
    assert!(trial_floors_met(
        Scenario::LeaseIsolation,
        &summary(),
        &markers,
        false
    ));
}

#[test]
fn detector_rejects_cross_node_cross_term_and_uncorrelated_events() {
    let cross_node = good().replacen("seq=4 node=n1", "seq=4 node=n2", 1);
    assert_eq!(scan(&cross_node).0, LeaseArtifactStatus::HarnessError);
    let cross_term = good().replacen("seq=5 node=n1 term=3", "seq=5 node=n1 term=4", 1);
    assert_eq!(scan(&cross_term).0, LeaseArtifactStatus::HarnessError);
    let uncorrelated = good().replacen(
        "phase=post-expiry-unavailable client=c1 msg_id=11",
        "phase=post-expiry-unavailable client=c1 msg_id=99",
        1,
    );
    assert_eq!(scan(&uncorrelated).0, LeaseArtifactStatus::HarnessError);
}

#[test]
fn detector_rejects_out_of_order_missing_and_duplicate_events() {
    let mut lines = good().lines().map(str::to_owned).collect::<Vec<_>>();
    lines.swap(1, 2);
    assert_eq!(scan(&lines.join("\n")).0, LeaseArtifactStatus::HarnessError);
    lines = good().lines().map(str::to_owned).collect::<Vec<_>>();
    lines.swap(3, 4);
    assert_eq!(scan(&lines.join("\n")).0, LeaseArtifactStatus::HarnessError);
    lines = good().lines().map(str::to_owned).collect();
    lines.pop();
    assert_eq!(scan(&lines.join("\n")).0, LeaseArtifactStatus::Incomplete);
    lines = good().lines().map(str::to_owned).collect();
    lines.insert(2, lines[1].clone());
    assert_eq!(scan(&lines.join("\n")).0, LeaseArtifactStatus::HarnessError);
}

#[test]
fn detector_classifies_read_ok_and_renewal_as_rd05_violations() {
    let served = good().replace(
        "phase=post-expiry-unavailable",
        "phase=post-expiry-read-served-violation",
    );
    let (status, markers) = scan(&served);
    assert_eq!(status, LeaseArtifactStatus::Violation);
    assert!(!trial_floors_met(
        Scenario::LeaseIsolation,
        &summary(),
        &markers,
        false
    ));
    let mut lines = good()
        .lines()
        .take(2)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    lines.push("rafter-maelstrom lease-isolation seq=3 node=n1 term=3 phase=post-expiry-renewed-violation client=c0 msg_id=7".to_owned());
    assert_eq!(scan(&lines.join("\n")).0, LeaseArtifactStatus::Violation);
}

#[test]
fn detector_treats_unexpected_error_and_malformed_marker_as_harness_errors() {
    let unexpected = good().replace(
        "phase=post-expiry-unavailable client=c1 msg_id=11",
        "phase=post-expiry-unexpected-error client=c1 msg_id=11 code=20",
    );
    assert_eq!(scan(&unexpected).0, LeaseArtifactStatus::HarnessError);
    let malformed = good().replacen("seq=1", "seq=1 extra=x", 1);
    assert_eq!(scan(&malformed).0, LeaseArtifactStatus::HarnessError);
}

#[test]
fn detector_rejects_second_correlated_terminal_after_either_result() {
    for phase in [
        "post-expiry-unavailable",
        "post-expiry-read-served-violation",
    ] {
        let mut source = good().replace("post-expiry-unavailable", phase);
        source.push_str("\nrafter-maelstrom lease-isolation seq=7 node=n1 term=3 phase=post-expiry-duplicate-terminal client=c1 msg_id=11");
        let (status, markers) = scan(&source);
        let expected = if phase == "post-expiry-read-served-violation" {
            LeaseArtifactStatus::ViolationWithHarnessError
        } else {
            LeaseArtifactStatus::HarnessError
        };
        assert_eq!(status, expected);
        assert_eq!(markers["lease_duplicate_terminal"], 1);

        let mut lines = good()
            .replace("post-expiry-unavailable", phase)
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        lines.insert(4, "rafter-maelstrom lease-isolation seq=5 node=n1 term=3 phase=post-expiry-duplicate-terminal client=c1 msg_id=11".to_owned());
        for (index, line) in lines.iter_mut().enumerate() {
            let (_, rest) = line.split_once(" node=").expect("marker has node");
            *line = format!(
                "rafter-maelstrom lease-isolation seq={} node={rest}",
                index + 1
            );
        }
        assert_eq!(scan(&lines.join("\n")).0, expected);
    }
}

#[test]
fn malformed_marker_after_read_served_preserves_violation_and_harness_error() {
    let mut source = good().replace(
        "post-expiry-unavailable",
        "post-expiry-read-served-violation",
    );
    source.push_str("\nrafter-maelstrom lease-isolation malformed");
    let (status, markers) = scan(&source);
    assert_eq!(status, LeaseArtifactStatus::ViolationWithHarnessError);
    assert!(markers["lease_sequence_invalid"] > 0);
}

#[test]
fn independent_history_parser_rejects_missing_and_swapped_probe_identity() {
    let completion = "{:index 2 :type :fail :process 0 :f :read :value nil :error [:temporarily-unavailable \"LeadershipLost [rafter-lease-probe client=c1 msg_id=11 code=11]\"]}";
    let exact = format!("{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{completion}");
    assert_eq!(
        history_completion_count(&exact, "c1", 11).expect("history parses"),
        1
    );
    assert_eq!(
        history_completion_count("", "c1", 11).expect("empty history parses"),
        0
    );
    assert_eq!(
        history_completion_count(&exact.replace("client=c1", "client=c2"), "c1", 11)
            .expect("swapped history parses"),
        0
    );
    assert!(history_completion_count(completion, "c1", 11).is_err());
    assert!(history_completion_count(
        "{:index 1 :type :invoke :process 0 :f :read :value nil}",
        "c1",
        11
    )
    .is_err());
    let swapped = format!(
        "{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{{:index 2 :type :invoke :process 1 :f :write :value 1}}\n{}",
        completion
            .replace(":index 2", ":index 3")
            .replace(":process 0", ":process 1")
    );
    assert!(history_completion_count(&swapped, "c1", 11).is_err());
    let mismatched_value = format!(
        "{{:index 1 :type :invoke :process 0 :f :read :value [0 nil]}}\n{}",
        completion.replace(":value nil", ":value [1 nil]")
    );
    assert!(history_completion_count(&mismatched_value, "c1", 11).is_err());
    let missing_value = format!(
        "{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{}",
        completion.replace(" :value nil", "")
    );
    assert!(history_completion_count(&missing_value, "c1", 11).is_err());
}

#[test]
fn lease_marker_parser_enforces_event_and_line_limits() {
    let mut markers = MARKERS.into_iter().map(|name| (name, 0)).collect();
    let mut events = Vec::new();
    let mut errors = 0;
    let count_error = scan_markers_with_limits(
        &good(),
        "n1",
        &mut markers,
        &mut events,
        &mut errors,
        MarkerLimits {
            events: 1,
            line_bytes: usize::MAX,
        },
    )
    .expect_err("marker inventory must be bounded");
    assert!(count_error.to_string().contains("marker count"));

    let mut markers = MARKERS.into_iter().map(|name| (name, 0)).collect();
    let line_error = scan_markers_with_limits(
        &good(),
        "n1",
        &mut markers,
        &mut Vec::new(),
        &mut 0,
        MarkerLimits {
            events: usize::MAX,
            line_bytes: 16,
        },
    )
    .expect_err("marker line must be bounded");
    assert!(line_error.to_string().contains("marker exceeds"));
}

#[test]
fn history_parser_enforces_operation_pending_and_line_limits() {
    let invoke = "{:index 1 :type :invoke :process 0 :f :read :value nil}";
    let second = "{:index 2 :type :invoke :process 1 :f :read :value nil}";
    let limits = |operations, pending, line_bytes| HistoryLimits {
        operations,
        pending,
        line_bytes,
    };
    assert!(history_completion_count_with_limits(
        &format!("{invoke}\n{second}"),
        "c1",
        11,
        limits(1, usize::MAX, usize::MAX),
    )
    .unwrap_err()
    .to_string()
    .contains("history exceeds 1 operations"));
    assert!(history_completion_count_with_limits(
        &format!("{invoke}\n{second}"),
        "c1",
        11,
        limits(usize::MAX, 1, usize::MAX),
    )
    .unwrap_err()
    .to_string()
    .contains("pending operations"));
    assert!(history_completion_count_with_limits(
        invoke,
        "c1",
        11,
        limits(usize::MAX, usize::MAX, 16),
    )
    .unwrap_err()
    .to_string()
    .contains("operation exceeds"));
}
