//! Stable lease transcript ordering, correlation, and history-binding scenarios.

use std::{collections::BTreeMap, time::Duration};

use super::{
    bind_lease_history, finish_lease_transcript, trial_process_timeout, validate_lease_transcript,
    LeaseMarker, LeaseTranscriptStatus, ScenarioMarkers,
};

fn good() -> Vec<LeaseMarker> {
    [
        "seq=1 node=n1 term=3 phase=fast-path-read-ok client=c0 msg_id=7",
        "seq=2 node=n1 term=3 phase=lease-expired client=c0 msg_id=7",
        "seq=3 node=n1 term=3 phase=read-buffered client=c1 msg_id=11",
        "seq=4 node=n1 term=3 phase=post-expiry-released client=c1 msg_id=11",
        "seq=5 node=n1 term=3 phase=post-expiry-handler client=c1 msg_id=11",
        "seq=6 node=n1 term=3 phase=post-expiry-unavailable client=c1 msg_id=11",
    ]
    .into_iter()
    .map(|fields| {
        LeaseMarker::parse(&format!("rafter-maelstrom lease-isolation {fields}"), "n1")
            .expect("fixture parses")
    })
    .collect()
}

#[test]
fn trial_timeout_is_bound_to_workload_duration_with_teardown_time() {
    let configuration = BTreeMap::from([("duration_seconds".to_owned(), "45".to_owned())]);
    assert_eq!(
        trial_process_timeout(&configuration).expect("valid trial timeout"),
        Duration::from_secs(45 + 2 * 60)
    );
    assert!(trial_process_timeout(&BTreeMap::from([(
        "duration_seconds".to_owned(),
        "0".to_owned()
    )]))
    .is_err());
    assert!(trial_process_timeout(&BTreeMap::from([(
        "duration_seconds".to_owned(),
        "not-a-duration".to_owned()
    )]))
    .is_err());
    assert!(trial_process_timeout(&BTreeMap::new()).is_err());
}

#[test]
fn accepts_only_expiry_before_the_correlated_buffered_read() {
    assert_eq!(
        validate_lease_transcript(&good()),
        Ok(LeaseTranscriptStatus::Complete)
    );
    let mut buffered_first = good();
    buffered_first.swap(1, 2);
    buffered_first[1].seq = 2;
    buffered_first[2].seq = 3;
    assert!(validate_lease_transcript(&buffered_first).is_err());
}

#[test]
fn rejects_cross_node_cross_term_and_uncorrelated_sequences() {
    let mut cross_node = good();
    cross_node[3].node = "n2".to_owned();
    assert!(validate_lease_transcript(&cross_node).is_err());
    let mut cross_term = good();
    cross_term[4].term = 4;
    assert!(validate_lease_transcript(&cross_term).is_err());
    let mut uncorrelated = good();
    uncorrelated[5].msg_id = 99;
    assert!(validate_lease_transcript(&uncorrelated).is_err());
}

#[test]
fn rejects_out_of_order_missing_and_duplicate_events() {
    let mut out_of_order = good();
    out_of_order.swap(3, 4);
    out_of_order[3].seq = 4;
    out_of_order[4].seq = 5;
    assert!(validate_lease_transcript(&out_of_order).is_err());
    assert_eq!(
        validate_lease_transcript(&good()[..5]),
        Ok(LeaseTranscriptStatus::Incomplete)
    );
    let mut duplicate = good();
    duplicate.insert(2, duplicate[1].clone());
    for (index, event) in duplicate.iter_mut().enumerate() {
        event.seq = (index + 1) as u64;
    }
    assert!(validate_lease_transcript(&duplicate).is_err());
}

#[test]
fn classifies_read_ok_and_renewal_as_violations_only() {
    let mut served = good();
    served[5].phase = "post-expiry-read-served-violation".to_owned();
    assert_eq!(
        validate_lease_transcript(&served),
        Ok(LeaseTranscriptStatus::Violation)
    );
    let mut renewed = good();
    renewed.truncate(2);
    renewed.push(LeaseMarker::parse(
        "rafter-maelstrom lease-isolation seq=3 node=n1 term=3 phase=post-expiry-renewed-violation client=c0 msg_id=7",
        "n1",
    ).expect("fixture parses"));
    assert_eq!(
        validate_lease_transcript(&renewed),
        Ok(LeaseTranscriptStatus::Violation)
    );
}

#[test]
fn unexpected_error_is_harness_error_and_malformed_fields_fail_closed() {
    let mut unexpected = good();
    unexpected[5] = LeaseMarker::parse(
        "rafter-maelstrom lease-isolation seq=6 node=n1 term=3 phase=post-expiry-unexpected-error client=c1 msg_id=11 code=20",
        "n1",
    ).expect("fixture parses");
    assert_eq!(
        validate_lease_transcript(&unexpected),
        Ok(LeaseTranscriptStatus::HarnessError)
    );
    assert!(LeaseMarker::parse(
        "rafter-maelstrom lease-isolation seq=1 node=n1 term=3 phase=fast-path-read-ok client=c0 msg_id=7 extra=x",
        "n1",
    ).is_err());
}

#[test]
fn duplicate_terminal_marker_fails_closed_after_either_terminal_kind() {
    for terminal in [
        "post-expiry-unavailable",
        "post-expiry-read-served-violation",
    ] {
        let mut events = good();
        events[5].phase = terminal.to_owned();
        events.push(LeaseMarker::parse(
            "rafter-maelstrom lease-isolation seq=7 node=n1 term=3 phase=post-expiry-duplicate-terminal client=c1 msg_id=11",
            "n1",
        ).expect("fixture parses"));
        let expected = if terminal == "post-expiry-read-served-violation" {
            LeaseTranscriptStatus::ViolationWithHarnessError
        } else {
            LeaseTranscriptStatus::HarnessError
        };
        assert_eq!(validate_lease_transcript(&events), Ok(expected));

        let mut duplicate_before_handler = good();
        duplicate_before_handler[5].phase = terminal.to_owned();
        duplicate_before_handler.insert(4, LeaseMarker::parse(
            "rafter-maelstrom lease-isolation seq=5 node=n1 term=3 phase=post-expiry-duplicate-terminal client=c1 msg_id=11",
            "n1",
        ).expect("fixture parses"));
        for (index, event) in duplicate_before_handler.iter_mut().enumerate() {
            event.seq = (index + 1) as u64;
        }
        assert_eq!(
            validate_lease_transcript(&duplicate_before_handler),
            Ok(expected)
        );
    }
}

#[test]
fn malformed_marker_after_read_served_preserves_violation_and_harness_error() {
    let mut events = good();
    events[5].phase = "post-expiry-read-served-violation".to_owned();
    let mut markers = ScenarioMarkers::default();
    finish_lease_transcript(&mut markers, &events, 1);
    assert_eq!(
        markers.lease_status,
        LeaseTranscriptStatus::ViolationWithHarnessError
    );
    assert_eq!(markers.lease_sequence_invalid, 2);
}

#[test]
fn retained_history_must_match_the_exact_probe_identity_once() {
    let events = good();
    let exact = concat!(
        "{:index 1 :type :invoke :process 0 :f :read :value nil}\n",
        "{:index 2 :type :fail :process 0 :f :read :value nil :error ",
        "[:temporarily-unavailable \"LeadershipLost [rafter-lease-probe client=c1 msg_id=11 code=11]\"]}"
    );
    let swapped = exact.replace("client=c1", "client=c2");

    let mut matched = ScenarioMarkers {
        lease_status: LeaseTranscriptStatus::Complete,
        lease_sequence_complete: 1,
        ..ScenarioMarkers::default()
    };
    bind_lease_history(&mut matched, &events, Some(exact));
    assert_eq!(matched.lease_history_probe_matches, 1);
    assert_eq!(matched.lease_status, LeaseTranscriptStatus::Complete);

    for history in [None, Some(swapped.as_str())] {
        let mut rejected = ScenarioMarkers {
            lease_status: LeaseTranscriptStatus::Complete,
            lease_sequence_complete: 1,
            ..ScenarioMarkers::default()
        };
        bind_lease_history(&mut rejected, &events, history);
        assert_eq!(rejected.lease_history_probe_mismatches, 1);
        assert_eq!(rejected.lease_status, LeaseTranscriptStatus::HarnessError);
    }
}
