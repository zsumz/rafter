//! Bounded pending read state and overload rejection.

use super::support::*;

#[test]
fn pending_reads_are_capped() {
    let mut leader = leader_with_current_term_commit();
    for request_id in 0..1024 {
        let outputs = leader.step(read_index(request_id));
        assert!(granted(&outputs).is_empty());
    }
    let outputs = leader.step(read_index(9999));
    assert!(matches!(
        outputs.as_slice(),
        [Output::ReadIndexRejected {
            read_id: ReadId(9999),
            reason: ReadIndexRejection::TooManyPendingReads,
        }]
    ));
}

#[test]
fn pending_read_cap_counts_grouped_read_ids() {
    let mut leader = leader_with_current_term_commit();
    let inputs = (0..1027).map(read_index).collect();

    let outputs = leader.step_batch(inputs);

    assert_eq!(leader.pending_read_count(), 1024);
    let first_rejection = outputs
        .iter()
        .position(|output| matches!(output, Output::ReadIndexRejected { .. }))
        .expect("suffix read barriers are rejected");
    assert!(
        outputs[..first_rejection]
            .iter()
            .any(|output| matches!(output, Output::Send { .. })),
        "accepted-prefix heartbeat effects precede rejected-suffix annotations"
    );
    assert_eq!(
        outputs[first_rejection..],
        [
            Output::ReadIndexRejected {
                read_id: ReadId(1024),
                reason: ReadIndexRejection::TooManyPendingReads,
            },
            Output::ReadIndexRejected {
                read_id: ReadId(1025),
                reason: ReadIndexRejection::TooManyPendingReads,
            },
            Output::ReadIndexRejected {
                read_id: ReadId(1026),
                reason: ReadIndexRejection::TooManyPendingReads,
            },
        ],
        "rejected suffix preserves input order"
    );
    let rounds = heartbeat_rounds_to(&outputs, NodeId(2));
    assert_eq!(
        rounds.len(),
        1,
        "the accepted prefix shares one confirmation round"
    );
}
