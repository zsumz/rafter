//! Incomplete invocations may explain a read, but cannot erase real-time order.
use super::{
    check_client_history_linearizable, insert_completed_read, insert_completed_write,
    insert_unknown_write, payload, ClientHistory, ClientWriteStatus, LogIndex, NodeId, ProposalId,
};
use rafter::Term;
use rafter_invariant_test::oracle_assert;

fn histories() -> impl Iterator<Item = ClientHistory> {
    [
        ClientWriteStatus::Pending,
        ClientWriteStatus::Accepted {
            node_id: NodeId(1),
            index: LogIndex(1),
            term: Term(1),
        },
    ]
    .into_iter()
    .map(|status| {
        let mut history = ClientHistory::default();
        insert_unknown_write(&mut history, ProposalId(1), 0, b"unfinished");
        history.writes.get_mut(&ProposalId(1)).unwrap().status = status;
        history
    })
}

#[test]
fn unfinished_write_may_explain_an_observed_value_without_a_reply() {
    for mut history in histories() {
        insert_completed_read(
            &mut history,
            1,
            1,
            2,
            LogIndex(1),
            Some(payload(b"unfinished")),
        );
        oracle_assert!(
            check_client_history_linearizable(&history).is_ok(),
            "a reply is not the linearization point"
        );
    }
}

#[test]
fn unfinished_write_may_be_omitted_when_no_read_observes_it() {
    for mut history in histories() {
        insert_completed_read(&mut history, 1, 1, 2, LogIndex::ZERO, None);
        check_client_history_linearizable(&history)
            .expect("an incomplete invocation need not take effect");
    }
}

#[test]
fn unfinished_write_cannot_disappear_after_a_read_requires_it() {
    for mut history in histories() {
        insert_completed_read(
            &mut history,
            1,
            1,
            2,
            LogIndex(1),
            Some(payload(b"unfinished")),
        );
        insert_completed_read(&mut history, 2, 3, 4, LogIndex(1), None);
        assert!(check_client_history_linearizable(&history).is_err());
    }
}

#[test]
fn future_unfinished_invocation_cannot_explain_an_earlier_read() {
    for mut history in histories() {
        history.writes.get_mut(&ProposalId(1)).unwrap().started_at = 3;
        insert_completed_read(
            &mut history,
            1,
            1,
            2,
            LogIndex(1),
            Some(payload(b"unfinished")),
        );
        assert!(check_client_history_linearizable(&history).is_err());
    }
}

#[test]
fn unfinished_write_cannot_explain_a_different_payload() {
    for mut history in histories() {
        insert_completed_read(
            &mut history,
            1,
            1,
            2,
            LogIndex(1),
            Some(payload(b"invented")),
        );
        assert!(check_client_history_linearizable(&history).is_err());
    }
}

#[test]
fn unfinished_write_does_not_make_completed_writes_optional() {
    for mut history in histories() {
        insert_completed_write(&mut history, ProposalId(2), 1, 2, LogIndex(2), b"completed");
        insert_completed_read(&mut history, 1, 3, 4, LogIndex(2), None);
        assert!(check_client_history_linearizable(&history).is_err());
    }
}

#[test]
fn rejected_write_cannot_explain_an_observed_value() {
    let mut history = histories().next().unwrap();
    history.writes.get_mut(&ProposalId(1)).unwrap().status = ClientWriteStatus::Rejected;
    insert_completed_read(
        &mut history,
        1,
        1,
        2,
        LogIndex(1),
        Some(payload(b"unfinished")),
    );
    assert!(check_client_history_linearizable(&history).is_err());
}
