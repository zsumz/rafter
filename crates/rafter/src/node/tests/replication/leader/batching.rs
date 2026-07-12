//! `AppendEntries` byte budgets and oversized-entry progress guarantees.

use super::support::*;
use super::*;

#[test]
fn leader_batches_lagging_follower_suffix_by_replication_byte_budget() {
    let mut leader = node_with_max_append_entries_bytes(1, &[2, 3], 180);
    let _ = elect_leader(&mut leader);

    // While the leadership probe is unanswered, proposals reach follower 2
    // as empty heartbeats only: the suffix accumulates on the leader.
    for byte in [b'a', b'b', b'c'] {
        let outputs = leader.step(Input::ClientProposal {
            payload: vec![byte; 100],
        });
        let request = append_entries_to(&outputs, NodeId(2));
        assert!(
            request.entries.is_empty(),
            "an unanswered probe defers the suffix to the confirming acknowledgement"
        );
    }

    assert_eq!(leader.last_log_index(), LogIndex(4));

    // The probe acknowledgement confirms the leadership no-op and fills the
    // window with the whole application suffix at once: one budget-bounded
    // batch per message, since a 180-byte budget fits one 100-byte payload but
    // not two.
    let catch_up_outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
        }),
    });

    let batches = append_entries_batches_to(&catch_up_outputs, NodeId(2));
    assert_eq!(
        batches.len(),
        3,
        "the byte budget splits the three-entry application suffix into three batches"
    );
    for (offset, (batch, byte)) in batches.iter().zip([b'a', b'b', b'c']).enumerate() {
        assert_eq!(batch.prev_log_index, LogIndex(offset as u64 + 1));
        assert_eq!(batch.entries.len(), 1);
        assert_eq!(
            batch.entries[0].application_payload(),
            Some(&vec![byte; 100][..])
        );
        assert!(replication_bytes(batch) <= leader.config.max_append_entries_bytes());
    }
}
#[test]
fn oversized_entry_still_replicates_as_single_entry_batch() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    // An entry far beyond any batch budget enters the log (as it can via
    // splice from a leader with a larger budget, or via hydration).
    leader
        .persistent
        .log
        .push(LogEntry::application(Term(1), vec![0xab; 700 * 1024]));

    let batch = leader
        .log_batch_from_bounded(LogIndex(2), 512)
        .expect("oversized entry still forms one batch");
    assert_eq!(
        batch.entries.len(),
        1,
        "an oversized entry must ship alone, never stall the batch"
    );
    assert_eq!(batch.first_index, LogIndex(2));
    assert_eq!(batch.last_index, LogIndex(2));
    assert_eq!(
        batch.replication_bytes,
        batch.entries[0].replication_bytes()
    );

    let followup = leader.log_batch_from_bounded(LogIndex(3), 512);
    assert!(followup.is_none(), "no second entry exists");
}
#[test]
fn budget_bounds_batches_beyond_the_first_entry() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    for payload in [b"one".as_slice(), b"two", b"three"] {
        leader
            .persistent
            .log
            .push(LogEntry::application(Term(1), payload.to_vec()));
    }

    let first = leader
        .log_batch_from_bounded(LogIndex(2), 1)
        .expect("budget smaller than any entry ships one");
    assert_eq!(
        first.entries.len(),
        1,
        "budget smaller than any entry ships one"
    );

    let all = leader
        .log_batch_from_bounded(LogIndex(2), 512 * 1024)
        .expect("generous budget ships entries");
    assert_eq!(all.entries.len(), 3, "a generous budget ships every entry");
    assert_eq!(
        all.replication_bytes,
        all.entries
            .iter()
            .map(LogEntry::replication_bytes)
            .sum::<usize>()
    );
}
