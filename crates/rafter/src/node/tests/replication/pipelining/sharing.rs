//! Allocation sharing across one append fan-out round.

use super::support::*;
use super::*;

#[test]
fn fan_out_shares_the_log_payload_allocation_across_followers() {
    let mut leader = pipelining_leader(1, |config| config);
    seed_replicating(&mut leader, NodeId(2), LogIndex::ZERO);
    seed_replicating(&mut leader, NodeId(3), LogIndex::ZERO);

    let outputs = leader.step(Input::Tick);
    let sent_entries: Vec<crate::SharedEntries> = outputs
        .iter()
        .filter_map(|output| match output {
            Output::Send {
                message: Message::AppendEntries(AppendEntries { entries, .. }),
                ..
            } if !entries.is_empty() => Some(entries.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        sent_entries.len(),
        2,
        "both followers receive the suffix entry"
    );
    assert!(
        sent_entries[0].shares_allocation(&sent_entries[1]),
        "both follower batches share one append-entry slice"
    );

    let sent_payloads: Vec<crate::SharedPayload> = sent_entries
        .iter()
        .filter_map(|entries| {
            entries.first().map(|entry| match &entry.kind {
                crate::LogEntryKind::Application(payload) => payload.clone(),
                crate::LogEntryKind::Configuration(_) => {
                    panic!("test log holds application entries")
                }
                crate::LogEntryKind::Noop => panic!("test batch should not start with a no-op"),
            })
        })
        .collect();

    let log_payload = match &leader.log_entries_from(LogIndex(1))[0].kind {
        crate::LogEntryKind::Application(payload) => payload.clone(),
        crate::LogEntryKind::Configuration(_) => unreachable!("test log holds application entries"),
        crate::LogEntryKind::Noop => unreachable!("first entry is an application fixture"),
    };
    assert!(
        sent_payloads[0].shares_allocation(&log_payload),
        "the first follower's batch shares the log allocation"
    );
    assert!(
        sent_payloads[1].shares_allocation(&log_payload),
        "the second follower's batch shares the log allocation"
    );

    // Prior-term entries never commit by counting (thesis 3.6.2), so the
    // apply-sharing leg proposes a current-term entry and commits it.
    let _ = leader.step(Input::ClientProposal {
        payload: payload(2),
    });
    let proposed_log_payload = match &leader
        .log_entries_from(LogIndex(1))
        .last()
        .expect("client proposal is appended")
        .kind
    {
        crate::LogEntryKind::Application(payload) => payload.clone(),
        crate::LogEntryKind::Configuration(_) | crate::LogEntryKind::Noop => {
            unreachable!("proposal is an application entry")
        }
    };
    let applied = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(3),
            sequence: 0,
        }),
    });
    let apply_payload = applied
        .iter()
        .filter_map(|output| match output {
            Output::Apply { index, payload, .. } if *index == LogIndex(3) => Some(payload.clone()),
            _ => None,
        })
        .next_back()
        .expect("quorum acknowledgement commits and applies the proposed entry");
    assert!(
        apply_payload.shares_allocation(&proposed_log_payload),
        "the applied payload shares the log allocation"
    );
}
