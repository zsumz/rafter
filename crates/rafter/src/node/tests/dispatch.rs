//! Batched input boundaries and load-bearing output ordering.

use super::helpers::{elect_leader, node};
use super::*;

#[test]
fn step_batch_preserves_empty_and_single_step_semantics() {
    let mut empty = node(1, &[2, 3]);
    assert!(empty.step_batch(Vec::new()).is_empty());

    let mut batched = node(1, &[2, 3]);
    let mut stepped = batched.clone();

    assert_eq!(
        batched.step_batch(vec![Input::Tick]),
        stepped.step(Input::Tick)
    );
    assert_eq!(batched, stepped);
}

#[test]
fn step_batch_groups_consecutive_read_barriers_into_one_confirmation_round() {
    let mut leader = leader_with_current_term_commit();

    let outputs = leader.step_batch(vec![read_index(1), read_index(2), read_index(3)]);

    assert_eq!(
        leader.pending_read_count(),
        3,
        "pending-read observability still counts barriers, not rounds"
    );
    let rounds = heartbeat_rounds_to(&outputs, NodeId(2));
    assert_eq!(
        rounds.len(),
        1,
        "one read batch owns one quorum-confirming heartbeat round per follower"
    );

    let outputs = acknowledge_round(&mut leader, NodeId(2), rounds[0]);
    assert_eq!(
        granted_reads(&outputs),
        vec![
            (ReadId(1), LogIndex(1)),
            (ReadId(2), LogIndex(1)),
            (ReadId(3), LogIndex(1)),
        ],
        "grouped read barriers grant in registration order"
    );
    assert_eq!(leader.pending_read_count(), 0);
}

#[test]
fn step_batch_coalesces_consecutive_proposals_into_one_window_fill() {
    let mut leader = replicated_leader();

    let inputs = (0..64)
        .map(|index| Input::ClientProposal {
            payload: vec![u8::try_from(index).expect("test index fits byte"); 512],
        })
        .collect();
    let outputs = leader.step_batch(inputs);

    assert_eq!(leader.last_log_index(), LogIndex(65));
    for follower_id in [NodeId(2), NodeId(3)] {
        let batches = append_entries_batches_to(&outputs, follower_id);
        assert_eq!(
            batches.len(),
            1,
            "one proposal batch fills {follower_id}'s window once"
        );
        assert_eq!(batches[0].prev_log_index, LogIndex(1));
        assert_eq!(batches[0].entries.len(), 64);
        assert_eq!(
            batches[0]
                .entries
                .iter()
                .filter_map(LogEntry::application_payload)
                .count(),
            64
        );
    }
}

#[test]
fn step_batch_flushes_proposals_before_and_after_a_read_barrier() {
    let mut leader = replicated_leader();
    assert_eq!(leader.commit_index(), LogIndex(1));

    let outputs = leader.step_batch(vec![
        Input::ClientProposal {
            payload: b"before-read".to_vec(),
        },
        Input::ReadIndex { read_id: ReadId(9) },
        Input::ClientProposal {
            payload: b"after-read".to_vec(),
        },
    ]);

    assert_eq!(leader.last_log_index(), LogIndex(3));
    assert_eq!(leader.pending_read_count(), 1);

    let batches = append_entries_batches_to(&outputs, NodeId(2));
    assert_eq!(
        batches.len(),
        3,
        "each batch kind retains its own ordered replication round"
    );
    assert_eq!(
        application_payloads(batches[0]),
        vec![b"before-read".as_slice()]
    );
    assert!(
        batches[1].entries.is_empty(),
        "the read barrier owns its quorum-confirming heartbeat round"
    );
    assert_eq!(
        application_payloads(batches[2]),
        vec![b"after-read".as_slice()]
    );
    assert!(batches[0].sequence < batches[1].sequence);
    assert!(batches[1].sequence < batches[2].sequence);
}

#[test]
fn proposal_batch_is_rejected_in_order_during_leadership_transfer() {
    let mut leader = leader_with_acknowledged_follower();
    let before = leader.last_log_index();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });

    let outputs = leader.step_batch(vec![
        Input::TrackedClientProposal {
            proposal_id: LocalProposalId(20),
            payload: b"first-blocked".to_vec(),
        },
        Input::TrackedClientProposal {
            proposal_id: LocalProposalId(21),
            payload: b"second-blocked".to_vec(),
        },
    ]);

    assert_eq!(leader.last_log_index(), before);
    assert!(leader.volatile.local_proposals.is_empty());
    assert_eq!(
        outputs,
        vec![
            Output::RejectProposal {
                proposal_id: Some(LocalProposalId(20)),
                reason: ProposalRejection::LeadershipTransferInProgress { target: NodeId(2) },
            },
            Output::RejectProposal {
                proposal_id: Some(LocalProposalId(21)),
                reason: ProposalRejection::LeadershipTransferInProgress { target: NodeId(2) },
            },
        ]
    );
}

fn replicated_leader() -> Node {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    acknowledge_append(&mut leader, NodeId(2), LogIndex(1));
    acknowledge_append(&mut leader, NodeId(3), LogIndex(1));
    leader
}

fn leader_with_current_term_commit() -> Node {
    let leader = replicated_leader();
    assert_eq!(leader.commit_index(), LogIndex(1));
    leader
}

fn leader_with_acknowledged_follower() -> Node {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    acknowledge_append(&mut leader, NodeId(2), LogIndex(1));
    leader
}

fn acknowledge_append(leader: &mut Node, follower_id: NodeId, match_index: LogIndex) {
    let _ = leader.step(Input::Message {
        from: follower_id,
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id,
            success: true,
            match_index,
        }),
    });
}

fn acknowledge_round(leader: &mut Node, follower_id: NodeId, sequence: u64) -> Vec<Output> {
    let match_index = leader.last_log_index();
    leader.step(Input::Message {
        from: follower_id,
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence,
            term: leader.current_term(),
            follower_id,
            success: true,
            match_index,
        }),
    })
}

fn read_index(id: u64) -> Input {
    Input::ReadIndex {
        read_id: ReadId(id),
    }
}

fn heartbeat_rounds_to(outputs: &[Output], follower_id: NodeId) -> Vec<u64> {
    append_entries_batches_to(outputs, follower_id)
        .into_iter()
        .map(|request| request.sequence)
        .collect()
}

fn granted_reads(outputs: &[Output]) -> Vec<(ReadId, LogIndex)> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::ReadIndexGranted {
                read_id,
                read_index,
            } => Some((*read_id, *read_index)),
            _ => None,
        })
        .collect()
}

fn append_entries_batches_to(outputs: &[Output], follower_id: NodeId) -> Vec<&AppendEntries> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Send {
                to,
                message: Message::AppendEntries(request),
            } if *to == follower_id => Some(request),
            _ => None,
        })
        .collect()
}

fn application_payloads(request: &AppendEntries) -> Vec<&[u8]> {
    request
        .entries
        .iter()
        .filter_map(LogEntry::application_payload)
        .collect()
}
