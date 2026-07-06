#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
#[allow(clippy::too_many_lines)]
fn three_node_proposal_appends_then_completes_after_quorum_ack() {
    let mut group = group(1, &[2, 3]);
    let report = group.step(GroupInput::Tick).expect("pre-vote starts");
    let pre_vote_term = report
        .peer_messages
        .iter()
        .find_map(|envelope| match &envelope.message {
            Message::PreVote(request) => Some(request.term),
            _ => None,
        })
        .expect("pre-vote request is emitted");

    let report = group
        .step(GroupInput::PeerMessage {
            envelope: PeerEnvelope {
                group_id: 7,
                from: NodeId(2),
                to: NodeId(1),
                message: Message::PreVoteResponse(PreVoteResponse {
                    term: pre_vote_term,
                    voter_id: NodeId(2),
                    vote_granted: true,
                }),
            },
        })
        .expect("pre-vote grant starts real election");
    let vote_term = report
        .peer_messages
        .iter()
        .find_map(|envelope| match &envelope.message {
            Message::RequestVote(request) => Some(request.term),
            _ => None,
        })
        .expect("request vote is emitted");

    let _ = group
        .step(GroupInput::PeerMessage {
            envelope: PeerEnvelope {
                group_id: 7,
                from: NodeId(2),
                to: NodeId(1),
                message: Message::RequestVoteResponse(RequestVoteResponse {
                    term: vote_term,
                    voter_id: NodeId(2),
                    vote_granted: true,
                }),
            },
        })
        .expect("vote grant elects leader");
    let leader_metrics = group.metrics();
    assert_eq!(leader_metrics.role, Role::Leader);
    assert_eq!(leader_metrics.replication.len(), 2);
    let mut followers = leader_metrics
        .replication
        .iter()
        .map(|progress| progress.follower_id)
        .collect::<Vec<_>>();
    followers.sort();
    assert_eq!(followers, vec![NodeId(2), NodeId(3)]);

    let proposal_id = LocalProposalId(2);
    let begin = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"two".to_vec(),
        })
        .expect("leader appends proposal");
    let peer_messages = match begin {
        ProposalBegin::Appended {
            local_proposal_id,
            peer_messages,
            ..
        } if local_proposal_id == proposal_id => peer_messages,
        other => panic!("expected appended proposal, got {other:?}"),
    };
    assert!(!peer_messages.is_empty());
    assert!(group.state_machine().applied.is_empty());
    assert_eq!(group.metrics().pending_proposals, 1);
    let sequence = peer_messages
        .iter()
        .find_map(|envelope| match &envelope.message {
            Message::AppendEntries(AppendEntries { sequence, .. }) => Some(*sequence),
            _ => None,
        })
        .expect("proposal append entries are returned");

    let report = group
        .step(GroupInput::PeerMessage {
            envelope: PeerEnvelope {
                group_id: 7,
                from: NodeId(2),
                to: NodeId(1),
                message: Message::AppendEntriesResponse(AppendEntriesResponse {
                    term: vote_term,
                    follower_id: NodeId(2),
                    success: true,
                    match_index: LogIndex(2),
                    sequence,
                }),
            },
        })
        .expect("quorum ack applies proposal");

    assert!(report.proposal_events.iter().any(|event| matches!(
        event,
        ProposalEvent::Applied {
            local_proposal_id,
            result,
            ..
        } if *local_proposal_id == proposal_id && result == b"two"
    )));
    assert_eq!(group.state_machine().applied, vec![b"two".to_vec()]);
    assert_eq!(group.metrics().pending_proposals, 0);
    assert_eq!(group.metrics().applied_index, LogIndex(2));
}
