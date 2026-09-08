//! Serialization of configuration changes in the replicated log.

use super::support::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

#[test]
fn local_configuration_proposal_waits_for_predecessor_commit() {
    let mut leader = committed_leader_with_learner_config();
    let _ = leader.step(Input::AddLearner {
        learner_id: NodeId(5),
    });
    let pending = leader.last_log_index();
    oracle_assert!(pending > leader.commit_index());
    let rejected = leader.step(Input::AddLearner {
        learner_id: NodeId(6),
    });
    oracle_assert_eq!(leader.last_log_index(), pending);
    oracle_assert!(rejected.iter().any(|output| matches!(output,
        Output::RejectProposal {
            reason: ProposalRejection::Configuration(
                ConfigurationProposalRejection::UncommittedConfiguration { index }
            ), ..
        } if *index == pending
    )));
    let _ = acknowledge(&mut leader, NodeId(2), pending);
    oracle_assert_eq!(leader.commit_index(), pending);
    let _ = leader.step(Input::AddLearner {
        learner_id: NodeId(6),
    });
    oracle_assert_eq!(leader.last_log_index(), pending.next());
}

#[test]
fn follower_rejects_second_uncommitted_configuration_entry() {
    let mut follower = node(2, &[1, 3, 4]);
    let joint = joint_configuration(ConfigurationId(9));
    let final_stable = stable_configuration(ConfigurationId(10), &[1, 3, 4]);

    assert_append_entries_response(
        &follower.step(Input::Message {
            from: NodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: vec![LogEntry::configuration(Term(2), joint)].into(),
                leader_commit: LogIndex::ZERO,
            }),
        }),
        NodeId(1),
        true,
        LogIndex(1),
    );

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(2),
            entries: vec![LogEntry::configuration(Term(2), final_stable)].into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    oracle_assert_eq!(follower.last_log_index(), LogIndex(1));
    oracle_assert!(append_response_matches(
        &outputs,
        NodeId(1),
        false,
        LogIndex::ZERO
    ));
}

fn append_response_matches(
    outputs: &[Output],
    to: NodeId,
    success: bool,
    match_index: LogIndex,
) -> bool {
    matches!(
        outputs,
        [Output::Send {
            to: actual_to,
            message: Message::AppendEntriesResponse(response),
        }] if *actual_to == to
            && response.success == success
            && response.match_index == match_index
    )
}

#[test]
fn follower_accepts_next_configuration_when_frame_commits_previous() {
    let mut follower = node(2, &[1, 3, 4]);
    let joint = joint_configuration(ConfigurationId(9));
    let final_stable = stable_configuration(ConfigurationId(10), &[1, 3, 4]);

    assert_append_entries_response(
        &follower.step(Input::Message {
            from: NodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: vec![LogEntry::configuration(Term(2), joint.clone())].into(),
                leader_commit: LogIndex::ZERO,
            }),
        }),
        NodeId(1),
        true,
        LogIndex(1),
    );

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 1,
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(2),
            entries: vec![LogEntry::configuration(Term(2), final_stable.clone())].into(),
            leader_commit: LogIndex(1),
        }),
    });

    assert_eq!(follower.commit_index(), LogIndex(1));
    assert_eq!(follower.last_log_index(), LogIndex(2));
    assert_eq!(follower.committed_configuration_entry(), Some(joint));
    assert_eq!(follower.effective_configuration_entry(), Some(final_stable));
    assert_append_entries_response(&outputs, NodeId(1), true, LogIndex(2));
}
