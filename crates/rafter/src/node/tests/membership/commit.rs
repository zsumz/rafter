//! Stable and joint quorum commitment under membership changes.

use super::support::*;

#[test]
fn stable_configuration_entry_commits_with_stable_majority() {
    let mut leader = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        stable_configuration(ConfigurationId(5), &[1, 2, 3]),
    )]);

    assert_eq!(leader.commit_index(), LogIndex::ZERO);

    let outputs = acknowledge(&mut leader, NodeId(2), LogIndex(1));

    assert_eq!(leader.commit_index(), LogIndex(1));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
}

#[test]
fn prior_term_quorum_candidate_waits_for_current_term_entry() {
    let mut leader = leader_with_log(vec![BootstrapLogEntry::application(
        LogIndex(1),
        Term(1),
        b"prior-term".to_vec(),
    )]);
    assert_eq!(leader.last_log_index(), LogIndex(2));

    let outputs = acknowledge(&mut leader, NodeId(2), LogIndex(1));
    assert_eq!(leader.commit_index(), LogIndex::ZERO);
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    let outputs = acknowledge(&mut leader, NodeId(3), LogIndex(1));

    assert_eq!(
        leader.commit_index(),
        LogIndex::ZERO,
        "a quorum-threshold prior-term candidate is not enough to advance commit"
    );
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));

    let outputs = acknowledge(&mut leader, NodeId(2), LogIndex(2));
    assert_eq!(leader.commit_index(), LogIndex::ZERO);
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    let outputs = acknowledge(&mut leader, NodeId(3), LogIndex(2));

    assert_eq!(leader.commit_index(), LogIndex(2));
    assert_eq!(
        outputs.iter().find_map(|output| match output {
            Output::Apply { index, payload, .. } => Some((*index, payload.as_slice())),
            Output::LocalProposalAppended { .. }
            | Output::LocalProposalDropped { .. }
            | Output::Send { .. }
            | Output::ApplySnapshot { .. }
            | Output::SendSnapshotChunk { .. }
            | Output::StageSnapshotChunk { .. }
            | Output::RejectProposal { .. }
            | Output::LeadershipTransferRejected { .. }
            | Output::ReadIndexGranted { .. }
            | Output::ReadIndexRejected { .. }
            | Output::ReadIndexCanceled { .. } => None,
        }),
        Some((LogIndex(1), &b"prior-term"[..]))
    );
}

#[test]
fn joint_configuration_entry_rejects_old_only_and_new_only_majorities() {
    let mut old_only = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        joint_configuration(ConfigurationId(6)),
    )]);

    let outputs = acknowledge(&mut old_only, NodeId(2), LogIndex(1));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    assert_eq!(old_only.commit_index(), LogIndex::ZERO);

    let mut new_only = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        joint_configuration(ConfigurationId(6)),
    )]);

    let outputs = acknowledge(&mut new_only, NodeId(4), LogIndex(1));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    assert_eq!(new_only.commit_index(), LogIndex::ZERO);

    let mut joint_quorum = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        joint_configuration(ConfigurationId(6)),
    )]);

    let outputs = acknowledge(&mut joint_quorum, NodeId(3), LogIndex(1));

    assert_eq!(joint_quorum.commit_index(), LogIndex(1));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));

    let heartbeat_outputs = joint_quorum.step(Input::Tick);
    let heartbeat = append_entries_to(&heartbeat_outputs, NodeId(3)).expect("commit heartbeat");
    assert_eq!(heartbeat.leader_commit, LogIndex(1));
}

#[test]
fn joint_configuration_governs_application_suffix_commit() {
    let mut old_only = leader_with_log(vec![
        BootstrapLogEntry::configuration(
            LogIndex(1),
            Term(2),
            joint_configuration(ConfigurationId(7)),
        ),
        BootstrapLogEntry::application(LogIndex(2), Term(2), b"joint-governed".to_vec()),
    ]);

    let outputs = acknowledge(&mut old_only, NodeId(2), LogIndex(2));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    assert_eq!(old_only.commit_index(), LogIndex::ZERO);

    let mut new_only = leader_with_log(vec![
        BootstrapLogEntry::configuration(
            LogIndex(1),
            Term(2),
            joint_configuration(ConfigurationId(7)),
        ),
        BootstrapLogEntry::application(LogIndex(2), Term(2), b"joint-governed".to_vec()),
    ]);

    let outputs = acknowledge(&mut new_only, NodeId(4), LogIndex(2));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    assert_eq!(new_only.commit_index(), LogIndex::ZERO);

    let mut joint_quorum = leader_with_log(vec![
        BootstrapLogEntry::configuration(
            LogIndex(1),
            Term(2),
            joint_configuration(ConfigurationId(7)),
        ),
        BootstrapLogEntry::application(LogIndex(2), Term(2), b"joint-governed".to_vec()),
    ]);

    let outputs = acknowledge(&mut joint_quorum, NodeId(3), LogIndex(2));

    assert_eq!(joint_quorum.commit_index(), LogIndex(2));
    assert_eq!(
        outputs.iter().find_map(|output| match output {
            Output::Apply { index, payload, .. } => Some((*index, payload.as_slice())),
            Output::LocalProposalAppended { .. }
            | Output::LocalProposalDropped { .. }
            | Output::Send { .. }
            | Output::ApplySnapshot { .. }
            | Output::SendSnapshotChunk { .. }
            | Output::StageSnapshotChunk { .. }
            | Output::RejectProposal { .. }
            | Output::LeadershipTransferRejected { .. }
            | Output::ReadIndexGranted { .. }
            | Output::ReadIndexRejected { .. }
            | Output::ReadIndexCanceled { .. } => None,
        }),
        Some((LogIndex(2), &b"joint-governed"[..]))
    );
}

#[test]
fn final_stable_configuration_commits_with_new_majority_after_joint_commit() {
    let mut leader = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        joint_configuration(ConfigurationId(8)),
    )]);
    leader.volatile.commit_index = LogIndex(1);
    leader.volatile.applied_index = LogIndex(1);
    let leave_outputs = leader.step(Input::LeaveJoint);

    assert_eq!(leader.last_log_index(), LogIndex(3));
    assert_eq!(
        leader
            .entry_at(LogIndex(3))
            .and_then(LogEntry::configuration_entry),
        Some(&stable_configuration(ConfigurationId(9), &[1, 3, 4]))
    );
    assert!(!leave_outputs
        .iter()
        .any(|output| matches!(output, Output::RejectProposal { .. })));

    assert!(acknowledge(&mut leader, NodeId(2), LogIndex(3)).is_empty());
    assert_eq!(leader.commit_index(), LogIndex(1));

    let outputs = acknowledge(&mut leader, NodeId(4), LogIndex(3));

    assert_eq!(leader.commit_index(), LogIndex(3));
    assert!(outputs.is_empty());

    let heartbeat_outputs = leader.step(Input::Tick);
    let heartbeat = append_entries_to(&heartbeat_outputs, NodeId(3)).expect("commit heartbeat");
    assert_eq!(heartbeat.leader_commit, LogIndex(3));
}

#[test]
fn grow_then_lose_originals() {
    let mut node = node_with_configuration(
        1,
        &[2, 3],
        stable_configuration(ConfigurationId(3), &[1, 2, 3, 4, 5]),
    );

    assert!(node.step(Input::Tick).is_empty());
    assert!(node.step(Input::Tick).is_empty());
    let outputs = node.step(Input::Tick);

    assert_eq!(node.role(), Role::PreCandidate);
    assert_eq!(
        send_targets(&outputs),
        vec![NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
    );
    assert!(outputs.iter().all(|output| matches!(
        output,
        Output::Send {
            message: Message::PreVote(_),
            ..
        }
    )));

    assert!(grant_pre_vote(&mut node, NodeId(4)).is_empty());
    assert_eq!(node.role(), Role::PreCandidate);
    let outputs = grant_pre_vote(&mut node, NodeId(5));

    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(
        send_targets(&outputs),
        vec![NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
    );
    assert!(outputs.iter().all(|output| matches!(
        output,
        Output::Send {
            message: Message::RequestVote(_),
            ..
        }
    )));

    assert!(grant_vote(&mut node, NodeId(4)).is_empty());
    assert_eq!(node.role(), Role::Candidate);

    let heartbeats = grant_vote(&mut node, NodeId(5));

    assert_eq!(node.role(), Role::Leader);
    assert_eq!(
        send_targets(&heartbeats),
        vec![NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
    );
}
