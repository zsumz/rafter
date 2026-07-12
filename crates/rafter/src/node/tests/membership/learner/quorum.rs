//! Learner and removed-voter exclusion from commit, read, and lease quorums.

use super::super::support::*;

#[test]
fn learner_acknowledgement_cannot_satisfy_read_index_quorum_but_voter_can() {
    let mut leader = committed_leader_with_learner_config();
    assert!(leader.is_effective_learner(NodeId(4)));

    let broadcasts = leader.step(Input::ReadIndex { read_id: ReadId(6) });
    let learner_round = append_entries_to(&broadcasts, NodeId(4))
        .expect("read-index round reaches learner")
        .sequence;
    let voter_round = append_entries_to(&broadcasts, NodeId(2))
        .expect("read-index round reaches voter")
        .sequence;
    let acknowledged_index = leader.last_log_index();

    let learner_outputs =
        acknowledge_round(&mut leader, NodeId(4), acknowledged_index, learner_round);
    assert!(learner_outputs
        .iter()
        .all(|output| !matches!(output, Output::ReadIndexGranted { .. })));
    assert_eq!(leader.pending_read_count(), 1);

    let voter_outputs = acknowledge_round(&mut leader, NodeId(2), acknowledged_index, voter_round);
    assert_eq!(
        voter_outputs,
        vec![Output::ReadIndexGranted {
            read_id: ReadId(6),
            read_index: leader.commit_index(),
        }]
    );
    assert_eq!(leader.pending_read_count(), 0);
}

#[test]
fn learner_acknowledgement_cannot_confirm_read_lease_but_voter_can() {
    let mut leader = committed_leader_with_learner_config_and_lease_reads();
    assert!(leader.is_effective_learner(NodeId(4)));
    assert!(!leader.read_lease_active());

    let broadcasts = leader.step(Input::Tick);
    let learner_round = append_entries_to(&broadcasts, NodeId(4))
        .expect("lease round reaches learner")
        .sequence;
    let voter_round = append_entries_to(&broadcasts, NodeId(2))
        .expect("lease round reaches voter")
        .sequence;

    let learner_match = leader.last_log_index();
    let _ = acknowledge_round(&mut leader, NodeId(4), learner_match, learner_round);
    assert!(!leader.read_lease_active());

    let voter_match = leader.last_log_index();
    let _ = acknowledge_round(&mut leader, NodeId(2), voter_match, voter_round);
    assert!(leader.read_lease_active());
}

#[test]
fn removed_voter_acknowledgement_cannot_satisfy_commit_read_index_or_lease_quorum() {
    let mut leader = committed_leader_after_removing_voter_with_lease_reads();
    let committed_membership = MembershipConfig::stable(membership(&[1, 2, 3]));
    assert_eq!(leader.committed_membership(), committed_membership);
    assert_eq!(leader.effective_membership(), committed_membership);
    assert!(!leader.is_effective_voter(NodeId(4)));
    assert!(!leader.read_lease_active());

    let _ = leader.step(Input::ClientProposal {
        payload: b"current-voters-only".to_vec(),
    });
    assert_eq!(leader.commit_index(), LogIndex(2));
    assert_eq!(leader.last_log_index(), LogIndex(3));

    let read_index = leader.commit_index();
    let broadcasts = leader.step(Input::ReadIndex { read_id: ReadId(7) });
    assert!(append_entries_to(&broadcasts, NodeId(4)).is_none());
    let round = append_entries_to(&broadcasts, NodeId(2))
        .expect("read-index round reaches current voter")
        .sequence;

    let removed_outputs = acknowledge_round(&mut leader, NodeId(4), LogIndex(3), round);
    assert!(removed_outputs
        .iter()
        .all(|output| !matches!(output, Output::ReadIndexGranted { .. })));
    assert_eq!(leader.commit_index(), LogIndex(2));
    assert_eq!(leader.pending_read_count(), 1);
    assert!(!leader.read_lease_active());

    let voter_outputs = acknowledge_round(&mut leader, NodeId(2), LogIndex(3), round);
    assert_eq!(leader.commit_index(), LogIndex(3));
    assert_eq!(leader.pending_read_count(), 0);
    assert!(leader.read_lease_active());
    assert!(voter_outputs.iter().any(|output| matches!(
        output,
        Output::ReadIndexGranted {
            read_id: ReadId(7),
            read_index: granted_index,
        } if *granted_index == read_index
    )));
    assert!(voter_outputs.iter().any(|output| matches!(
        output,
        Output::Apply {
            index: LogIndex(3),
            payload,
            ..
        } if payload == b"current-voters-only"
    )));
}
