//! Read barrier registration, round sequencing, and ordered confirmation.

use super::support::*;

#[test]
fn read_index_broadcasts_confirmation_round_immediately() {
    let mut leader = leader_with_current_term_commit();

    let heartbeats = leader.step(read_index(42));
    assert_eq!(leader.pending_read_count(), 1);
    let Output::Send {
        message: Message::AppendEntries(AppendEntries { sequence, .. }),
        ..
    } = &heartbeats[0]
    else {
        panic!("expected heartbeat");
    };
    let round = *sequence;

    let outputs = ack(&mut leader, 2, round);
    assert_eq!(granted(&outputs), vec![(ReadId(42), LogIndex(1))]);
    assert_eq!(leader.pending_read_count(), 0);
}

#[test]
fn changing_read_id_does_not_affect_quorum_behavior() {
    let mut first = leader_with_current_term_commit();
    let mut second = leader_with_current_term_commit();

    let first_round = heartbeat_round(&first.step(read_index(100)));
    let second_round = heartbeat_round(&second.step(read_index(200)));
    assert_eq!(first_round, second_round);
    assert_eq!(first.pending_read_count(), second.pending_read_count());

    let first_outputs = ack(&mut first, 2, first_round);
    let second_outputs = ack(&mut second, 2, second_round);

    assert_eq!(granted(&first_outputs), vec![(ReadId(100), LogIndex(1))]);
    assert_eq!(granted(&second_outputs), vec![(ReadId(200), LogIndex(1))]);
    assert_eq!(first.pending_read_count(), second.pending_read_count());
    assert_eq!(first.commit_index(), second.commit_index());
}

#[test]
fn delayed_ack_from_an_older_round_never_confirms_a_barrier() {
    let mut leader = leader_with_current_term_commit();

    // Observe the last pre-registration round from a heartbeat.
    let pre_round = heartbeat_round(&leader.step(Input::Tick));
    let post_round = heartbeat_round(&leader.step(read_index(9)));

    // Delayed echoes of pre-registration rounds must not count — even a
    // quorum of them proves nothing about leadership after registration.
    let outputs = ack(&mut leader, 2, pre_round);
    assert!(granted(&outputs).is_empty());
    let outputs = ack(&mut leader, 3, pre_round);
    assert!(granted(&outputs).is_empty());
    assert_eq!(leader.pending_read_count(), 1);

    // An echo of the eagerly broadcast post-registration round confirms.
    assert!(post_round > pre_round);
    let outputs = ack(&mut leader, 2, post_round);
    assert_eq!(granted(&outputs), vec![(ReadId(9), LogIndex(1))]);
}

#[test]
fn acknowledgement_observed_before_registration_cannot_confirm_later_read() {
    let mut leader = leader_with_current_term_commit();

    // Process quorum evidence while there is no read to confirm.
    let pre_registration_round = heartbeat_round(&leader.step(Input::Tick));
    let outputs = ack(&mut leader, 2, pre_registration_round);
    assert!(granted(&outputs).is_empty());
    assert_eq!(leader.pending_read_count(), 0);

    // Registering afterward must not inherit the already-observed acknowledgement.
    let registration_outputs = leader.step(read_index(10));
    let post_registration_round = heartbeat_round(&registration_outputs);
    assert!(granted(&registration_outputs).is_empty());
    assert_eq!(leader.pending_read_count(), 1);

    // Only a fresh quorum round observed after registration confirms the read.
    assert!(post_registration_round > pre_registration_round);
    let outputs = ack(&mut leader, 2, post_registration_round);
    assert_eq!(granted(&outputs), vec![(ReadId(10), LogIndex(1))]);
    assert_eq!(leader.pending_read_count(), 0);
}

#[test]
fn zero_sequence_echo_never_confirms_a_barrier() {
    let mut leader = leader_with_current_term_commit();
    let _ = leader.step(read_index(1));

    // A directly constructed or non-codec message with no round information
    // echoes zero.
    let outputs = ack(&mut leader, 2, 0);
    assert!(granted(&outputs).is_empty());
    assert_eq!(leader.pending_read_count(), 1);
}

#[test]
fn multiple_reads_grant_in_registration_order() {
    let mut leader = leader_with_current_term_commit();

    let _ = leader.step(read_index(1));
    // A second entry commits, then a second read registers at the higher index.
    let _ = leader.step(Input::ClientProposal {
        payload: b"second".to_vec(),
    });
    let _ = ack(&mut leader, 2, 0); // advances match/commit via match_index
    assert_eq!(leader.commit_index(), LogIndex(2));
    let round = heartbeat_round(&leader.step(read_index(2)));

    // One ack at the latest round confirms both barriers, in order.
    let outputs = ack(&mut leader, 2, round);
    assert_eq!(
        granted(&outputs),
        vec![(ReadId(1), LogIndex(1)), (ReadId(2), LogIndex(2))]
    );
}

#[test]
fn single_voter_grants_immediately() {
    let mut solo = Node::new(NodeConfig::new(NodeId(1), vec![], 3).expect("single voter config"));
    for _ in 0..3 {
        let _ = solo.step(Input::Tick);
    }
    assert_eq!(solo.role(), Role::Leader);
    assert_eq!(solo.commit_index(), LogIndex(1));

    let outputs = solo.step(read_index(11));
    assert_eq!(granted(&outputs), vec![(ReadId(11), LogIndex(1))]);
}
