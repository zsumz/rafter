//! Safe stable and joint membership transition derivation.

use super::support::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq, oracle_violation};

#[test]
fn change_membership_derives_joint_configuration_for_voter_changes() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    let target = membership(&[1, 3]);

    let outputs = leader.step(Input::ChangeMembership {
        target: target.clone(),
        promotion_barriers: Vec::new(),
    });

    oracle_assert_eq!(leader.last_log_index(), LogIndex(2));
    let Some(ConfigurationEntry::Joint {
        config_id,
        membership: joint,
    }) = leader
        .entry_at(LogIndex(2))
        .and_then(LogEntry::configuration_entry)
    else {
        oracle_violation!("expected derived joint configuration entry");
    };
    oracle_assert_eq!(*config_id, ConfigurationId(1));
    oracle_assert_eq!(joint.old(), &membership(&[1, 2, 3]));
    oracle_assert_eq!(joint.new_membership(), &target);
    oracle_assert!(!outputs
        .iter()
        .any(|output| matches!(output, Output::RejectProposal { .. })));
}

#[test]
fn leave_joint_derives_stable_configuration_from_joint_new_side() {
    let joint = joint_configuration(ConfigurationId(7));
    let mut leader = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        joint,
    )]);
    leader.volatile.commit_index = LogIndex(1);
    leader.volatile.applied_index = LogIndex(1);

    let outputs = leader.step(Input::LeaveJoint);

    assert_eq!(leader.last_log_index(), LogIndex(3));
    let Some(ConfigurationEntry::Stable {
        config_id,
        membership: stable,
    }) = leader
        .entry_at(LogIndex(3))
        .and_then(LogEntry::configuration_entry)
    else {
        panic!("expected derived stable configuration entry");
    };
    assert_eq!(*config_id, ConfigurationId(8));
    assert_eq!(stable, &membership(&[1, 3, 4]));
    assert!(!outputs
        .iter()
        .any(|output| matches!(output, Output::RejectProposal { .. })));
}
