//! Unit tests for membership slot indexes and projected progress.

use super::slots::ReplicaSlot;
use super::*;
use crate::{LogIndex, MembershipConfig, MembershipSet, NodeId};

fn membership(voters: &[u64], learners: &[u64]) -> MembershipSet {
    MembershipSet::new(
        voters.iter().copied().map(NodeId).collect(),
        learners.iter().copied().map(NodeId).collect(),
    )
    .expect("membership is valid")
}

#[test]
fn membership_index_preserves_sorted_replica_slots() {
    let membership = MembershipConfig::stable(membership(&[3, 1, 2], &[5, 4]));
    let index = MembershipIndex::new(&membership, NodeId(2));

    assert_eq!(
        index.replica_ids(),
        &[NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
    );
    assert_eq!(index.self_slot(), Some(ReplicaSlot::new(1)));
    assert!(index.old_voters().contains(ReplicaSlot::new(0)));
    assert!(index.old_voters().contains(ReplicaSlot::new(1)));
    assert!(index.old_voters().contains(ReplicaSlot::new(2)));
}

#[test]
fn membership_index_checks_stable_quorum_from_slots() {
    let membership = MembershipConfig::stable(membership(&[1, 2, 3], &[4]));
    let index = MembershipIndex::new(&membership, NodeId(1));

    assert!(index.has_quorum([NodeId(1), NodeId(2)]));
    assert!(!index.has_quorum([NodeId(1), NodeId(4)]));
}

#[test]
fn membership_index_matches_stable_membership_without_rebuild() {
    let stable = membership(&[1, 2, 3], &[4]);
    let index = MembershipIndex::new(&MembershipConfig::stable(stable.clone()), NodeId(2));

    assert!(index.matches(&MembershipConfig::stable(stable.clone()), NodeId(2)));
    assert!(!index.matches(&MembershipConfig::stable(stable.clone()), NodeId(1)));
    assert!(!index.matches(
        &MembershipConfig::stable(membership(&[1, 2, 3], &[4, 5])),
        NodeId(2),
    ));
    assert!(!index.matches(
        &MembershipConfig::joint(stable, membership(&[2, 3, 5], &[])),
        NodeId(2),
    ));
}

#[test]
fn membership_index_checks_joint_quorum_from_both_halves() {
    let old = membership(&[1, 2, 3], &[]);
    let new = membership(&[3, 4, 5], &[]);
    let membership = MembershipConfig::joint(old.clone(), new.clone());
    let index = MembershipIndex::new(&membership, NodeId(1));

    assert!(index.has_quorum([NodeId(1), NodeId(3), NodeId(4)]));
    assert!(!index.has_quorum([NodeId(1), NodeId(2)]));
    assert!(!index.has_quorum([NodeId(3), NodeId(4)]));
    assert!(index.new_voters().is_some());
}

#[test]
fn acknowledgement_set_drops_removed_slots_on_membership_refresh() {
    let old = MembershipConfig::stable(membership(&[1, 2], &[]));
    let new = MembershipConfig::stable(membership(&[1, 3, 4], &[]));
    let mut acks = AcknowledgementSet::new(&old, NodeId(1));
    acks.insert(NodeId(2), &old, NodeId(1));

    assert!(acks.has_quorum_with_self(&old, NodeId(1)));
    assert!(!acks.has_quorum_with_self(&new, NodeId(1)));
}

#[test]
fn acknowledgement_set_projects_retained_nodes_across_slot_changes() {
    let old = MembershipConfig::stable(membership(&[1, 4], &[]));
    let new = MembershipConfig::stable(membership(&[1, 4, 5], &[3]));
    let mut acks = AcknowledgementSet::new(&old, NodeId(1));
    acks.insert(NodeId(4), &old, NodeId(1));

    assert!(acks.has_quorum_with_self(&old, NodeId(1)));
    assert!(acks.has_quorum_with_self(&new, NodeId(1)));
}

#[test]
fn progress_set_rebuild_preserves_existing_replica_progress() {
    let old = MembershipConfig::stable(membership(&[1, 2, 3], &[]));
    let new = MembershipConfig::joint(membership(&[1, 2, 3], &[]), membership(&[2, 3, 4], &[5]));
    let mut progress = ProgressSet::default();
    progress.reset(&old, NodeId(1), LogIndex(7), LogIndex(10));
    progress
        .get_mut(NodeId(2))
        .expect("old voter has progress")
        .match_index = LogIndex(6);

    progress.rebuild(&new, NodeId(1), LogIndex(3), LogIndex(11));

    assert_eq!(
        progress.get(NodeId(2)).map(|p| p.match_index),
        Some(LogIndex(6))
    );
    assert_eq!(
        progress.get(NodeId(4)).map(|p| p.next_index),
        Some(LogIndex(3))
    );
    assert_eq!(
        progress.get(NodeId(5)).map(|p| p.next_index),
        Some(LogIndex(3))
    );
    assert_eq!(
        progress.get(NodeId(1)).map(|p| p.match_index),
        Some(LogIndex(11))
    );
    assert_eq!(
        progress.get(NodeId(1)).map(|p| p.next_index),
        Some(LogIndex(12))
    );
}

#[test]
fn progress_set_rebuild_refreshes_local_progress_when_membership_is_unchanged() {
    let membership = MembershipConfig::stable(membership(&[1, 2, 3], &[]));
    let mut progress = ProgressSet::default();
    progress.reset(&membership, NodeId(1), LogIndex(7), LogIndex(10));

    progress.rebuild(&membership, NodeId(1), LogIndex(3), LogIndex(12));

    assert_eq!(
        progress.get(NodeId(1)).map(|p| p.match_index),
        Some(LogIndex(12))
    );
    assert_eq!(
        progress.get(NodeId(1)).map(|p| p.next_index),
        Some(LogIndex(13))
    );
}

#[test]
fn slot_set_uses_heap_words_beyond_inline_capacity() {
    let mut set = SlotSet::empty(70);
    set.insert(ReplicaSlot::new(69));

    assert!(set.contains(ReplicaSlot::new(69)));
    assert!(!set.contains(ReplicaSlot::new(68)));
    assert_eq!(set.count(), 1);
}
