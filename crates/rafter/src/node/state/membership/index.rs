//! Stable and joint membership represented over sorted replica slots.

use crate::{MembershipConfig, MembershipSet, NodeId};

use super::slots::{ReplicaSlot, SlotSet};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct MembershipIndex {
    replicas: Vec<NodeId>,
    old_voters: SlotSet,
    new_voters: Option<SlotSet>,
    learners: SlotSet,
    self_slot: Option<ReplicaSlot>,
}

impl MembershipIndex {
    pub(in crate::node) fn new(membership: &MembershipConfig, self_id: NodeId) -> Self {
        match membership {
            MembershipConfig::Stable(stable) => Self::stable(stable, self_id),
            MembershipConfig::Joint(joint) => {
                let replicas = membership.replica_ids();
                let old_voters = slot_set_for(&replicas, joint.old().voters());
                let new_voters = slot_set_for(&replicas, joint.new_membership().voters());
                let learners = learners_for_joint(&replicas, joint.old(), joint.new_membership());
                let self_slot = slot_of(&replicas, self_id);
                Self {
                    replicas,
                    old_voters,
                    new_voters: Some(new_voters),
                    learners,
                    self_slot,
                }
            }
        }
    }

    fn stable(membership: &MembershipSet, self_id: NodeId) -> Self {
        let replicas = membership.replica_ids();
        let old_voters = slot_set_for(&replicas, membership.voters());
        let learners = slot_set_for(&replicas, membership.learners());
        let self_slot = slot_of(&replicas, self_id);
        Self {
            replicas,
            old_voters,
            new_voters: None,
            learners,
            self_slot,
        }
    }

    pub(in crate::node) fn replica_ids(&self) -> &[NodeId] {
        &self.replicas
    }

    pub(in crate::node) const fn old_voters(&self) -> &SlotSet {
        &self.old_voters
    }

    pub(in crate::node) const fn new_voters(&self) -> Option<&SlotSet> {
        self.new_voters.as_ref()
    }

    pub(super) const fn self_slot(&self) -> Option<ReplicaSlot> {
        self.self_slot
    }

    pub(super) fn slot(&self, node_id: NodeId) -> Option<ReplicaSlot> {
        slot_of(&self.replicas, node_id)
    }

    pub(in crate::node) fn has_quorum<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = NodeId>,
    {
        let mut ack_slots = SlotSet::empty(self.replicas.len());
        for node_id in acknowledgements {
            if let Some(slot) = self.slot(node_id) {
                ack_slots.insert(slot);
            }
        }
        self.has_quorum_slots(&ack_slots)
    }

    fn has_quorum_slots(&self, acknowledgements: &SlotSet) -> bool {
        quorum_reached(&self.old_voters, acknowledgements, self.replicas.len())
            && self
                .new_voters
                .as_ref()
                .is_none_or(|voters| quorum_reached(voters, acknowledgements, self.replicas.len()))
    }

    pub(super) fn has_quorum_slots_with_self(&self, acknowledgements: &SlotSet) -> bool {
        quorum_reached_with_self(
            &self.old_voters,
            acknowledgements,
            self.self_slot,
            self.replicas.len(),
        ) && self.new_voters.as_ref().is_none_or(|voters| {
            quorum_reached_with_self(
                voters,
                acknowledgements,
                self.self_slot,
                self.replicas.len(),
            )
        })
    }

    pub(super) fn matches(&self, membership: &MembershipConfig, self_id: NodeId) -> bool {
        match membership {
            MembershipConfig::Stable(stable) => self.matches_stable(stable, self_id),
            // Joint configurations are less common and more subtle; rebuild
            // them explicitly rather than hiding a four-way set comparison in
            // the hot stable-membership fast path.
            MembershipConfig::Joint(_) => false,
        }
    }

    fn matches_stable(&self, membership: &MembershipSet, self_id: NodeId) -> bool {
        self.new_voters.is_none()
            && self.self_slot == slot_of(&self.replicas, self_id)
            && sorted_union_matches(&self.replicas, membership.voters(), membership.learners())
            && slot_set_matches_members(&self.old_voters, &self.replicas, membership.voters())
            && slot_set_matches_members(&self.learners, &self.replicas, membership.learners())
    }
}

fn slot_of(replicas: &[NodeId], node_id: NodeId) -> Option<ReplicaSlot> {
    replicas.binary_search(&node_id).ok().map(ReplicaSlot::new)
}

fn slot_set_for(replicas: &[NodeId], nodes: &[NodeId]) -> SlotSet {
    let mut set = SlotSet::empty(replicas.len());
    for node_id in nodes {
        if let Some(slot) = slot_of(replicas, *node_id) {
            set.insert(slot);
        }
    }
    set
}

fn learners_for_joint(
    replicas: &[NodeId],
    old_membership: &MembershipSet,
    new_membership: &MembershipSet,
) -> SlotSet {
    let mut set = SlotSet::empty(replicas.len());
    for learner in old_membership
        .learners()
        .iter()
        .chain(new_membership.learners())
    {
        if let Some(slot) = slot_of(replicas, *learner) {
            set.insert(slot);
        }
    }
    set
}

fn sorted_union_matches(target: &[NodeId], left: &[NodeId], right: &[NodeId]) -> bool {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut target_index = 0;

    while left_index < left.len() || right_index < right.len() {
        let next = match (left.get(left_index), right.get(right_index)) {
            (Some(left_id), Some(right_id)) if left_id <= right_id => {
                left_index += 1;
                *left_id
            }
            (Some(left_id), None) => {
                left_index += 1;
                *left_id
            }
            (Some(_) | None, Some(right_id)) => {
                right_index += 1;
                *right_id
            }
            (None, None) => break,
        };

        if target.get(target_index).copied() != Some(next) {
            return false;
        }
        target_index += 1;
    }

    target_index == target.len()
}

fn slot_set_matches_members(set: &SlotSet, replicas: &[NodeId], members: &[NodeId]) -> bool {
    replicas.iter().copied().enumerate().all(|(slot, node_id)| {
        set.contains(ReplicaSlot::new(slot)) == members.binary_search(&node_id).is_ok()
    })
}

fn quorum_reached(voters: &SlotSet, acknowledgements: &SlotSet, slot_count: usize) -> bool {
    let quorum_size = majority(voters.count());
    voters
        .iter(slot_count)
        .filter(|slot| acknowledgements.contains(*slot))
        .count()
        >= quorum_size
}

fn quorum_reached_with_self(
    voters: &SlotSet,
    acknowledgements: &SlotSet,
    self_slot: Option<ReplicaSlot>,
    slot_count: usize,
) -> bool {
    let quorum_size = majority(voters.count());
    voters
        .iter(slot_count)
        .filter(|slot| acknowledgements.contains(*slot) || Some(*slot) == self_slot)
        .count()
        >= quorum_size
}

fn majority(voters: usize) -> usize {
    (voters / 2) + 1
}
