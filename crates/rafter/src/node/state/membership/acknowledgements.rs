//! Quorum acknowledgements projected across effective membership changes.

use crate::{MembershipConfig, NodeId};

use super::index::MembershipIndex;
use super::slots::SlotSet;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct AcknowledgementSet {
    index: MembershipIndex,
    acks: SlotSet,
}

impl AcknowledgementSet {
    pub(in crate::node) fn new(membership: &MembershipConfig, self_id: NodeId) -> Self {
        let index = MembershipIndex::new(membership, self_id);
        let acks = SlotSet::empty(index.replica_ids().len());
        Self { index, acks }
    }

    pub(in crate::node) fn insert(
        &mut self,
        node_id: NodeId,
        membership: &MembershipConfig,
        self_id: NodeId,
    ) {
        self.refresh(membership, self_id);
        if let Some(slot) = self.index.slot(node_id) {
            self.acks.insert(slot);
        }
    }

    pub(in crate::node) fn has_quorum_with_self(
        &mut self,
        membership: &MembershipConfig,
        self_id: NodeId,
    ) -> bool {
        self.refresh(membership, self_id);
        self.index.has_quorum_slots_with_self(&self.acks)
    }

    pub(in crate::node) fn clear(&mut self) {
        self.acks = SlotSet::empty(self.index.replica_ids().len());
    }

    fn refresh(&mut self, membership: &MembershipConfig, self_id: NodeId) {
        if self.index.matches(membership, self_id) {
            return;
        }
        let next_index = MembershipIndex::new(membership, self_id);
        if self.index == next_index {
            return;
        }

        let mut next_acks = SlotSet::empty(next_index.replica_ids().len());
        for slot in self.acks.iter(self.index.replica_ids().len()) {
            let Some(node_id) = self.index.replica_ids().get(slot.index()).copied() else {
                continue;
            };
            if let Some(next_slot) = next_index.slot(node_id) {
                next_acks.insert(next_slot);
            }
        }
        self.index = next_index;
        self.acks = next_acks;
    }
}
