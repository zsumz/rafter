//! Replication progress aligned with effective membership slots.

use crate::{LogIndex, MembershipConfig, NodeId};

use super::super::Progress;
use super::index::MembershipIndex;
use super::slots::{ReplicaSlot, SlotSet};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct ProgressSet {
    index: MembershipIndex,
    progress: Vec<Progress>,
}

impl ProgressSet {
    pub(in crate::node) fn reset(
        &mut self,
        membership: &MembershipConfig,
        self_id: NodeId,
        follower_next_index: LogIndex,
        local_match_index: LogIndex,
    ) {
        self.index = MembershipIndex::new(membership, self_id);
        self.progress = self
            .index
            .replica_ids()
            .iter()
            .copied()
            .map(|replica| {
                if replica == self_id {
                    Progress::local(local_match_index)
                } else {
                    Progress::probing(follower_next_index)
                }
            })
            .collect();
    }

    pub(in crate::node) fn rebuild(
        &mut self,
        membership: &MembershipConfig,
        self_id: NodeId,
        first_sendable_index: LogIndex,
        local_match_index: LogIndex,
    ) {
        if self.index.matches(membership, self_id)
            && self.progress.len() == self.index.replica_ids().len()
        {
            self.refresh_local_progress(self_id, local_match_index);
            return;
        }
        let next_index = MembershipIndex::new(membership, self_id);
        if self.index == next_index && self.progress.len() == next_index.replica_ids().len() {
            self.refresh_local_progress(self_id, local_match_index);
            return;
        }

        let old_index = self.index.clone();
        let old_progress = std::mem::take(&mut self.progress);
        self.progress = next_index
            .replica_ids()
            .iter()
            .copied()
            .map(|replica| {
                let progress = old_index
                    .slot(replica)
                    .and_then(|slot| old_progress.get(slot.index()).cloned())
                    .unwrap_or_else(|| {
                        if replica == self_id {
                            Progress::local(local_match_index)
                        } else {
                            Progress::probing(first_sendable_index)
                        }
                    });
                let mut progress = progress;
                if replica == self_id {
                    Self::apply_local_progress_floor(&mut progress, local_match_index);
                }
                progress
            })
            .collect();
        self.index = next_index;
    }

    fn refresh_local_progress(&mut self, self_id: NodeId, local_match_index: LogIndex) {
        let Some(slot) = self.index.self_slot().filter(|slot| {
            self.index
                .replica_ids()
                .get(slot.index())
                .is_some_and(|node_id| *node_id == self_id)
        }) else {
            return;
        };
        if let Some(progress) = self.progress.get_mut(slot.index()) {
            Self::apply_local_progress_floor(progress, local_match_index);
        }
    }

    fn apply_local_progress_floor(progress: &mut Progress, local_match_index: LogIndex) {
        progress.match_index = progress.match_index.max(local_match_index);
        progress.next_index = progress.next_index.max(local_match_index.next());
    }

    pub(in crate::node) const fn index(&self) -> &MembershipIndex {
        &self.index
    }

    pub(in crate::node) fn contains(&self, node_id: NodeId) -> bool {
        self.index.slot(node_id).is_some()
    }

    pub(in crate::node) fn get(&self, node_id: NodeId) -> Option<&Progress> {
        self.index
            .slot(node_id)
            .and_then(|slot| self.progress.get(slot.index()))
    }

    pub(in crate::node) fn get_mut(&mut self, node_id: NodeId) -> Option<&mut Progress> {
        self.index
            .slot(node_id)
            .and_then(|slot| self.progress.get_mut(slot.index()))
    }

    pub(in crate::node) fn iter_followers(&self) -> impl Iterator<Item = (NodeId, &Progress)> + '_ {
        let self_slot = self.index.self_slot();
        self.index
            .replica_ids()
            .iter()
            .copied()
            .enumerate()
            .filter_map(move |(slot, node_id)| {
                (Some(ReplicaSlot::new(slot)) != self_slot)
                    .then(|| self.progress.get(slot).map(|progress| (node_id, progress)))
                    .flatten()
            })
    }

    pub(in crate::node) fn replica_count(&self) -> usize {
        self.index.replica_ids().len()
    }

    pub(in crate::node) fn replica_id_at(&self, slot: usize) -> Option<NodeId> {
        self.index.replica_ids().get(slot).copied()
    }

    pub(in crate::node) fn match_indexes_for(&self, voters: &SlotSet) -> Vec<LogIndex> {
        voters
            .iter(self.progress.len())
            .filter_map(|slot| self.progress.get(slot.index()))
            .map(|progress| progress.match_index)
            .collect()
    }
}
