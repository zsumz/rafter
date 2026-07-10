use crate::{LogIndex, MembershipConfig, MembershipSet, NodeId};

use super::Progress;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ReplicaSlot(usize);

impl ReplicaSlot {
    const fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::node) struct SlotSet {
    words: SlotWords,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum SlotWords {
    Inline(u64),
    Heap(Vec<u64>),
}

impl Default for SlotSet {
    fn default() -> Self {
        Self {
            words: SlotWords::Inline(0),
        }
    }
}

impl SlotSet {
    pub(in crate::node) fn empty(slot_count: usize) -> Self {
        if slot_count <= u64::BITS as usize {
            Self::default()
        } else {
            Self {
                words: SlotWords::Heap(vec![0; word_count(slot_count)]),
            }
        }
    }

    fn insert(&mut self, slot: ReplicaSlot) {
        match &mut self.words {
            SlotWords::Inline(bits) if slot.index() < u64::BITS as usize => {
                *bits |= 1_u64 << slot.index();
            }
            SlotWords::Inline(bits) => {
                let mut words = vec![0; word_count(slot.index() + 1)];
                words[0] = *bits;
                words[word_index(slot)] |= slot_mask(slot);
                self.words = SlotWords::Heap(words);
            }
            SlotWords::Heap(words) => {
                let word_index = word_index(slot);
                if word_index >= words.len() {
                    words.resize(word_index + 1, 0);
                }
                words[word_index] |= slot_mask(slot);
            }
        }
    }

    fn contains(&self, slot: ReplicaSlot) -> bool {
        match &self.words {
            SlotWords::Inline(bits) => {
                slot.index() < u64::BITS as usize && (*bits & (1_u64 << slot.index())) != 0
            }
            SlotWords::Heap(words) => words
                .get(word_index(slot))
                .is_some_and(|word| (*word & slot_mask(slot)) != 0),
        }
    }

    pub(in crate::node) fn count(&self) -> usize {
        match &self.words {
            SlotWords::Inline(bits) => bits.count_ones() as usize,
            SlotWords::Heap(words) => words.iter().map(|word| word.count_ones() as usize).sum(),
        }
    }

    fn iter(&self, slot_count: usize) -> SlotSetIter<'_> {
        SlotSetIter {
            set: self,
            next_slot: 0,
            slot_count,
        }
    }
}

struct SlotSetIter<'a> {
    set: &'a SlotSet,
    next_slot: usize,
    slot_count: usize,
}

impl Iterator for SlotSetIter<'_> {
    type Item = ReplicaSlot;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_slot < self.slot_count {
            let slot = ReplicaSlot(self.next_slot);
            self.next_slot += 1;
            if self.set.contains(slot) {
                return Some(slot);
            }
        }
        None
    }
}

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

    const fn self_slot(&self) -> Option<ReplicaSlot> {
        self.self_slot
    }

    fn slot(&self, node_id: NodeId) -> Option<ReplicaSlot> {
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

    fn has_quorum_slots_with_self(&self, acknowledgements: &SlotSet) -> bool {
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

    fn matches(&self, membership: &MembershipConfig, self_id: NodeId) -> bool {
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
                (Some(ReplicaSlot(slot)) != self_slot)
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

fn word_count(bit_count: usize) -> usize {
    bit_count.saturating_add(u64::BITS as usize - 1) / u64::BITS as usize
}

fn word_index(slot: ReplicaSlot) -> usize {
    slot.index() / u64::BITS as usize
}

fn slot_mask(slot: ReplicaSlot) -> u64 {
    1_u64 << (slot.index() % u64::BITS as usize)
}

fn slot_of(replicas: &[NodeId], node_id: NodeId) -> Option<ReplicaSlot> {
    replicas.binary_search(&node_id).ok().map(ReplicaSlot)
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
        set.contains(ReplicaSlot(slot)) == members.binary_search(&node_id).is_ok()
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(index.self_slot(), Some(ReplicaSlot(1)));
        assert!(index.old_voters().contains(ReplicaSlot(0)));
        assert!(index.old_voters().contains(ReplicaSlot(1)));
        assert!(index.old_voters().contains(ReplicaSlot(2)));
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
        let new =
            MembershipConfig::joint(membership(&[1, 2, 3], &[]), membership(&[2, 3, 4], &[5]));
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
        set.insert(ReplicaSlot(69));

        assert!(set.contains(ReplicaSlot(69)));
        assert!(!set.contains(ReplicaSlot(68)));
        assert_eq!(set.count(), 1);
    }
}
