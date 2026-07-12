//! Compact sets over the sorted replica slots in one membership index.

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct ReplicaSlot(usize);

impl ReplicaSlot {
    pub(super) const fn new(index: usize) -> Self {
        Self(index)
    }

    pub(super) const fn index(self) -> usize {
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

    pub(super) fn insert(&mut self, slot: ReplicaSlot) {
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

    pub(super) fn contains(&self, slot: ReplicaSlot) -> bool {
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

    pub(super) fn iter(&self, slot_count: usize) -> SlotSetIter<'_> {
        SlotSetIter {
            set: self,
            next_slot: 0,
            slot_count,
        }
    }
}

pub(super) struct SlotSetIter<'a> {
    set: &'a SlotSet,
    next_slot: usize,
    slot_count: usize,
}

impl Iterator for SlotSetIter<'_> {
    type Item = ReplicaSlot;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_slot < self.slot_count {
            let slot = ReplicaSlot::new(self.next_slot);
            self.next_slot += 1;
            if self.set.contains(slot) {
                return Some(slot);
            }
        }
        None
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
