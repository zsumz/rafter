//! Volatile local proposal correlation ordered by log index.
//!
//! Proposal IDs are local-only metadata. This tracker follows the natural log
//! order without making replicated protocol state depend on correlation data.

use std::collections::{vec_deque, VecDeque};

use crate::{LocalProposalId, LogIndex, Term};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::node) struct LocalProposal {
    pub term: Term,
    pub id: LocalProposalId,
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct LocalProposalTracker {
    proposals: VecDeque<(LogIndex, LocalProposal)>,
}

impl LocalProposalTracker {
    pub(in crate::node) fn insert(&mut self, index: LogIndex, proposal: LocalProposal) {
        match self.proposals.back_mut() {
            None => {
                self.proposals.push_back((index, proposal));
            }
            Some((last_index, last_proposal)) if *last_index == index => {
                *last_proposal = proposal;
            }
            Some((last_index, _)) if *last_index < index => {
                self.proposals.push_back((index, proposal));
            }
            Some(_) => self.insert_out_of_order(index, proposal),
        }
    }

    fn insert_out_of_order(&mut self, index: LogIndex, proposal: LocalProposal) {
        let position = self
            .proposals
            .iter()
            .position(|(existing_index, _)| *existing_index >= index);
        match position {
            Some(position) if self.proposals[position].0 == index => {
                self.proposals[position].1 = proposal;
            }
            Some(position) => {
                self.proposals.insert(position, (index, proposal));
            }
            None => self.proposals.push_back((index, proposal)),
        }
    }

    pub(in crate::node) fn remove(&mut self, index: LogIndex) -> Option<LocalProposal> {
        if self
            .proposals
            .front()
            .is_some_and(|(current, _)| *current == index)
        {
            return self.proposals.pop_front().map(|(_, proposal)| proposal);
        }
        let position = self
            .proposals
            .iter()
            .position(|(current, _)| *current == index)?;
        self.proposals
            .remove(position)
            .map(|(_, proposal)| proposal)
    }

    pub(in crate::node) fn split_off(&mut self, index: LogIndex) -> Self {
        let position = self
            .proposals
            .iter()
            .position(|(proposal_index, _)| *proposal_index >= index)
            .unwrap_or(self.proposals.len());
        Self {
            proposals: self.proposals.split_off(position),
        }
    }

    #[cfg(test)]
    pub(in crate::node) fn contains_key(&self, index: LogIndex) -> bool {
        self.proposals
            .iter()
            .any(|(proposal_index, _)| *proposal_index == index)
    }

    #[cfg(test)]
    pub(in crate::node) fn keys(&self) -> impl Iterator<Item = &LogIndex> {
        self.proposals.iter().map(|(index, _)| index)
    }

    #[cfg(test)]
    pub(in crate::node) fn is_empty(&self) -> bool {
        self.proposals.is_empty()
    }
}

impl IntoIterator for LocalProposalTracker {
    type Item = (LogIndex, LocalProposal);
    type IntoIter = vec_deque::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.proposals.into_iter()
    }
}
