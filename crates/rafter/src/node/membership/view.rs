//! Membership views and quorum interpretation.
//!
//! Raft uses several distinct membership views: static startup membership,
//! currently effective membership, committed membership, and membership
//! captured at the snapshot boundary. This module names those views explicitly
//! while `ConfigurationIndex` owns the derived retained-log lookup
//! structure.

use crate::{
    CommittedConfiguration, ConfigurationEntry, LogIndex, MembershipConfig, NodeId,
    PromotionBarrier,
};

use super::super::state::MembershipIndex;
use super::super::{Node, Role};

impl Node {
    /// Returns the currently effective membership.
    #[must_use]
    pub fn effective_membership(&self) -> MembershipConfig {
        self.effective_configuration_entry().map_or_else(
            || self.effective_base_membership_ref().clone(),
            |entry| entry.membership_config(),
        )
    }

    pub(super) fn effective_base_membership_ref(&self) -> &MembershipConfig {
        self.persistent
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.metadata.committed_membership())
            .unwrap_or_else(|| self.config.static_membership_ref())
    }

    /// Membership committed at `index`, as recoverable from the retained log,
    /// the previous snapshot's compacted state, or finally the static bootstrap
    /// configuration.
    ///
    /// If the derived configuration index is stale, invalid entries are ignored
    /// defensively. Normal log mutation keeps the index exact.
    #[must_use]
    pub fn membership_at_index(&self, index: LogIndex) -> MembershipConfig {
        let first_log_index = self.snapshot_index().next();
        self.derived
            .configuration
            .entry_at_or_before(first_log_index, &self.persistent.log, index)
            .map(ConfigurationEntry::membership_config)
            .or_else(|| {
                let snapshot_index = self.snapshot_index();
                (snapshot_index <= index)
                    .then(|| self.snapshot_committed_membership())
                    .flatten()
            })
            .unwrap_or_else(|| self.config.static_membership())
    }

    /// Returns `None` if the derived configuration index is stale.
    #[must_use]
    pub fn effective_configuration_entry(&self) -> Option<ConfigurationEntry> {
        self.derived
            .configuration
            .effective_entry(&self.persistent.log)
            .cloned()
    }

    /// Returns the committed membership.
    #[must_use]
    pub fn committed_membership(&self) -> MembershipConfig {
        self.committed_configuration_entry()
            .map(|entry| entry.membership_config())
            .or_else(|| self.snapshot_committed_membership())
            .unwrap_or_else(|| self.config.static_membership())
    }

    /// Returns committed membership captured in the installed snapshot.
    #[must_use]
    pub fn snapshot_committed_membership(&self) -> Option<MembershipConfig> {
        self.persistent
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.metadata.committed_membership().cloned())
    }

    /// Returns committed configuration identity captured in the installed
    /// snapshot.
    #[must_use]
    pub fn snapshot_committed_configuration_state(&self) -> Option<CommittedConfiguration> {
        self.persistent
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.metadata.committed_configuration_state())
    }

    /// Returns `None` if the derived configuration index is stale or no
    /// configuration entry is committed in the retained log.
    #[must_use]
    pub fn committed_configuration_entry(&self) -> Option<ConfigurationEntry> {
        self.derived
            .configuration
            .committed_entry(
                self.snapshot_index().next(),
                &self.persistent.log,
                self.volatile.commit_index,
            )
            .cloned()
    }

    /// Returns the committed configuration identity at the current commit
    /// index, if known.
    #[must_use]
    pub fn committed_configuration_state(&self) -> Option<CommittedConfiguration> {
        self.committed_configuration_state_at(self.volatile.commit_index)
    }

    /// Returns whether `node_id` is an effective voter.
    #[must_use]
    pub fn is_effective_voter(&self, node_id: NodeId) -> bool {
        self.effective_membership().contains_voter(node_id)
    }

    /// Returns whether `node_id` is an effective learner.
    #[must_use]
    pub fn is_effective_learner(&self, node_id: NodeId) -> bool {
        self.effective_membership().contains_learner(node_id)
    }

    /// Returns the current learner promotion barrier, if one can be issued.
    #[must_use]
    pub fn promotion_barrier(&self, learner_id: NodeId) -> Option<PromotionBarrier> {
        (self.role() == Role::Leader && self.is_effective_learner(learner_id))
            .then(|| PromotionBarrier::new(learner_id, self.commit_index()))
    }

    pub(in crate::node) fn has_effective_quorum<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = NodeId>,
    {
        if self.derived.configuration.is_empty() {
            return MembershipIndex::new(self.effective_base_membership_ref(), self.id())
                .has_quorum(acknowledgements);
        }
        MembershipIndex::new(&self.effective_membership(), self.id()).has_quorum(acknowledgements)
    }

    pub(in crate::node) fn uncommitted_configuration_indexes(&self) -> Vec<LogIndex> {
        self.derived
            .configuration
            .indexes_after(self.snapshot_index().next(), self.volatile.commit_index)
    }

    /// Returns the latest committed configuration identity recoverable at or
    /// below `commit_index`.
    #[must_use]
    pub fn committed_configuration_state_at(
        &self,
        commit_index: LogIndex,
    ) -> Option<CommittedConfiguration> {
        self.derived
            .configuration
            .committed_state_at(
                self.snapshot_index().next(),
                &self.persistent.log,
                commit_index,
            )
            .or_else(|| {
                self.persistent
                    .committed_configuration
                    .filter(|state| state.index <= commit_index)
            })
            .or_else(|| {
                self.snapshot_committed_configuration_state()
                    .filter(|state| state.index <= commit_index)
            })
    }
}
