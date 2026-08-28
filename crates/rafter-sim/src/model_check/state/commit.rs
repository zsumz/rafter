use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
};

use rafter::{LogIndex, MembershipConfig, NodeId, Role, Term};

use super::logical_log::LogPrefixWitness;
use super::ExplorationState;

mod certificate;
mod prefix;

#[derive(Clone, Debug, Default)]
pub(crate) struct CommitHistory {
    pub(crate) certificates: BTreeMap<(NodeId, Term, LogIndex), CommitCertificate>,
    pub(crate) committed_prefix: Option<LogPrefixWitness>,
    committed_prefix_owner: Option<NodeId>,
    pub(crate) committed_in_terms: Vec<Term>,
    pub(crate) unwitnessed_committed_prefixes: BTreeSet<(NodeId, LogIndex)>,
    pub(crate) unwitnessed_commit_terms: BTreeSet<LogIndex>,
    pub(crate) leader_completeness_checked_through: BTreeMap<(NodeId, Term, usize), LogIndex>,
    pub(crate) violations: BTreeSet<CommitHistoryViolation>,
}

/// One configuration entry appended by the transition currently being
/// observed. Commits below `index` still use the frozen pre-transition
/// membership; commits at or above it use the post-append membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check) struct ConfigurationAppend {
    pub(in crate::model_check) proposer: NodeId,
    pub(in crate::model_check) index: LogIndex,
}

impl Hash for CommitHistory {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.certificates.hash(state);
        self.committed_prefix.hash(state);
        self.committed_prefix_owner.hash(state);
        self.committed_in_terms.hash(state);
        self.unwitnessed_committed_prefixes.hash(state);
        self.unwitnessed_commit_terms.hash(state);
        self.leader_completeness_checked_through.hash(state);
        self.violations.hash(state);
    }
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct CommitTransitionContext {
    pub(crate) node_id: NodeId,
    pub(crate) term: Term,
    pub(crate) role: Role,
    pub(crate) effective_membership: MembershipConfig,
    pub(crate) old_commit: LogIndex,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct CommitCertificate {
    pub(crate) leader_id: NodeId,
    pub(crate) leader_term: Term,
    pub(crate) committed_through: LogIndex,
    pub(crate) candidate_term: Term,
    pub(crate) membership: MembershipConfig,
    pub(crate) stored_by: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CommitHistoryViolation {
    pub(crate) invariant: &'static str,
    pub(crate) message: String,
}

impl ExplorationState {
    pub(in crate::model_check) fn commit_transition_context(
        &self,
    ) -> BTreeMap<NodeId, CommitTransitionContext> {
        self.cluster
            .nodes
            .iter()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    CommitTransitionContext {
                        node_id: *node_id,
                        term: node.current_term(),
                        role: node.role(),
                        effective_membership: node.effective_membership(),
                        old_commit: node.commit_index(),
                    },
                )
            })
            .collect()
    }

    pub(in crate::model_check) fn record_commit_observation(
        &mut self,
        before: &BTreeMap<NodeId, CommitTransitionContext>,
        configuration_append: Option<ConfigurationAppend>,
        follower_commit_authority: Option<(NodeId, Term)>,
    ) {
        let observations = self.commit_history.record_commit_transitions(
            before,
            &self.cluster,
            configuration_append,
            follower_commit_authority,
            &self.logical_log_history,
        );
        self.observations.union_with(observations);
        self.refresh_committed_prefixes();
    }

    pub(in crate::model_check) fn refresh_committed_prefixes(&mut self) {
        let observations = self
            .commit_history
            .observe_committed_prefixes(&self.cluster, &self.logical_log_history);
        self.observations.union_with(observations);
    }

    pub(in crate::model_check) fn refresh_seeded_commit_history(&mut self) {
        self.commit_history
            .record_seeded_commit_terms(&self.cluster, &self.logical_log_history);
        let _ = self
            .commit_history
            .observe_committed_prefixes(&self.cluster, &self.logical_log_history);
    }

    pub(in crate::model_check) fn witness_seeded_commit_authority(
        &mut self,
        old_commit: LogIndex,
        new_commit: LogIndex,
        term: Term,
    ) {
        self.commit_history
            .record_seeded_commit_authority(old_commit, new_commit, term);
    }

    pub(in crate::model_check) fn record_leader_completeness_observation(&mut self) {
        let observations = self
            .commit_history
            .record_leader_completeness(&self.election_history);
        self.observations.union_with(observations);
    }
}
