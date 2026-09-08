//! Frozen predecessor and commit knowledge when a local proposal adds a configuration.

use rafter::{LogIndex, NodeId, Term};

use super::super::ExplorationState;
use crate::{model_check::observations::Observation, Cluster};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ConfigurationProposalWitness {
    pub(crate) proposer: NodeId,
    pub(crate) term: Term,
    pub(crate) index: LogIndex,
    pub(crate) predecessor: Option<LogIndex>,
    pub(crate) commit_before: LogIndex,
}

impl ExplorationState {
    /// Called only for a local membership operation, after its effects but
    /// before any later transition. Replication and bootstrap are not proposals.
    pub(in crate::model_check) fn record_configuration_proposal(
        &mut self,
        before: &Cluster,
        proposer: NodeId,
    ) {
        let first = before.last_log_index(proposer).next();
        if self.cluster.last_log_index(proposer) < first {
            return;
        }
        let bootstrap = before.bootstrap_state(proposer);
        let mut predecessor = bootstrap
            .log
            .iter()
            .rev()
            .find(|entry| entry.kind.is_configuration())
            .map(|entry| entry.index)
            .or_else(|| {
                bootstrap.snapshot.as_ref().and_then(|snapshot| {
                    snapshot
                        .metadata
                        .committed_configuration_state()
                        .map(|state| state.index)
                })
            });
        for (offset, entry) in self
            .cluster
            .log_entries_from(proposer, first)
            .iter()
            .enumerate()
        {
            if !entry.kind.is_configuration() {
                continue;
            }
            let index = LogIndex(first.0 + offset as u64);
            self.commit_history
                .configuration_proposals
                .insert(ConfigurationProposalWitness {
                    proposer,
                    term: entry.term,
                    index,
                    predecessor,
                    commit_before: bootstrap.commit_index,
                });
            if predecessor.is_some() {
                self.mark_observation(Observation::ConfigurationProposalPredecessorChecks);
            }
            predecessor = Some(index);
        }
    }
}
