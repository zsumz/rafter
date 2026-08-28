//! Term, vote, and election-certificate history for one exploration state.
//!
//! Authority observation records every term floor and durable vote it sees, so
//! a regression or a same-term vote conflict is captured at the moment the
//! state is observed rather than inferred later from surviving state.

use std::{cmp::Ordering, collections::btree_map::Entry};

use crate::Cluster;

use super::super::super::observations::{Observation, ObservationSet};
use super::{
    ElectionCertificate, ElectionConflict, ElectionHistory, TermRegression, VoteConflict, VoteLoss,
};

impl ElectionHistory {
    pub(in crate::model_check) fn record_seeded_leaders(&mut self, cluster: &Cluster) {
        self.uncertified_seeded_leaders
            .extend(cluster.nodes.iter().filter_map(|(node_id, node)| {
                (node.role() == rafter::Role::Leader).then_some((*node_id, node.current_term()))
            }));
    }

    pub(in crate::model_check) fn observe_authority_state(
        &mut self,
        cluster: &Cluster,
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        for (node_id, node) in &cluster.nodes {
            let observed_term = node.current_term();
            match self.term_floor_by_node.entry(*node_id) {
                Entry::Vacant(entry) => {
                    entry.insert(observed_term);
                }
                Entry::Occupied(mut entry) => {
                    let floor = *entry.get();
                    match observed_term.cmp(&floor) {
                        Ordering::Less => {
                            self.term_regressions.insert(TermRegression {
                                node_id: *node_id,
                                previous_floor: floor,
                                observed: observed_term,
                            });
                        }
                        Ordering::Equal => {}
                        Ordering::Greater => {
                            entry.insert(observed_term);
                            observations.mark(Observation::TermAdvances);
                        }
                    }
                }
            }

            let vote_key = (*node_id, observed_term);
            if let Some(voted_for) = node.voted_for() {
                match self.votes_by_node_term.entry(vote_key) {
                    Entry::Vacant(entry) => {
                        entry.insert(voted_for);
                    }
                    Entry::Occupied(entry) if *entry.get() != voted_for => {
                        self.vote_conflicts.insert(VoteConflict {
                            node_id: *node_id,
                            term: observed_term,
                            first_vote: *entry.get(),
                            second_vote: voted_for,
                        });
                    }
                    Entry::Occupied(_) => {
                        observations.mark(Observation::SameTermVoteReobservations);
                    }
                }
            } else if let Some(previous_vote) = self.votes_by_node_term.get(&vote_key) {
                self.vote_losses.insert(VoteLoss {
                    node_id: *node_id,
                    term: observed_term,
                    previous_vote: *previous_vote,
                });
            }
        }
        observations
    }

    pub(in crate::model_check) fn record_election(&mut self, certificate: ElectionCertificate) {
        if let Some(previous) = self
            .elected_by_term
            .get(&certificate.term)
            .and_then(|certificates| certificates.first())
        {
            if previous.leader_id != certificate.leader_id {
                self.conflicting_elections.insert(ElectionConflict {
                    term: certificate.term,
                    first_leader: previous.leader_id,
                    second_leader: certificate.leader_id,
                });
            }
        }
        self.elected_by_term
            .entry(certificate.term)
            .or_default()
            .push(certificate);
    }
}
