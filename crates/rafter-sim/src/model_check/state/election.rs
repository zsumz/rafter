//! Election history: term floors, durable votes, and leader certificates.
//!
//! Every term regression, same-term vote conflict, lost vote, unfenced
//! authority transition, and pre-vote violation is retained as evidence rather
//! than collapsed into a boolean, so a later check can say which node did what
//! in which term. A leader with no certificate is recorded as uncertified.

use std::collections::{BTreeMap, BTreeSet};

use rafter::{BootstrapState, LogIndex, MembershipConfig, NodeId, Term};

use super::logical_log::LogPrefixWitness;

mod authority;
mod history;
mod votes;

#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct ElectionHistory {
    pub(crate) transition_contexts_observed: u64,
    pub(crate) uncertified_seeded_leaders: BTreeSet<(NodeId, Term)>,
    pub(crate) term_floor_by_node: BTreeMap<NodeId, Term>,
    pub(crate) votes_by_node_term: BTreeMap<(NodeId, Term), NodeId>,
    pub(crate) term_regressions: BTreeSet<TermRegression>,
    pub(crate) vote_conflicts: BTreeSet<VoteConflict>,
    pub(crate) vote_losses: BTreeSet<VoteLoss>,
    pub(crate) vote_grants: Vec<VoteGrantObservation>,
    pub(crate) authority_transition_violations: Vec<AuthorityTransitionViolation>,
    pub(crate) pre_vote_violations: Vec<PreVoteViolation>,
    pub(crate) grants_by_candidate: BTreeMap<(Term, NodeId), BTreeSet<NodeId>>,
    pub(crate) elected_by_term: BTreeMap<Term, Vec<ElectionCertificate>>,
    pub(crate) conflicting_elections: BTreeSet<ElectionConflict>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TermRegression {
    pub(crate) node_id: NodeId,
    pub(crate) previous_floor: Term,
    pub(crate) observed: Term,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct VoteConflict {
    pub(crate) node_id: NodeId,
    pub(crate) term: Term,
    pub(crate) first_vote: NodeId,
    pub(crate) second_vote: NodeId,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct VoteLoss {
    pub(crate) node_id: NodeId,
    pub(crate) term: Term,
    pub(crate) previous_vote: NodeId,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct VoteGrantObservation {
    pub(crate) voter_id: NodeId,
    pub(crate) candidate_id: NodeId,
    pub(crate) term: Term,
    pub(crate) candidate_last_log_index: LogIndex,
    pub(crate) candidate_last_log_term: Term,
    pub(crate) voter_last_log_index: LogIndex,
    pub(crate) voter_last_log_term: Term,
    pub(crate) voter_membership: MembershipConfig,
    pub(crate) durable_vote: Option<NodeId>,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct AuthorityTransitionViolation {
    pub(crate) node_id: NodeId,
    pub(crate) message_kind: &'static str,
    pub(crate) message_term: Term,
    pub(crate) before_term: Term,
    pub(crate) after_term: Term,
    pub(crate) before_vote: Option<NodeId>,
    pub(crate) after_vote: Option<NodeId>,
    pub(crate) before_role: rafter::Role,
    pub(crate) after_role: rafter::Role,
    pub(crate) reason: AuthorityTransitionViolationKind,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum AuthorityTransitionViolationKind {
    HigherTermNotFenced,
    StaleTermCreatedLeader,
    StaleTermLoweredAuthority,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct PreVoteViolation {
    pub(crate) node_id: NodeId,
    pub(crate) message_kind: &'static str,
    pub(crate) message_term: Term,
    pub(crate) before_term: Term,
    pub(crate) after_term: Term,
    pub(crate) before_vote: Option<NodeId>,
    pub(crate) after_vote: Option<NodeId>,
    pub(crate) before_role: rafter::Role,
    pub(crate) after_role: rafter::Role,
    pub(crate) reason: PreVoteViolationKind,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PreVoteViolationKind {
    RequestMutatedAuthority,
    RequestDisruptedLeader,
    StaleResponseAdvancedAuthority,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct ElectionCertificate {
    pub(crate) leader_id: NodeId,
    pub(crate) term: Term,
    pub(crate) membership: MembershipConfig,
    pub(crate) granted_by: BTreeSet<NodeId>,
    pub(crate) last_log_index: LogIndex,
    pub(crate) last_log_term: Term,
    pub(crate) logical_prefix_at_election: Option<LogPrefixWitness>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ElectionConflict {
    pub(crate) term: Term,
    pub(crate) first_leader: NodeId,
    pub(crate) second_leader: NodeId,
}

fn last_log_term_from_bootstrap(bootstrap: &BootstrapState) -> Term {
    bootstrap.log.last().map_or_else(
        || {
            bootstrap
                .snapshot
                .as_ref()
                .map_or(Term::default(), |snapshot| {
                    snapshot.metadata.last_included_term
                })
        },
        |entry| entry.term,
    )
}
