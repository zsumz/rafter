//! Fixture builders and drivers shared by the election negative controls.
//!
//! Seeds the observed request-vote grant those controls judge, the three-node
//! cluster whose staggered timeouts make a stale pre-vote reachable, and the
//! recorder and delivery drivers the suite steps through.

use super::super::*;
use crate::model_check::{scheduling::Operation, state::apply_to_state};

pub(super) fn request_vote_grant_state(
    candidate_id: NodeId,
    voter_entries: &[(u64, Term, &[u8])],
    request: RequestVote,
    durable_vote: Option<NodeId>,
) -> ExplorationState {
    let mut before = one_node_cluster();
    before
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(3), voter_entries))
        .expect("before voter bootstrap is valid");

    let mut after_bootstrap = bootstrap_state(request.term, voter_entries);
    after_bootstrap.voted_for = durable_vote;
    let mut after = one_node_cluster();
    after
        .restart_node_from_bootstrap(NodeId(1), after_bootstrap)
        .expect("after voter bootstrap is valid");

    let mut state = ExplorationState::new(after);
    let delivered = Envelope {
        from: candidate_id,
        to: NodeId(1),
        message: Message::RequestVote(request),
    };
    let emitted = [Envelope {
        from: NodeId(1),
        to: candidate_id,
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: request.term,
            voter_id: NodeId(1),
            vote_granted: true,
        }),
    }];
    state.record_election_observation(&before, Some(&delivered), &emitted);
    state
}

pub(super) fn pre_vote_three_node_cluster() -> Cluster {
    Cluster::new(vec![
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3).expect("node-1 config is valid"),
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 9).expect("node-2 config is valid"),
        NodeConfig::new(NodeId(3), vec![NodeId(1), NodeId(2)], 9).expect("node-3 config is valid"),
    ])
}

pub(super) fn record_election_authority_observation(state: &mut ExplorationState) {
    state.observe_election_authority();
}

pub(super) fn record_election_certificate(
    state: &mut ExplorationState,
    certificate: ElectionCertificate,
) {
    state.election_history_mut().record_election(certificate);
}

pub(super) fn deliver_all_pending_in_state(state: &mut ExplorationState) {
    while state.cluster().pending().next().is_some() {
        apply_to_state(state, Operation::DeliverReadyAt(0));
    }
}

pub(super) fn deliver_pending_matching_in_state(
    state: &mut ExplorationState,
    mut predicate: impl FnMut(&Envelope) -> bool,
) -> usize {
    let mut delivered = 0;
    loop {
        let position = state
            .cluster()
            .pending()
            .enumerate()
            .find_map(|(position, envelope)| predicate(envelope).then_some(position));
        let Some(position) = position else {
            break;
        };
        apply_to_state(state, Operation::DeliverReadyAt(position));
        delivered += 1;
    }
    delivered
}
