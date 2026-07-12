use std::hash::{Hash, Hasher};

use rafter::{
    AppendEntries, BootstrapLogEntry, BootstrapState, LogEntry, LogIndex, Message, NodeConfig,
    NodeId, Role, SharedEntries, Term,
};

use super::super::super::{
    helpers::{elect_node_one_in_state, three_node_configs},
    scheduling::Operation,
    state::apply_to_state,
    ProposalId,
};
use super::*;

#[test]
fn higher_term_follower_commit_is_not_attributed_to_old_leader_authority() {
    let mut state = ExplorationState::new(Cluster::new(three_node_configs()));
    elect_node_one_in_state(&mut state);
    assert_eq!(state.cluster().role(NodeId(1)), Role::Leader);
    assert_eq!(state.cluster().commit_index(NodeId(1)), LogIndex(1));

    apply_to_state(
        &mut state,
        Operation::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
            stale_leader: false,
        },
    );
    state.drop_all_messages();
    state.inject_message(
        NodeId(2),
        NodeId(1),
        Message::AppendEntries(AppendEntries {
            term: Term(2),
            leader_id: NodeId(2),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(1),
            sequence: 10,
            entries: SharedEntries::from(vec![LogEntry::application(
                Term(2),
                b"higher-term".to_vec(),
            )]),
            leader_commit: LogIndex(2),
        }),
    );

    apply_to_state(&mut state, Operation::DeliverReadyAt(0));

    assert_eq!(state.cluster().current_term(NodeId(1)), Term(2));
    assert_eq!(state.cluster().role(NodeId(1)), Role::Follower);
    assert_eq!(state.cluster().commit_index(NodeId(1)), LogIndex(2));
    check_commit_history(&state, &[])
        .expect("higher-term follower commit should not be misattributed to the old leader");
}

#[test]
fn diagnostic_commit_certificates_do_not_change_the_state_hash() {
    let base = state_with_committed_single_node_entry();
    let mut with_certificate = base.clone();
    let mut context = with_certificate.commit_transition_context();
    let leader = context
        .get_mut(&NodeId(1))
        .expect("single node is present in commit context");
    leader.role = Role::Leader;
    leader.term = Term(1);
    leader.effective_membership = stable_membership(&[1], &[]);
    leader.old_commit = LogIndex::ZERO;

    with_certificate.record_commit_observation(&context, None, None);

    assert!(
        with_certificate
            .commit_history()
            .certificates
            .contains_key(&(NodeId(1), Term(1), LogIndex(1))),
        "test setup should differ by a diagnostic commit certificate"
    );
    assert_eq!(state_hash(&base), state_hash(&with_certificate));
}

fn state_with_committed_single_node_entry() -> ExplorationState {
    let mut cluster = Cluster::new(vec![
        NodeConfig::new(NodeId(1), Vec::new(), 3).expect("single-node config is valid")
    ]);
    cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            BootstrapState {
                current_term: Term(1),
                voted_for: None,
                commit_index: LogIndex(1),
                committed_configuration: None,
                snapshot: None,
                log: vec![BootstrapLogEntry::application(
                    LogIndex(1),
                    Term(1),
                    b"committed".to_vec(),
                )],
            },
        )
        .expect("single-node committed bootstrap is valid");
    let mut state = ExplorationState::new(cluster);
    state.witness_seeded_commit_authority(LogIndex::ZERO, LogIndex(1), Term(1));
    state
}

fn state_hash(state: &ExplorationState) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    state.hash(&mut hasher);
    hasher.finish()
}
