use rafter::{
    BootstrapLogEntry, BootstrapState, CommittedConfiguration, ConfigurationEntry, ConfigurationId,
    JointMembership, LogIndex, NodeConfig, NodeId, Role, Term,
};

use super::*;

#[test]
fn commit_certificate_uses_pre_transition_joint_quorum_for_candidate_below_config() {
    let mut state = state_with_bootstraps(
        voter_configs(&[1, 2, 3]),
        &[
            (
                1,
                bootstrap_with_log(
                    Term(2),
                    LogIndex(1),
                    vec![app_entry(1, Term(2), b"candidate")],
                    None,
                ),
            ),
            (
                2,
                bootstrap_with_log(
                    Term(2),
                    LogIndex::ZERO,
                    vec![app_entry(1, Term(2), b"candidate")],
                    None,
                ),
            ),
        ],
    );
    let context = leader_context(
        &state,
        1,
        Term(2),
        joint_membership(&[1, 2, 3], &[1, 4, 5]),
        LogIndex::ZERO,
    );

    state.record_commit_observation(&context, None);

    let failure = check_commit_history(&state, &[])
        .expect_err("old-side-only storage must not satisfy the joint quorum");
    assert_eq!(
        failure.invariant(),
        catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM
    );
    assert!(
        failure.message.contains("without an effective quorum"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn commit_certificate_rejects_learner_storage_as_voter_quorum() {
    let mut state = state_with_bootstraps(
        voter_and_learner_configs(&[1, 2, 3], &[4]),
        &[
            (
                1,
                bootstrap_with_log(
                    Term(2),
                    LogIndex(1),
                    vec![app_entry(1, Term(2), b"candidate")],
                    None,
                ),
            ),
            (
                4,
                bootstrap_with_log(
                    Term(2),
                    LogIndex::ZERO,
                    vec![app_entry(1, Term(2), b"candidate")],
                    None,
                ),
            ),
        ],
    );
    let context = leader_context(
        &state,
        1,
        Term(2),
        stable_membership(&[1, 2, 3], &[4]),
        LogIndex::ZERO,
    );

    state.record_commit_observation(&context, None);

    let failure = check_commit_history(&state, &[])
        .expect_err("a learner replica must not count toward voter quorum");
    assert_eq!(
        failure.invariant(),
        catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM
    );
}

#[test]
fn commit_certificate_records_self_removing_leader_after_stepdown() {
    let config_id = ConfigurationId(41);
    let configuration = ConfigurationEntry::stable(
        config_id,
        MembershipSet::new(vec![NodeId(2)], Vec::new()).expect("membership is valid"),
    );
    let mut state = state_with_bootstraps(
        voter_configs(&[1, 2]),
        &[(
            1,
            bootstrap_with_log(
                Term(2),
                LogIndex(1),
                vec![BootstrapLogEntry::configuration(
                    LogIndex(1),
                    Term(2),
                    configuration,
                )],
                Some(CommittedConfiguration {
                    index: LogIndex(1),
                    config_id,
                }),
            ),
        )],
    );
    let context = leader_context(
        &state,
        1,
        Term(2),
        stable_membership(&[1], &[]),
        LogIndex::ZERO,
    );

    assert_ne!(state.cluster.role(NodeId(1)), Role::Leader);
    state.record_commit_observation(&context, None);

    check_commit_history(&state, &[])
        .expect("self-removing leader commit should still have a valid certificate");
    assert!(
        state
            .commit_history
            .certificates
            .contains_key(&(NodeId(1), Term(2), LogIndex(1))),
        "commit transition should be recorded even after the leader steps down"
    );
}

#[test]
fn commit_certificate_detects_prior_term_candidate_commit() {
    let mut state = state_with_bootstraps(
        voter_configs(&[1]),
        &[(
            1,
            bootstrap_with_log(
                Term(3),
                LogIndex(1),
                vec![app_entry(1, Term(2), b"prior-term")],
                None,
            ),
        )],
    );
    let context = leader_context(
        &state,
        1,
        Term(3),
        stable_membership(&[1], &[]),
        LogIndex::ZERO,
    );

    state.record_commit_observation(&context, None);

    let failure =
        check_commit_history(&state, &[]).expect_err("prior-term candidate commit must fail");
    assert_eq!(
        failure.invariant(),
        catalog::CM_03_LEADERS_ONLY_COMMIT_CURRENT_TERM_ENTRIES
    );
    assert!(
        failure.message.contains("term 2 while leading term 3"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn commit_certificate_uses_post_append_joint_quorum_for_same_operation_commit() {
    let config_id = ConfigurationId(43);
    let old = MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("new membership is valid");
    let configuration =
        ConfigurationEntry::joint(config_id, JointMembership::new(old.clone(), new));
    let mut state = state_with_bootstraps(
        voter_configs(&[1]),
        &[{
            (
                1,
                bootstrap_with_log(
                    Term(2),
                    LogIndex(1),
                    vec![BootstrapLogEntry::configuration(
                        LogIndex(1),
                        Term(2),
                        configuration,
                    )],
                    Some(CommittedConfiguration {
                        index: LogIndex(1),
                        config_id,
                    }),
                ),
            )
        }],
    );
    let context = leader_context(
        &state,
        1,
        Term(2),
        MembershipConfig::stable(old),
        LogIndex::ZERO,
    );

    state.record_commit_observation(&context, Some(NodeId(1)));

    let failure = check_commit_history(&state, &[])
        .expect_err("same-operation joint commit must require the new-side majority");
    assert_eq!(
        failure.invariant(),
        catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM
    );
    assert!(
        failure.message.contains("without an effective quorum"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn leader_completeness_rechecks_when_committed_ledger_grows_after_election() {
    let mut state = state_with_bootstraps(voter_configs(&[1, 2]), &[]);
    let certificate = election_certificate(4, 2, stable_membership(&[1, 2], &[]), &[1, 2]);
    state
        .election_history
        .elected_by_term
        .insert(certificate.term, certificate);
    state.record_leader_completeness_observation();
    assert_eq!(
        state
            .commit_history
            .leader_completeness_checked_through
            .get(&(NodeId(2), Term(4))),
        Some(&LogIndex::ZERO)
    );

    state
        .cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_with_log(
                Term(3),
                LogIndex(1),
                vec![app_entry(1, Term(3), b"late-commit")],
                None,
            ),
        )
        .expect("late committed prefix bootstrap is valid");
    state.refresh_log_history();
    state.refresh_committed_prefixes();
    state.record_leader_completeness_observation();

    let failure = check_commit_history(&state, &[])
        .expect_err("later lower-term commit must be checked against existing leader");
    assert_eq!(failure.invariant(), catalog::LG_05_LEADER_COMPLETENESS);
    assert!(
        failure.message.contains("without committed prefix"),
        "unexpected failure message: {}",
        failure.message
    );
}

fn state_with_bootstraps(
    configs: Vec<NodeConfig>,
    bootstraps: &[(u64, BootstrapState)],
) -> ExplorationState {
    let mut cluster = Cluster::new(configs);
    for (node_id, bootstrap) in bootstraps {
        cluster
            .restart_node_from_bootstrap(NodeId(*node_id), bootstrap.clone())
            .expect("test bootstrap is valid");
    }
    ExplorationState::new(cluster)
}

fn leader_context(
    state: &ExplorationState,
    node_id: u64,
    term: Term,
    membership: MembershipConfig,
    old_commit: LogIndex,
) -> std::collections::BTreeMap<NodeId, CommitTransitionContext> {
    let mut context = state.commit_transition_context();
    let node_id = NodeId(node_id);
    let leader = context
        .get_mut(&node_id)
        .expect("leader node is present in context");
    leader.role = Role::Leader;
    leader.term = term;
    leader.effective_membership = membership;
    leader.old_commit = old_commit;
    context
}

pub(super) fn voter_configs(voters: &[u64]) -> Vec<NodeConfig> {
    voters.iter().map(|id| voter_config(*id, voters)).collect()
}

fn voter_and_learner_configs(voters: &[u64], learners: &[u64]) -> Vec<NodeConfig> {
    let mut configs = voter_configs(voters);
    configs.extend(learners.iter().map(|id| {
        NodeConfig::new_non_voter(NodeId(*id), ids(voters), 3).expect("learner config is valid")
    }));
    configs
}

fn voter_config(id: u64, voters: &[u64]) -> NodeConfig {
    let peers = voters
        .iter()
        .copied()
        .filter(|peer| *peer != id)
        .map(NodeId)
        .collect();
    NodeConfig::new(NodeId(id), peers, 3).expect("voter config is valid")
}

pub(super) fn bootstrap_with_log(
    current_term: Term,
    commit_index: LogIndex,
    log: Vec<BootstrapLogEntry>,
    committed_configuration: Option<CommittedConfiguration>,
) -> BootstrapState {
    BootstrapState {
        current_term,
        voted_for: None,
        commit_index,
        committed_configuration,
        snapshot: None,
        log,
    }
}

pub(super) fn app_entry(index: u64, term: Term, payload: &[u8]) -> BootstrapLogEntry {
    BootstrapLogEntry::application(LogIndex(index), term, payload.to_vec())
}
