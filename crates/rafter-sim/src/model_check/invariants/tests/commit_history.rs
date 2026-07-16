use rafter::{
    BootstrapLogEntry, BootstrapState, CommittedConfiguration, ConfigurationEntry, ConfigurationId,
    JointMembership, LogIndex, NodeConfig, NodeId, Role, Term,
};

use super::super::history::{
    check_current_term_commit_certificates, check_joint_commit_quorums, check_stable_commit_quorums,
};
use super::*;
use crate::model_check::observations::Observation;
use rafter_invariant_test::{
    oracle_assert, oracle_assert_eq, oracle_expect_err, oracle_invoke_recorder,
};

#[rafter_invariant_test::detector_test]
fn commit_certificate_uses_pre_transition_joint_quorum_for_candidate_below_config() {
    let pending_configuration = ConfigurationEntry::stable(
        ConfigurationId(42),
        MembershipSet::new(vec![NodeId(1), NodeId(2)], Vec::new())
            .expect("pending membership is valid"),
    );
    let mut state = state_with_bootstraps(
        voter_configs(&[1, 2, 3]),
        &[
            (
                1,
                bootstrap_with_log(
                    Term(2),
                    LogIndex(1),
                    vec![
                        app_entry(1, Term(2), b"candidate"),
                        BootstrapLogEntry::configuration(
                            LogIndex(2),
                            Term(2),
                            pending_configuration,
                        ),
                    ],
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

    state.record_commit_observation(
        &context,
        Some(ConfigurationAppend {
            proposer: NodeId(1),
            index: LogIndex(2),
        }),
        None,
    );

    let failure = oracle_expect_err!(
        check_joint_commit_quorums(&state, &[]),
        "old-side-only storage must not satisfy the joint quorum",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM
    );
    oracle_assert!(
        failure.message.contains("without an effective quorum"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[rafter_invariant_test::detector_test]
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

    state.record_commit_observation(&context, None, None);

    let failure = oracle_expect_err!(
        check_stable_commit_quorums(&state, &[]),
        "a learner replica must not count toward voter quorum",
    );
    oracle_assert_eq!(
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

    assert_ne!(state.cluster().role(NodeId(1)), Role::Leader);
    state.record_commit_observation(&context, None, None);

    check_commit_history(&state, &[])
        .expect("self-removing leader commit should still have a valid certificate");
    assert!(
        state
            .commit_history()
            .certificates
            .contains_key(&(NodeId(1), Term(2), LogIndex(1))),
        "commit transition should be recorded even after the leader steps down"
    );
}

#[rafter_invariant_test::detector_test]
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

    state.record_commit_observation(&context, None, None);

    let failure = oracle_expect_err!(
        check_current_term_commit_certificates(&state, &[]),
        "prior-term candidate commit must fail",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::CM_03_LEADERS_ONLY_COMMIT_CURRENT_TERM_ENTRIES
    );
    oracle_assert!(
        failure.message.contains("term 2 while leading term 3"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[rafter_invariant_test::detector_test]
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

    state.record_commit_observation(
        &context,
        Some(ConfigurationAppend {
            proposer: NodeId(1),
            index: LogIndex(1),
        }),
        None,
    );

    let failure = oracle_expect_err!(
        check_joint_commit_quorums(&state, &[]),
        "same-operation joint commit must require the new-side majority",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM
    );
    oracle_assert!(
        failure.message.contains("without an effective quorum"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn valid_pre_transition_joint_commit_marks_joint_quorum_observation() {
    let pending_configuration = ConfigurationEntry::stable(
        ConfigurationId(45),
        MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("pending membership is valid"),
    );
    let mut state = state_with_bootstraps(
        voter_configs(&[1, 2, 3, 4, 5]),
        &[
            (
                1,
                bootstrap_with_log(
                    Term(2),
                    LogIndex(1),
                    vec![
                        app_entry(1, Term(2), b"candidate"),
                        BootstrapLogEntry::configuration(
                            LogIndex(2),
                            Term(2),
                            pending_configuration,
                        ),
                    ],
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
        joint_membership(&[1, 2, 3], &[1, 4, 5]),
        LogIndex::ZERO,
    );

    state.record_commit_observation(
        &context,
        Some(ConfigurationAppend {
            proposer: NodeId(1),
            index: LogIndex(2),
        }),
        None,
    );

    check_joint_commit_quorums(&state, &[]).expect("both joint majorities store the candidate");
    assert!(state
        .observation_set()
        .contains(Observation::PreTransitionJointCommitCertificates));
}

#[test]
fn valid_post_append_joint_commit_marks_joint_quorum_observation() {
    let config_id = ConfigurationId(44);
    let old = MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("new membership is valid");
    let configuration =
        ConfigurationEntry::joint(config_id, JointMembership::new(old.clone(), new));
    let entry = || BootstrapLogEntry::configuration(LogIndex(1), Term(2), configuration.clone());
    let mut state = state_with_bootstraps(
        voter_configs(&[1, 2, 3]),
        &[
            (
                1,
                bootstrap_with_log(
                    Term(2),
                    LogIndex(1),
                    vec![entry()],
                    Some(CommittedConfiguration {
                        index: LogIndex(1),
                        config_id,
                    }),
                ),
            ),
            (
                2,
                bootstrap_with_log(Term(2), LogIndex::ZERO, vec![entry()], None),
            ),
            (
                3,
                bootstrap_with_log(Term(2), LogIndex::ZERO, vec![entry()], None),
            ),
        ],
    );
    let context = leader_context(
        &state,
        1,
        Term(2),
        MembershipConfig::stable(old),
        LogIndex::ZERO,
    );

    state.record_commit_observation(
        &context,
        Some(ConfigurationAppend {
            proposer: NodeId(1),
            index: LogIndex(1),
        }),
        None,
    );

    check_joint_commit_quorums(&state, &[]).expect("post-append joint majorities store the entry");
    assert!(state
        .observation_set()
        .contains(Observation::PostAppendJointCommitCertificates));
}

#[test]
fn current_term_commit_covering_prior_term_prefix_marks_atomic_observation() {
    let mut state = state_with_bootstraps(
        voter_configs(&[1]),
        &[{
            (
                1,
                bootstrap_with_log(
                    Term(3),
                    LogIndex(2),
                    vec![
                        app_entry(1, Term(2), b"prior-term"),
                        app_entry(2, Term(3), b"current-term-commit-point"),
                    ],
                    None,
                ),
            )
        }],
    );
    let context = leader_context(
        &state,
        1,
        Term(3),
        stable_membership(&[1], &[]),
        LogIndex::ZERO,
    );

    state.record_commit_observation(&context, None, None);

    check_current_term_commit_certificates(&state, &[])
        .expect("a current-term commit point may commit its prior-term prefix");
    check_stable_commit_quorums(&state, &[])
        .expect("the one-voter stable membership stores the commit point");
    assert!(state
        .observation_set()
        .contains(Observation::StableCommitCertificates));
    assert!(state
        .observation_set()
        .contains(Observation::CurrentTermCommitCoveringPriorTermPrefix));
}

#[rafter_invariant_test::detector_test]
fn leader_completeness_rechecks_when_committed_ledger_grows_after_election() {
    let mut state = state_with_bootstraps(voter_configs(&[1, 2]), &[]);
    let certificate = election_certificate(4, 2, stable_membership(&[1, 2], &[]), &[1, 2]);
    state
        .election_history_mut()
        .elected_by_term
        .insert(certificate.term, vec![certificate]);
    oracle_invoke_recorder!(record_leader_completeness_check(&mut state));
    assert_eq!(
        state
            .commit_history()
            .leader_completeness_checked_through
            .get(&(NodeId(2), Term(4), 0)),
        Some(&LogIndex::ZERO)
    );

    state
        .inject_bootstrap_state(
            NodeId(2),
            bootstrap_with_log(
                Term(4),
                LogIndex::ZERO,
                vec![app_entry(1, Term(3), b"late-commit")],
                None,
            ),
        )
        .expect("former leader log repair is valid");
    state.refresh_log_history();

    state
        .inject_bootstrap_state(
            NodeId(1),
            bootstrap_with_log(
                Term(3),
                LogIndex(1),
                vec![app_entry(1, Term(3), b"late-commit")],
                None,
            ),
        )
        .expect("late committed prefix bootstrap is valid");
    state.witness_seeded_commit_authority(LogIndex::ZERO, LogIndex(1), Term(3));
    state.refresh_log_history();
    state.refresh_committed_prefixes();
    oracle_invoke_recorder!(record_leader_completeness_check(&mut state));

    let failure = oracle_expect_err!(
        check_commit_history(&state, &[]),
        "later lower-term commit must be checked against existing leader",
    );
    oracle_assert_eq!(failure.invariant(), catalog::LG_05_LEADER_COMPLETENESS);
    oracle_assert!(
        failure.message.contains("without committed prefix"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn leader_completeness_checks_every_same_leader_same_term_certificate() {
    let mut state = state_with_bootstraps(
        voter_configs(&[1]),
        &[{
            (
                1,
                bootstrap_with_log(
                    Term(4),
                    LogIndex(1),
                    vec![app_entry(1, Term(3), b"committed")],
                    None,
                ),
            )
        }],
    );
    state.witness_seeded_commit_authority(LogIndex::ZERO, LogIndex(1), Term(3));
    let committed_prefix = state
        .commit_history()
        .committed_prefix
        .clone()
        .expect("fixture has a committed logical prefix");
    let mut valid = election_certificate(4, 1, stable_membership(&[1], &[]), &[1]);
    valid.logical_prefix_at_election = Some(committed_prefix);
    let invalid = election_certificate(4, 1, stable_membership(&[1], &[]), &[1]);
    state
        .election_history_mut()
        .elected_by_term
        .insert(Term(4), vec![valid, invalid]);

    state.record_leader_completeness_observation();

    let failure = oracle_expect_err!(
        check_commit_history(&state, &[]),
        "a valid certificate must not mask a later invalid certificate",
    );
    oracle_assert_eq!(failure.invariant(), catalog::LG_05_LEADER_COMPLETENESS);
}

#[test]
fn leader_completeness_fails_closed_without_election_prefix_witness() {
    let mut state = state_with_bootstraps(voter_configs(&[1, 2]), &[]);
    let mut certificate = election_certificate(4, 2, stable_membership(&[1, 2], &[]), &[1, 2]);
    certificate.logical_prefix_at_election = None;
    state
        .election_history_mut()
        .elected_by_term
        .insert(certificate.term, vec![certificate]);

    let failure = oracle_expect_err!(
        check_commit_history(&state, &[]),
        "missing election-time prefix identity must fail closed",
    );
    oracle_assert_eq!(
        failure.kind(),
        crate::model_check::FailureKind::HarnessError
    );
    oracle_assert!(failure
        .message
        .contains("has no frozen logical-prefix witness"));
}

pub(super) fn state_with_bootstraps(
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

fn record_leader_completeness_check(state: &mut ExplorationState) {
    state.record_leader_completeness_observation();
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
