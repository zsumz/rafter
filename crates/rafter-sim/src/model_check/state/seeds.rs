use rafter::{
    AppendEntries, BootstrapLogEntry, BootstrapState, CommittedConfiguration, ConfigurationEntry,
    ConfigurationId, JointMembership, LogIndex, MembershipSet, Message, NodeConfig, NodeId, Term,
};

use crate::Cluster;

use super::super::helpers::{
    bootstrap_state, config, elect_node_one_with_node_three, three_node_configs,
};
use super::ExplorationState;

impl ExplorationState {
    pub(in crate::model_check) fn seeded_low_empty_probe(configs: Vec<NodeConfig>) -> Self {
        let mut cluster = Cluster::new(configs);
        cluster
            .restart_node_from_bootstrap(
                NodeId(2),
                bootstrap_state(Term(1), &[(1, Term(1), b"committed-one")]),
            )
            .expect("pre-committed follower seed is valid");
        cluster.deliver_message(
            NodeId(1),
            NodeId(2),
            Message::AppendEntries(AppendEntries {
                term: Term(1),
                leader_id: NodeId(1),
                prev_log_index: LogIndex(1),
                prev_log_term: Term(1),
                sequence: 0,
                entries: Vec::new().into(),
                leader_commit: LogIndex(1),
            }),
        );
        cluster.drop_matching(|_| true);
        cluster.queue_message(
            NodeId(1),
            NodeId(2),
            Message::AppendEntries(AppendEntries {
                term: Term(1),
                leader_id: NodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term(0),
                sequence: 1,
                entries: Vec::new().into(),
                leader_commit: LogIndex(3),
            }),
        );
        Self::new(cluster)
    }

    pub(in crate::model_check) fn seeded_divergent_suffix_probe(configs: Vec<NodeConfig>) -> Self {
        let mut cluster = Cluster::new(configs);
        let leader_entries = &[
            (1, Term(1), b"committed-one".as_slice()),
            (2, Term(2), b"leader-two".as_slice()),
        ];
        for node_id in [NodeId(1), NodeId(3)] {
            cluster
                .restart_node_from_bootstrap(node_id, bootstrap_state(Term(2), leader_entries))
                .expect("committed leader-side seed is valid");
            cluster.deliver_message(
                NodeId(1),
                node_id,
                Message::AppendEntries(AppendEntries {
                    term: Term(2),
                    leader_id: NodeId(1),
                    prev_log_index: LogIndex(2),
                    prev_log_term: Term(2),
                    sequence: 0,
                    entries: Vec::new().into(),
                    leader_commit: LogIndex(2),
                }),
            );
        }
        cluster
            .restart_node_from_bootstrap(
                NodeId(2),
                bootstrap_state(
                    Term(2),
                    &[
                        (1, Term(1), b"committed-one"),
                        (2, Term(1), b"divergent-two"),
                    ],
                ),
            )
            .expect("pre-diverged follower seed is valid");
        cluster.deliver_message(
            NodeId(1),
            NodeId(2),
            Message::AppendEntries(AppendEntries {
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex(1),
                prev_log_term: Term(1),
                sequence: 0,
                entries: Vec::new().into(),
                leader_commit: LogIndex(1),
            }),
        );
        cluster.drop_matching(|_| true);
        cluster.queue_message(
            NodeId(1),
            NodeId(2),
            Message::AppendEntries(AppendEntries {
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex(1),
                prev_log_term: Term(1),
                sequence: 1,
                entries: Vec::new().into(),
                leader_commit: LogIndex(2),
            }),
        );

        let mut state = Self::new(cluster);
        state
            .forbidden_applied_payloads
            .insert(b"divergent-two".to_vec().into());
        state
    }

    pub(in crate::model_check) fn seeded_single_voter_prior_application_noop() -> Self {
        let payload = b"leadership-noop-prior-app".to_vec();
        let mut cluster = Cluster::new(vec![config(1, &[], 1)]);
        cluster
            .restart_node_from_bootstrap(
                NodeId(1),
                bootstrap_state(Term(1), &[(1, Term(1), payload.as_slice())]),
            )
            .expect("single-voter prior application seed is valid");

        let mut state = Self::new(cluster);
        state.require_applied_payload(NodeId(1), LogIndex(1), payload.into());
        state
    }

    pub(in crate::model_check) fn seeded_single_voter_prior_configuration_noop() -> Self {
        let config_id = ConfigurationId(7);
        let membership =
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("membership is valid");
        let configuration = ConfigurationEntry::stable(config_id, membership);
        let mut cluster = Cluster::new(vec![config(1, &[], 1)]);
        cluster
            .restart_node_from_bootstrap(
                NodeId(1),
                BootstrapState {
                    current_term: Term(1),
                    voted_for: None,
                    commit_index: LogIndex::ZERO,
                    committed_configuration: None,
                    snapshot: None,
                    log: vec![BootstrapLogEntry::configuration(
                        LogIndex(1),
                        Term(1),
                        configuration,
                    )],
                },
            )
            .expect("single-voter prior configuration seed is valid");

        let mut state = Self::new(cluster);
        state.require_committed_configuration(
            NodeId(1),
            CommittedConfiguration {
                index: LogIndex(1),
                config_id,
            },
        );
        state
    }

    pub(in crate::model_check) fn seeded_joint_self_quorum_prior_application_noop() -> Self {
        let payload = b"joint-self-quorum-prior-app".to_vec();
        let config_id = ConfigurationId(9);
        let membership =
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("membership is valid");
        let configuration = ConfigurationEntry::joint(
            config_id,
            JointMembership::new(membership.clone(), membership),
        );
        let mut cluster = Cluster::new(vec![config(1, &[], 1)]);
        cluster
            .restart_node_from_bootstrap(
                NodeId(1),
                BootstrapState {
                    current_term: Term(1),
                    voted_for: None,
                    commit_index: LogIndex(1),
                    committed_configuration: Some(CommittedConfiguration {
                        index: LogIndex(1),
                        config_id,
                    }),
                    snapshot: None,
                    log: vec![
                        BootstrapLogEntry::configuration(LogIndex(1), Term(1), configuration),
                        BootstrapLogEntry::application(LogIndex(2), Term(1), payload.clone()),
                    ],
                },
            )
            .expect("joint self-quorum prior application seed is valid");

        let mut state = Self::new(cluster);
        state.require_applied_payload(NodeId(1), LogIndex(2), payload.into());
        state
    }

    pub(in crate::model_check) fn seeded_leadership_transfer_noop_commit() -> Self {
        let mut cluster = Cluster::new(three_node_configs());
        elect_node_one_with_node_three(&mut cluster);
        cluster.deliver_all();
        cluster.transfer_leadership(NodeId(1), NodeId(2));
        let mut state = Self::new(cluster);
        state.require_commit_index(NodeId(2), LogIndex(2));
        state
    }
}
