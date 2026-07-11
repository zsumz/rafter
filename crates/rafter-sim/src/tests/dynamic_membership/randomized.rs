use std::fmt;

use super::super::helpers::elect_node_one;
use super::super::*;
use super::fixtures::{
    add_voter_final_entry, applied_payloads, flush_commit_notifications, learner_four_node_cluster,
    propose_add_voter_joint, restart_all_nodes_from_compacted_snapshots,
};
use crate::model_check::MessageKind;
use rafter::ConfigurationId;

#[test]
fn randomized_membership_trace_is_replayable() {
    let (trace, expected) = record_randomized_add_voter_trace(SimSeed(0x4459_4e41));
    let replayed = replay_membership_trace(&trace);

    assert_eq!(replayed, expected);
    assert!(trace
        .iter()
        .any(|action| matches!(action, MembershipTraceAction::Deliver { .. })));
    assert!(trace
        .iter()
        .any(|action| matches!(action, MembershipTraceAction::ProposeAddVoterJoint)));
    assert!(trace
        .iter()
        .any(|action| action.to_string().starts_with("deliver ")));
}

#[test]
fn randomized_reconfiguration_survives_snapshot_compaction_and_restart() {
    let mut cluster = learner_four_node_cluster(SimSeed(0x5a7c_0a91));
    elect_node_one(&mut cluster);
    let mut trace = Vec::new();

    cluster.propose(NodeId(1), b"before-compact".to_vec());
    drain_random_ready(&mut cluster, &mut trace);
    propose_add_voter_joint(&mut cluster, ConfigurationId(60));
    drain_random_ready(&mut cluster, &mut trace);
    cluster.dangerous_raw_configuration_proposal(
        NodeId(1),
        add_voter_final_entry(ConfigurationId(61)),
        Vec::new(),
    );
    drain_random_ready(&mut cluster, &mut trace);
    cluster.propose(NodeId(1), b"after-reconfig".to_vec());
    drain_random_ready(&mut cluster, &mut trace);

    for node_id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        assert!(
            cluster
                .effective_membership(node_id)
                .contains_voter(NodeId(4)),
            "{node_id} should recover the new voter before compaction"
        );
    }
    restart_all_nodes_from_compacted_snapshots(&mut cluster);

    elect_node_one(&mut cluster);
    cluster.propose(NodeId(1), b"after-compaction-restart".to_vec());
    drain_random_ready(&mut cluster, &mut trace);
    flush_commit_notifications(&mut cluster, NodeId(1));

    for node_id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        assert!(
            cluster
                .effective_membership(node_id)
                .contains_voter(NodeId(4)),
            "{node_id} should retain the dynamic voter after snapshot restart"
        );
        assert!(
            applied_payloads(&cluster, node_id)
                .iter()
                .any(|payload| payload.as_ref() == b"after-compaction-restart"),
            "{node_id} should commit after compacted dynamic-membership restart"
        );
    }
    assert!(
        trace
            .iter()
            .any(|action| matches!(action, MembershipTraceAction::Deliver { .. })),
        "the scenario should exercise randomized delivery"
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MembershipTraceAction {
    Tick(NodeId),
    ProposeAddVoterJoint,
    ProposeAddVoterFinal,
    Deliver {
        from: NodeId,
        to: NodeId,
        message: MessageKind,
    },
}

impl fmt::Display for MembershipTraceAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tick(node_id) => write!(formatter, "tick {node_id}"),
            Self::ProposeAddVoterJoint => formatter.write_str("propose add-voter joint"),
            Self::ProposeAddVoterFinal => formatter.write_str("propose add-voter final"),
            Self::Deliver { from, to, message } => {
                write!(formatter, "deliver {message} {from}->{to}")
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MembershipTraceSummary {
    leaders: Vec<NodeId>,
    commit_indexes: Vec<(NodeId, LogIndex)>,
    last_log_indexes: Vec<(NodeId, LogIndex)>,
    node_four_is_voter: bool,
}

fn record_randomized_add_voter_trace(
    seed: SimSeed,
) -> (Vec<MembershipTraceAction>, MembershipTraceSummary) {
    let mut cluster = learner_four_node_cluster(seed);
    let mut trace = Vec::new();

    for _ in 0..3 {
        cluster.tick(NodeId(1));
        trace.push(MembershipTraceAction::Tick(NodeId(1)));
    }
    drain_random_ready(&mut cluster, &mut trace);

    propose_add_voter_joint(&mut cluster, ConfigurationId(50));
    trace.push(MembershipTraceAction::ProposeAddVoterJoint);
    drain_random_ready(&mut cluster, &mut trace);

    cluster.dangerous_raw_configuration_proposal(
        NodeId(1),
        add_voter_final_entry(ConfigurationId(51)),
        Vec::new(),
    );
    trace.push(MembershipTraceAction::ProposeAddVoterFinal);
    drain_random_ready(&mut cluster, &mut trace);

    let summary = membership_trace_summary(&cluster);
    (trace, summary)
}

fn replay_membership_trace(trace: &[MembershipTraceAction]) -> MembershipTraceSummary {
    let mut cluster = learner_four_node_cluster(SimSeed(0));

    for action in trace {
        match action {
            MembershipTraceAction::Tick(node_id) => cluster.tick(*node_id),
            MembershipTraceAction::ProposeAddVoterJoint => {
                propose_add_voter_joint(&mut cluster, ConfigurationId(50));
            }
            MembershipTraceAction::ProposeAddVoterFinal => {
                cluster.dangerous_raw_configuration_proposal(
                    NodeId(1),
                    add_voter_final_entry(ConfigurationId(51)),
                    Vec::new(),
                );
            }
            MembershipTraceAction::Deliver { from, to, message } => {
                assert!(
                    cluster.deliver_one_matching(|envelope| envelope.from == *from
                        && envelope.to == *to
                        && MessageKind::from(&envelope.message) == *message),
                    "replay could not deliver {message} {from}->{to}"
                );
            }
        }
    }

    membership_trace_summary(&cluster)
}

fn drain_random_ready(cluster: &mut Cluster, trace: &mut Vec<MembershipTraceAction>) {
    while let Some(envelope) = cluster.deliver_random_ready() {
        trace.push(MembershipTraceAction::Deliver {
            from: envelope.from,
            to: envelope.to,
            message: MessageKind::from(&envelope.message),
        });
    }
}

fn membership_trace_summary(cluster: &Cluster) -> MembershipTraceSummary {
    let node_ids = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    MembershipTraceSummary {
        leaders: cluster.leaders(),
        commit_indexes: node_ids
            .into_iter()
            .map(|node_id| (node_id, cluster.commit_index(node_id)))
            .collect(),
        last_log_indexes: node_ids
            .into_iter()
            .map(|node_id| (node_id, cluster.last_log_index(node_id)))
            .collect(),
        node_four_is_voter: cluster
            .effective_membership(NodeId(1))
            .contains_voter(NodeId(4)),
    }
}
