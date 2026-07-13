use rafter::{
    LeadershipTransferRejection, LocalProposalId, NodeConfig, NodeId, Output, ProposalRejection,
    ReadId, ReadIndexCancelReason, ReadIndexRejection, Role, Term,
};

use super::Cluster;
use crate::ReadTerminalOutput;

const NODE_ID: NodeId = NodeId(1);

#[test]
fn recorder_preserves_rejected_and_canceled_read_outputs_in_order() {
    let config =
        NodeConfig::new(NODE_ID, vec![NodeId(2), NodeId(3)], 3).expect("test node config is valid");
    let mut cluster = Cluster::new(vec![config]);
    cluster.record_outputs(
        NODE_ID,
        vec![
            Output::ReadIndexRejected {
                read_id: ReadId(7),
                reason: ReadIndexRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(0),
                },
            },
            Output::ReadIndexCanceled {
                read_id: ReadId(8),
                reason: ReadIndexCancelReason::LeadershipTransfer { target: NodeId(2) },
            },
        ],
    );

    assert_eq!(
        cluster.read_terminal_outputs(),
        &[
            ReadTerminalOutput::Rejected {
                node_id: NODE_ID,
                operation_id: None,
                request_id: 7,
                reason: ReadIndexRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(0),
                },
            },
            ReadTerminalOutput::Canceled {
                node_id: NODE_ID,
                operation_id: None,
                request_id: 8,
                reason: ReadIndexCancelReason::LeadershipTransfer { target: NodeId(2) },
            },
        ]
    );
    assert_eq!(
        cluster.read_output_correlation_errors().len(),
        2,
        "every unmatched terminal output must fail closed in recorder evidence"
    );
}

#[test]
fn recorder_preserves_membership_and_transfer_rejection_identity() {
    let config =
        NodeConfig::new(NODE_ID, vec![NodeId(2), NodeId(3)], 3).expect("test node config is valid");
    let mut cluster = Cluster::new(vec![config]);
    cluster.record_outputs(
        NODE_ID,
        vec![
            Output::RejectProposal {
                proposal_id: None,
                reason: ProposalRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(0),
                    payload_len: 0,
                },
            },
            Output::RejectProposal {
                proposal_id: Some(LocalProposalId(7)),
                reason: ProposalRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(0),
                    payload_len: 3,
                },
            },
            Output::LeadershipTransferRejected {
                target: NodeId(2),
                reason: LeadershipTransferRejection::NotLeader,
            },
        ],
    );

    assert_eq!(cluster.proposal_rejections().len(), 2);
    assert_eq!(cluster.proposal_rejections()[0].node_id, NODE_ID);
    assert_eq!(cluster.proposal_rejections()[0].proposal_id, None);
    assert_eq!(
        cluster.proposal_rejections()[1].proposal_id,
        Some(LocalProposalId(7))
    );
    assert_eq!(cluster.transfer_rejections().len(), 1);
    assert_eq!(cluster.transfer_rejections()[0].node_id, NODE_ID);
    assert_eq!(cluster.transfer_rejections()[0].target, NodeId(2));
}

#[test]
fn recorder_assigns_distinct_generations_when_a_terminal_read_id_is_reused() {
    let config =
        NodeConfig::new(NODE_ID, vec![NodeId(2), NodeId(3)], 3).expect("test node config is valid");
    let mut cluster = Cluster::new(vec![config]);

    cluster.read_index(NODE_ID, 7);
    cluster.read_index(NODE_ID, 7);

    assert_eq!(cluster.read_registrations()[0].operation_id, 0);
    assert_eq!(cluster.read_registrations()[1].operation_id, 1);
    assert_eq!(cluster.read_terminal_outputs()[0].operation_id(), Some(0));
    assert_eq!(cluster.read_terminal_outputs()[1].operation_id(), Some(1));
}

#[test]
fn restart_retires_a_pending_generation_before_read_id_reuse() {
    let configs = [1_u64, 2, 3]
        .into_iter()
        .map(|id| {
            NodeConfig::new(
                NodeId(id),
                [1_u64, 2, 3]
                    .into_iter()
                    .filter(|peer| *peer != id)
                    .map(NodeId)
                    .collect(),
                3,
            )
            .expect("test node config is valid")
        })
        .collect();
    let mut cluster = Cluster::new(configs);
    for _ in 0..32 {
        if cluster.role(NODE_ID) == Role::Leader {
            break;
        }
        cluster.tick(NODE_ID);
        cluster.deliver_all();
    }
    assert_eq!(cluster.role(NODE_ID), Role::Leader);

    let first = cluster.read_index(NODE_ID, 7);
    assert_eq!(first.operation_id, 0);
    assert!(cluster.read_grants().is_empty());
    assert!(cluster.read_terminal_outputs().is_empty());

    let bootstrap = cluster.bootstrap_state(NODE_ID);
    cluster
        .restart_node_from_bootstrap(NODE_ID, bootstrap)
        .expect("restart is valid");
    let second = cluster.read_index(NODE_ID, 7);

    assert_eq!(second.operation_id, 1);
    assert_eq!(cluster.read_terminal_outputs().len(), 1);
    assert_eq!(cluster.read_terminal_outputs()[0].operation_id(), Some(1));
}
