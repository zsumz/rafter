use rafter::NodeId;

use super::super::{
    project_raft_trace_to_tla, render_tla_trace_spec, require_tla_projectable_raft_trace, Action,
    EnvelopeIdentity, MessageKind, ProposalId, TlaAbstractionGap, TlaAction, TlaProjection,
    TlaTraceRenderError,
};
use crate::SimTick;

#[test]
fn raft_trace_projects_to_tla_action_vocabulary() {
    let trace = vec![
        Action::Tick(NodeId(1)),
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(7),
        },
        Action::ReadIndex {
            to: NodeId(1),
            request_id: 11,
        },
        Action::Restart(NodeId(3)),
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(2),
            message: MessageKind::RequestVote,
            identity: identity(),
        },
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(2),
            message: MessageKind::AppendEntries,
            identity: identity(),
        },
    ];

    let projection = project_raft_trace_to_tla(&trace);
    let actions = require_tla_projectable_raft_trace(&trace)
        .expect("trace should fit the abstract TLA+ action vocabulary");

    assert_eq!(
        actions,
        vec![
            TlaAction::Timeout { node_id: NodeId(1) },
            TlaAction::ClientAppend { node_id: NodeId(1) },
            TlaAction::RegisterRead { node_id: NodeId(1) },
            TlaAction::Restart { node_id: NodeId(3) },
            TlaAction::DeliverRequestVote {
                from: NodeId(1),
                to: NodeId(2),
            },
            TlaAction::DeliverAppend {
                from: NodeId(1),
                to: NodeId(2),
            },
        ]
    );
    assert_eq!(actions[0].tla_name(), "Timeout");
    assert_eq!(actions[2].tla_name(), "RegisterRead");
    assert_eq!(actions[3].tla_name(), "Restart");
    assert_eq!(actions[5].to_string(), "DeliverAppend(node-1->node-2)");
    assert_eq!(projection[1].rust_action(), &trace[1]);
    assert_eq!(
        projection[1].projection(),
        TlaProjection::Action(actions[1])
    );
}

#[test]
fn raft_trace_tla_projection_names_abstraction_gaps() {
    let trace = vec![
        Action::Deliver {
            from: NodeId(2),
            to: NodeId(1),
            message: MessageKind::AppendEntriesResponse,
            identity: identity(),
        },
        Action::Deliver {
            from: NodeId(2),
            to: NodeId(1),
            message: MessageKind::PreVote,
            identity: identity(),
        },
        Action::AddLearner {
            to: NodeId(1),
            learner_id: NodeId(4),
        },
    ];

    let projection = project_raft_trace_to_tla(&trace);
    let failure = require_tla_projectable_raft_trace(&trace)
        .expect_err("response messages are an implementation-level gap");

    assert_eq!(
        projection[0].projection(),
        TlaProjection::Gap(TlaAbstractionGap::AppendEntriesResponse)
    );
    assert_eq!(
        projection[1].projection(),
        TlaProjection::Gap(TlaAbstractionGap::PreVote)
    );
    assert_eq!(
        projection[2].projection(),
        TlaProjection::Gap(TlaAbstractionGap::MembershipChange)
    );
    assert_eq!(failure.action_index(), 0);
    assert_eq!(failure.action(), &trace[0]);
    assert_eq!(failure.gap().code(), "append_entries_response_abstracted");
}

const fn identity() -> EnvelopeIdentity {
    EnvelopeIdentity::new(SimTick(0), 0)
}

#[test]
fn raft_trace_renders_tla_tlc_checkable_sample_spec() {
    let trace = vec![
        Action::Tick(NodeId(1)),
        Action::Restart(NodeId(2)),
        Action::Tick(NodeId(2)),
    ];
    let spec = render_tla_trace_spec("RaftTraceSample", &trace)
        .expect("sample trace should fit the abstract TLA+ action subset");

    assert_eq!(
        spec.module(),
        include_str!("../../../../../specs/tla/raft/RaftTraceSample.tla")
    );
    assert_eq!(
        spec.config(),
        include_str!("../../../../../specs/tla/raft/RaftTraceSample.cfg")
    );
}

#[test]
fn raft_trace_tla_render_rejects_node_outside_config_bound() {
    let trace = vec![Action::Tick(NodeId(4))];
    let error = render_tla_trace_spec("InvalidNodeTrace", &trace)
        .expect_err("n4 is outside the generated three-node TLA+ config");
    let text = error.to_string();

    match error {
        TlaTraceRenderError::NodeOutOfBounds {
            action_index,
            action,
            node_id,
        } => {
            assert_eq!(action_index, 0);
            assert_eq!(action, TlaAction::Timeout { node_id: NodeId(4) });
            assert_eq!(node_id, NodeId(4));
        }
        other => panic!("unexpected render error: {other}"),
    }
    assert!(text.contains("trace action 0"));
    assert!(text.contains("n4"));
}

#[test]
fn raft_trace_tla_render_rejects_too_many_proposal_values() {
    let trace = vec![
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
        },
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(2),
        },
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(3),
        },
    ];
    let error = render_tla_trace_spec("TooManyValuesTrace", &trace)
        .expect_err("third proposal needs a third symbolic TLA+ value");
    let text = error.to_string();

    match error {
        TlaTraceRenderError::TooManyValues {
            action_index,
            requested_value,
            available_values,
            ..
        } => {
            assert_eq!(action_index, 2);
            assert_eq!(requested_value, 3);
            assert_eq!(available_values, 2);
        }
        other => panic!("unexpected render error: {other}"),
    }
    assert!(text.contains("trace action 2"));
    assert!(text.contains("v3"));
}

#[test]
fn raft_trace_tla_render_rejects_too_many_read_requests() {
    let trace = vec![
        Action::ReadIndex {
            to: NodeId(1),
            request_id: 1,
        },
        Action::ReadIndex {
            to: NodeId(1),
            request_id: 2,
        },
        Action::ReadIndex {
            to: NodeId(1),
            request_id: 3,
        },
    ];
    let error = render_tla_trace_spec("TooManyReadsTrace", &trace)
        .expect_err("third read needs a third symbolic TLA+ read request");
    let text = error.to_string();

    match error {
        TlaTraceRenderError::TooManyReadRequests {
            action_index,
            requested_read_request,
            available_read_requests,
            ..
        } => {
            assert_eq!(action_index, 2);
            assert_eq!(requested_read_request, 3);
            assert_eq!(available_read_requests, 2);
        }
        other => panic!("unexpected render error: {other}"),
    }
    assert!(text.contains("trace action 2"));
    assert!(text.contains("r3"));
}
