use rafter::{MembershipSet, NodeId, PromotionBarrier};

use super::super::{Action, ProposalId, SoakAction};

#[derive(Clone, Debug)]
pub(in crate::model_check) struct EnabledAction {
    pub(in crate::model_check) trace: Action,
    pub(in crate::model_check) operation: Operation,
}

#[derive(Clone, Debug)]
pub(in crate::model_check) struct EnabledSoakAction {
    pub(in crate::model_check) trace: SoakAction,
    pub(in crate::model_check) operation: SoakOperation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::model_check) enum Operation {
    Tick(NodeId),
    Restart(NodeId),
    Propose {
        to: NodeId,
        proposal_id: ProposalId,
        stale_leader: bool,
    },
    ReadIndex {
        to: NodeId,
        request_id: u64,
    },
    AddLearner {
        to: NodeId,
        learner_id: NodeId,
    },
    RemoveLearner {
        to: NodeId,
        learner_id: NodeId,
    },
    PromoteLearner {
        to: NodeId,
        learner_id: NodeId,
        promotion_barrier: PromotionBarrier,
    },
    RemoveVoter {
        to: NodeId,
        voter_id: NodeId,
    },
    EnterJoint {
        to: NodeId,
        target: MembershipSet,
        promotion_barriers: Vec<PromotionBarrier>,
    },
    LeaveJoint {
        to: NodeId,
    },
    Transfer {
        from: NodeId,
        target: NodeId,
    },
    DeliverReadyAt(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::model_check) enum SoakOperation {
    Tick(NodeId),
    Propose {
        to: NodeId,
        proposal_id: ProposalId,
        stale_leader: bool,
    },
    DeliverReadyAt(usize),
    DelayAt(usize, u64),
    DropAt(usize),
    DuplicateAt(usize),
    Restart(NodeId),
    ReadIndex {
        to: NodeId,
        request_id: u64,
    },
    AddLearner {
        to: NodeId,
        learner_id: NodeId,
    },
    RemoveLearner {
        to: NodeId,
        learner_id: NodeId,
    },
    PromoteLearner {
        to: NodeId,
        learner_id: NodeId,
        promotion_barrier: PromotionBarrier,
    },
    RemoveVoter {
        to: NodeId,
        voter_id: NodeId,
    },
    EnterJoint {
        to: NodeId,
        target: MembershipSet,
        promotion_barriers: Vec<PromotionBarrier>,
    },
    LeaveJoint {
        to: NodeId,
    },
    Transfer {
        from: NodeId,
        target: NodeId,
    },
    Partition {
        a: NodeId,
        b: NodeId,
    },
    Heal,
    LossyRestart(NodeId),
}
