use rafter::{Input, LocalProposalId, MembershipConfig, MembershipSet, NodeId};

use crate::records::LocalProposalEvent;
use crate::Cluster;
use crate::ReadRegistered;

use super::super::super::{helpers::proposal_payload, scheduling::Operation};

#[derive(Clone, Debug, Default)]
pub(in crate::model_check::state::application) struct AppliedOperationEffects {
    pub(in crate::model_check::state::application) emitted: Vec<crate::Envelope>,
    pub(in crate::model_check::state::application) local_proposals: Vec<LocalProposalEvent>,
    pub(in crate::model_check::state::application) read_registration: Option<ReadRegistered>,
}

pub(in crate::model_check::state::application) fn apply_to_cluster(
    cluster: &mut Cluster,
    operation: &Operation,
) -> AppliedOperationEffects {
    match operation {
        Operation::Tick(node_id) => {
            let outputs = cluster.node_mut(*node_id).step(Input::Tick);
            let recorded = cluster.record_outputs_observed(*node_id, outputs);
            return AppliedOperationEffects {
                emitted: recorded.emitted,
                local_proposals: recorded.local_proposals,
                read_registration: None,
            };
        }
        Operation::Restart(_) | Operation::ApplicationLossRestart(_) => {
            unreachable!("restart operations need invariant context")
        }
        Operation::Propose {
            to, proposal_id, ..
        } => {
            let outputs = cluster.node_mut(*to).step(Input::TrackedClientProposal {
                proposal_id: LocalProposalId(proposal_id.0),
                payload: proposal_payload(*proposal_id),
            });
            let recorded = cluster.record_outputs_observed(*to, outputs);
            return AppliedOperationEffects {
                emitted: recorded.emitted,
                local_proposals: recorded.local_proposals,
                read_registration: None,
            };
        }
        Operation::ReadIndex { to, request_id } => {
            let read_registration = cluster.read_index(*to, *request_id);
            return AppliedOperationEffects {
                read_registration: Some(read_registration),
                ..AppliedOperationEffects::default()
            };
        }
        Operation::AddLearner { to, learner_id } => cluster.add_learner(*to, *learner_id),
        Operation::RemoveLearner { to, learner_id } => {
            if let Some(target) =
                remove_learner_target(cluster.effective_membership(*to), *learner_id)
            {
                cluster.change_membership(*to, target, Vec::new());
            }
        }
        Operation::PromoteLearner {
            to,
            learner_id,
            promotion_barrier,
        } => cluster.promote_learner(*to, *learner_id, *promotion_barrier),
        Operation::RemoveVoter { to, voter_id } => cluster.remove_voter(*to, *voter_id),
        Operation::EnterJoint {
            to,
            target,
            promotion_barriers,
        } => cluster.enter_joint(*to, target.clone(), promotion_barriers.clone()),
        Operation::LeaveJoint { to } => cluster.leave_joint(*to),
        Operation::Transfer { from, target } => cluster.transfer_leadership(*from, *target),
        Operation::DeliverReadyAt(position) => {
            if let Some(queued) = cluster.network.remove(*position) {
                let recorded = cluster.deliver_observed(queued.envelope);
                return AppliedOperationEffects {
                    emitted: recorded.emitted,
                    local_proposals: recorded.local_proposals,
                    read_registration: None,
                };
            }
        }
    }
    AppliedOperationEffects::default()
}

fn remove_learner_target(current: MembershipConfig, learner_id: NodeId) -> Option<MembershipSet> {
    let MembershipConfig::Stable(current) = current else {
        return None;
    };
    if !current.learners().contains(&learner_id) {
        return None;
    }
    let learners = current
        .learners()
        .iter()
        .copied()
        .filter(|node_id| *node_id != learner_id)
        .collect();
    MembershipSet::new(current.voters().to_vec(), learners).ok()
}
