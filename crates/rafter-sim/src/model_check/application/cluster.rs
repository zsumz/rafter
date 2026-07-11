use rafter::{MembershipConfig, MembershipSet, NodeId};

use crate::Cluster;

use super::super::{helpers::proposal_payload, scheduling::Operation};

#[derive(Clone, Debug, Default)]
pub(in crate::model_check::application) struct AppliedOperationEffects {
    pub(in crate::model_check::application) emitted: Vec<crate::Envelope>,
}

pub(in crate::model_check::application) fn apply_to_cluster(
    cluster: &mut Cluster,
    operation: Operation,
) -> AppliedOperationEffects {
    match operation {
        Operation::Tick(node_id) => cluster.tick(node_id),
        Operation::Restart(_) => unreachable!("restart operations need invariant context"),
        Operation::Propose {
            to, proposal_id, ..
        } => cluster.propose(to, proposal_payload(proposal_id)),
        Operation::ReadIndex { to, request_id } => cluster.read_index(to, request_id),
        Operation::AddLearner { to, learner_id } => cluster.add_learner(to, learner_id),
        Operation::RemoveLearner { to, learner_id } => {
            if let Some(target) =
                remove_learner_target(cluster.effective_membership(to), learner_id)
            {
                cluster.change_membership(to, target, Vec::new());
            }
        }
        Operation::PromoteLearner {
            to,
            learner_id,
            promotion_barrier,
        } => cluster.promote_learner(to, learner_id, promotion_barrier),
        Operation::RemoveVoter { to, voter_id } => cluster.remove_voter(to, voter_id),
        Operation::EnterJoint {
            to,
            target,
            promotion_barriers,
        } => cluster.enter_joint(to, target, promotion_barriers),
        Operation::LeaveJoint { to } => cluster.leave_joint(to),
        Operation::DeliverReadyAt(position) => {
            if let Some(queued) = cluster.network.remove(position) {
                let retained_len = cluster.network.len();
                cluster.deliver(queued.envelope);
                return AppliedOperationEffects {
                    emitted: cluster
                        .network
                        .iter()
                        .skip(retained_len)
                        .map(|queued| queued.envelope.clone())
                        .collect(),
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
