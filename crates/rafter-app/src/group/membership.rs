use super::{
    Debug, GroupStepReport, MembershipChange, MembershipEvent, MembershipStepContext,
    PersistedRaftRuntime, ProposalRejection, RaftGroup, RaftInput, ReplicatedStateMachine,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    pub(super) fn membership_change_input(change: MembershipChange) -> RaftInput {
        match change {
            MembershipChange::AddLearner { node_id, info: _ } => RaftInput::AddLearner {
                learner_id: node_id,
            },
            MembershipChange::PromoteLearner { node_id, barrier } => RaftInput::PromoteLearner {
                learner_id: node_id,
                promotion_barrier: barrier,
            },
            MembershipChange::RemoveNode { node_id } => {
                RaftInput::RemoveVoter { voter_id: node_id }
            }
            MembershipChange::EnterJoint {
                target,
                promotion_barriers,
            } => RaftInput::EnterJoint {
                target,
                promotion_barriers,
            },
            MembershipChange::LeaveJoint => RaftInput::LeaveJoint,
            MembershipChange::ChangeVoters { target } => RaftInput::ChangeMembership {
                target,
                promotion_barriers: Vec::new(),
            },
        }
    }

    pub(super) fn record_membership_rejection(
        &self,
        reason: ProposalRejection,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) {
        report.membership_events.push(MembershipEvent::Rejected {
            group_id: self.group_id.clone(),
            reason,
            leader_hint: self.raft.leader_hint(),
        });
    }

    pub(super) fn record_membership_changes(
        &self,
        context: &MembershipStepContext,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) {
        let effective_membership = self.raft.membership();
        let committed_membership = self.raft.committed_membership();

        if context.membership_request
            && effective_membership != context.previous_effective
            && committed_membership == context.previous_committed
        {
            let index = self.raft.last_log_index();
            let term = self
                .raft
                .term_at_index(index)
                .unwrap_or_else(|| self.raft.current_term());
            report.membership_events.push(MembershipEvent::Appended {
                group_id: self.group_id.clone(),
                index,
                term,
                membership: effective_membership,
            });
        }

        if committed_membership == context.previous_committed {
            return;
        }
        let index = self.raft.commit_index();
        let term = self
            .raft
            .term_at_index(index)
            .unwrap_or_else(|| self.raft.current_term());
        report.membership_events.push(MembershipEvent::Applied {
            group_id: self.group_id.clone(),
            index,
            term,
            membership: committed_membership,
        });
    }
}
