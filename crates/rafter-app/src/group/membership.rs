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

    /// Reports every membership fact this step moved.
    ///
    /// **Two independent diffs, and no third condition on either.** The stream
    /// is what a transport driver follows to keep its peer set current, so a
    /// configuration this replica moved through and did not report is a set of
    /// replicas the link layer authorizes wrongly until some unrelated later
    /// change happens to arrive. The cause of the move is therefore not consulted:
    /// a local membership request, a configuration entry that arrived by
    /// replication, a new leader truncating an uncommitted one back off the log,
    /// and a snapshot install are the same fact to a consumer, and the two of
    /// them that carry no membership request are the two most dangerous to miss.
    ///
    /// This previously gated the effective event on the step having carried a
    /// membership *request* and on the committed configuration standing still.
    /// Both clauses were wrong and each hid a different case. The request clause
    /// silenced every follower: a replica that learns a joint configuration by
    /// replication never widens, so the joiner it must authorize cannot catch up
    /// and the change that needs it cannot commit. The standing-still clause
    /// silenced every step that moved both at once — a single-voter leader
    /// commits its own change in the step that appends it — leaving a consumer
    /// the committed fact alone, which is exactly the fact it may not widen for.
    ///
    /// **Order is load-bearing and is effective-then-committed.** One step can
    /// commit a configuration while a later one is already in effect, and a
    /// consumer that may only narrow for the committed fact has to have seen the
    /// widening first, or it narrows past a joiner the configuration in effect
    /// still needs.
    ///
    /// A step that moves neither fact reports nothing, which is what keeps "the
    /// configuration changed" readable: a consumer republishing an unchanged
    /// peer set every tick cannot tell a real change from noise.
    pub(super) fn record_membership_changes(
        &self,
        context: &MembershipStepContext,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) {
        let effective_membership = self.raft.membership();
        let committed_membership = self.raft.committed_membership();

        if effective_membership != context.previous_effective {
            // The observation point rather than the entry that caused the move,
            // because a truncation and a snapshot install have no such entry.
            // `MembershipEvent::EffectiveChanged` says so at the field.
            let index = self.raft.last_log_index();
            let term = self
                .raft
                .term_at_index(index)
                .unwrap_or_else(|| self.raft.current_term());
            report
                .membership_events
                .push(MembershipEvent::EffectiveChanged {
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
