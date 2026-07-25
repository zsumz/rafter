use super::{
    BTreeSet, Debug, ErrorCause, GroupError, GroupFatalState, GroupResult, PersistedRaftRuntime,
    RaftGroup, ReplicatedStateMachine, RuntimeGroupError, StateMachineOperation,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    pub(super) fn reject_if_poisoned(&self) -> GroupResult<A, R, ()> {
        if let GroupFatalState::Poisoned { reason } = &self.fatal_state {
            return Err(GroupError::Poisoned {
                reason: reason.clone(),
                cause: self.poison_cause.clone(),
            });
        }
        Ok(())
    }

    pub(super) fn poison_with_state_machine_error(
        &mut self,
        operation: StateMachineOperation,
        source: A::Error,
    ) -> RuntimeGroupError<A, R> {
        self.enter_poisoned(format!("{operation:?} failed"), None);
        GroupError::StateMachine { operation, source }
    }

    pub(super) fn poison_with_malformed_snapshot(
        &mut self,
        reason: String,
    ) -> RuntimeGroupError<A, R> {
        // A malformed snapshot output has no underlying error, so there is
        // nothing honest to retain beside the health state.
        self.enter_poisoned(format!("malformed snapshot output: {reason}"), None);
        GroupError::MalformedSnapshot { reason }
    }

    pub(super) fn enter_poisoned(&mut self, reason: String, cause: Option<ErrorCause>) {
        self.fatal_state = GroupFatalState::Poisoned { reason };
        self.poison_cause = cause;
        self.poisoned_waiters.proposals.extend(
            self.pending_proposals
                .iter()
                .map(|(id, request_id)| (*id, *request_id)),
        );
        let mut read_ids = self.pending_reads.keys().copied().collect::<BTreeSet<_>>();
        read_ids.extend(self.pending_query_reads.keys().copied());
        read_ids.extend(self.completed_query_reads.keys().copied());
        self.poisoned_waiters.reads.extend(read_ids);
        self.pending_proposals.clear();
        self.pending_reads.clear();
        self.pending_query_reads.clear();
        self.completed_query_reads.clear();
    }
}
