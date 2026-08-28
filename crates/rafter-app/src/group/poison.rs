use super::{
    Arc, BTreeSet, ClientRequestId, Debug, ErrorCause, GroupError, GroupResult, LocalProposalId,
    PersistedRaftRuntime, RaftGroup, ReadId, ReplicatedStateMachine, RuntimeGroupError,
    StateMachineOperation,
};

/// Fatal health state for a Raft group.
///
/// This enum is exhaustive: a group is either healthy or permanently poisoned
/// until the caller replaces it through an explicit recovery path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupFatalState {
    /// The group may continue accepting inputs.
    Healthy,
    /// A permanent local fault ended this group incarnation.
    Poisoned {
        /// Human-readable summary suitable for metrics and readiness output.
        reason: String,
    },
}

/// Proposal and read waiters drained when a group enters a fatal poison state.
///
/// A poison is not an event stream: the group moves every pending waiter here
/// and emits nothing further for them. A driver that routes reports and does
/// not drain this leaves those clients waiting forever, which is why every
/// stepping path drains it.
///
/// Writes here must be reported as an unknown outcome, not a refusal: the entry
/// may already be in the durable log and may commit under a later incarnation.
/// Reads are terminal — a barrier the group dropped will never produce an
/// answer.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PoisonedWaiters {
    /// Each dropped proposal's local ID, paired with the caller's own request
    /// ID when one was supplied, so a driver can name the write to its client.
    pub proposals: Vec<(LocalProposalId, Option<ClientRequestId>)>,
    /// Each dropped barrier's read ID. Those IDs are spent; a retry issues a
    /// new read.
    pub reads: Vec<ReadId>,
}

impl PoisonedWaiters {
    /// Returns `true` when no proposal or read waiters were drained by poison.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.proposals.is_empty() && self.reads.is_empty()
    }
}

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

    /// Poisons the group and reports the state machine's own error.
    ///
    /// The failure has two owners from here on: the group keeps it as the
    /// poison cause, so every later refusal on this group reports what broke
    /// rather than the reason string alone, and the caller that triggered it
    /// receives the same error inside [`GroupError::StateMachine`]. They share
    /// one allocation because `A::Error` is deliberately not `Clone`.
    pub(super) fn poison_with_state_machine_error(
        &mut self,
        operation: StateMachineOperation,
        source: A::Error,
    ) -> RuntimeGroupError<A, R> {
        let source = Arc::new(source);
        self.enter_poisoned(
            format!("{operation:?} failed"),
            Some(ErrorCause::from_shared(Arc::clone(&source))),
        );
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
