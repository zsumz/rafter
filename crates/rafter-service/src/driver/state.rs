#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) struct InMemoryRaftState<G, A, R> {
    pub(super) group_id: G,
    pub(super) primary_node_id: NodeId,
    pub(super) groups: BTreeMap<NodeId, RaftGroup<G, A, R>>,
    pub(super) network: VecDeque<PeerEnvelope<G>>,
    pub(super) metrics: MetricsPublisher<G>,
    pub(super) next_proposal_id: Option<u64>,
    pub(super) next_read_id: Option<u64>,
    /// The answer a routed [`ReadEvent`] carried for a barrier the group ended
    /// during routing, waiting for the read loop that owns it.
    ///
    /// One slot rather than a table: this driver resolves each client future
    /// inside the call that created it and holds its state lock for the whole
    /// call, so at most one barrier is outstanding at a time. A leftover from
    /// an operation that already returned is discarded by the next read's first
    /// look, because generated read IDs strictly increase and cannot match one.
    pub(super) routed_read_outcome: Option<(ReadId, ReadError)>,
    pub(super) max_drive_steps: usize,
    pub(super) shutting_down: bool,
}

impl<G, A, R> InMemoryRaftState<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    A::Error: Debug + Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    R::Error: Debug + Send + 'static,
{
    pub(super) fn reject_if_shutting_down(&self) -> Result<(), ManagedDriverError> {
        if self.shutting_down {
            return Err(ManagedDriverError::ShuttingDown);
        }
        Ok(())
    }

    pub(super) fn reject_for_operation(&self, group_id: &G) -> ManagedResult<A, R, ()> {
        if self.shutting_down {
            return Err(ManagedOperationError::ShuttingDown);
        }
        if group_id != &self.group_id {
            // A command the driver never looked at is not a delivery failure.
            return Err(ManagedOperationError::WrongGroup);
        }
        Ok(())
    }

    pub(super) fn tick_primary(&mut self) -> Result<(), ManagedDriverError> {
        let report = match self
            .primary_group_mut()
            .map_err(ManagedDriverError::from)?
            .step(GroupInput::Tick)
        {
            Ok(report) => report,
            Err(error) => {
                self.publish_primary_metrics();
                return Err(ManagedDriverError::Group {
                    cause: ErrorCause::new(error),
                });
            }
        };
        self.route_report(report);
        if let Err(error) = self.drain_network() {
            self.publish_primary_metrics();
            return Err(ManagedDriverError::from(error));
        }
        self.publish_primary_metrics();
        Ok(())
    }

    pub(super) fn primary_role(&self) -> Result<Role, ManagedDriverError> {
        self.groups
            .get(&self.primary_node_id)
            .map(|group| group.metrics().role)
            .ok_or(ManagedDriverError::MissingPrimary {
                node_id: self.primary_node_id,
            })
    }

    pub(super) fn primary_group_mut(&mut self) -> ManagedGroupResult<'_, G, A, R> {
        self.groups
            .get_mut(&self.primary_node_id)
            .ok_or(ManagedOperationError::MissingNode {
                node_id: self.primary_node_id,
            })
    }

    /// Puts the report's peer messages on the in-memory network and keeps the
    /// answer of any barrier the group ended in that step.
    ///
    /// Read events are kept for the reason
    /// [`TransportDriverState::route_report`] keeps them: the app layer ends a
    /// barrier in whichever step observes the cause, and for a leadership change
    /// that step is a delivery rather than the read call. A driver that dropped
    /// them would go around its loop and ask the group to re-reserve a spent
    /// `ReadId`, which `RaftGroup` answers with `NonMonotonicReadId` — reported
    /// to the client as a driver invariant violation, in place of the ordinary
    /// cancellation that actually happened.
    ///
    /// [`terminal_read_error`] decides what is terminal, and the transport
    /// driver reads its events through the same function.
    pub(super) fn route_report(&mut self, report: GroupStepReport<G, A::CommandResult>) {
        self.network.extend(report.peer_messages);
        for event in &report.read_events {
            if let Some(resolved) = terminal_read_error(event) {
                self.routed_read_outcome = Some(resolved);
            }
        }
    }

    /// Takes the routed answer for `read_id`, if routing produced one.
    ///
    /// The slot is cleared either way: an answer that is not this read's belongs
    /// to an operation that already returned, and keeping it would only let it
    /// be mistaken for a later one.
    pub(super) fn take_routed_read_outcome(
        &mut self,
        read_id: Option<ReadId>,
    ) -> Option<ReadError> {
        let (resolved, error) = self.routed_read_outcome.take()?;
        (Some(resolved) == read_id).then_some(error)
    }

    pub(super) fn poisoned_read_error_from_primary(
        &mut self,
        read_id: ReadId,
    ) -> Option<ReadError> {
        let reason = self.primary_poison_reason()?;
        let group = self.groups.get_mut(&self.primary_node_id)?;
        if !group.poisoned_waiters().reads.contains(&read_id) {
            return None;
        }
        let cause = group.poison_cause().cloned();
        let _ = group.drain_poisoned_waiters();
        Some(ReadError::Poisoned { reason, cause })
    }

    pub(super) fn primary_poison_reason(&self) -> Option<String> {
        self.groups
            .get(&self.primary_node_id)
            .and_then(|group| match group.fatal_state() {
                GroupFatalState::Poisoned { reason } => Some(reason.clone()),
                GroupFatalState::Healthy => None,
            })
    }

    pub(super) fn dispatch_one(&mut self) -> DriverStepResult<G, A, R> {
        let Some(envelope) = self.network.pop_front() else {
            return Ok(None);
        };
        let to = envelope.to;
        let group = self
            .groups
            .get_mut(&to)
            .ok_or(ManagedOperationError::MissingNode { node_id: to })?;
        let report = group.step(GroupInput::PeerMessage { envelope })?;
        Ok(Some(report))
    }

    pub(super) fn drain_network(&mut self) -> ManagedResult<A, R, ()> {
        for _ in 0..self.max_drive_steps {
            let Some(report) = self.dispatch_one()? else {
                return Ok(());
            };
            self.route_report(report);
        }
        Err(ManagedOperationError::DriveBoundReached {
            max_steps: self.max_drive_steps,
        })
    }

    /// Publishes the primary's metrics, and discards the one thing publishing
    /// can report.
    ///
    /// This driver owns the publisher and is the only thing that closes it, in
    /// `release_groups` and `shutdown`. A refusal here therefore means the
    /// driver is already down, and a metrics snapshot from a driver that is
    /// already down is exactly the one nobody is waiting for.
    pub(super) fn publish_primary_metrics(&self) {
        if let Some(group) = self.groups.get(&self.primary_node_id) {
            let _ = self.metrics.publish(group.metrics());
        }
    }

    pub(super) fn abandon_read(&mut self, read_id: ReadId) {
        let removed = self
            .groups
            .get_mut(&self.primary_node_id)
            .is_some_and(|group| group.cancel_read(read_id));
        if removed {
            self.publish_primary_metrics();
        }
    }

    pub(super) fn reserve_local_proposal_ids(
        &mut self,
        count: usize,
    ) -> Result<Vec<LocalProposalId>, WriteError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let count = u64::try_from(count).map_err(|_| WriteError::LocalProposalIdExhausted)?;
        let first = self
            .next_proposal_id
            .ok_or(WriteError::LocalProposalIdExhausted)?;
        let last = first
            .checked_add(count - 1)
            .ok_or(WriteError::LocalProposalIdExhausted)?;
        self.next_proposal_id = last.checked_add(1);
        Ok((first..=last).map(LocalProposalId).collect())
    }

    pub(super) fn next_read_id(&mut self) -> Result<ReadId, ReadError> {
        let id = self.next_read_id.ok_or(ReadError::ReadIdExhausted)?;
        self.next_read_id = id.checked_add(1);
        Ok(ReadId(id))
    }
}

/// Locks driver state, taking the contents of a poisoned mutex rather than
/// propagating the poison.
///
/// A `PoisonError` says a thread panicked while holding the lock, not that this
/// driver's state is invalid. Every mutation here is a whole-value assignment or
/// a map insert/remove, so an interrupted one leaves the state consistent even
/// when it leaves the operation unfinished — and the failure a client should
/// hear about is the group's own poison, which
/// [`rafter_app::group::RaftGroup`] reports as a typed error with its cause.
/// Propagating the mutex poison instead would replace that answer with a panic
/// on every later call, including the calls a supervisor makes to release the
/// group and read what happened.
pub(super) fn lock_state<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
