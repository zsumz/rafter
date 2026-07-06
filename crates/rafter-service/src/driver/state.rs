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
            return Err(ManagedOperationError::Transport("wrong group".to_owned()));
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
                    message: format!("{error:?}"),
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

    pub(super) fn route_report(&mut self, report: GroupStepReport<G, A::CommandResult>) {
        self.network.extend(report.peer_messages);
    }

    pub(super) fn poisoned_write_error_from_primary(
        &mut self,
        local_proposal_id: LocalProposalId,
        options: WriteOptions,
    ) -> Option<WriteError> {
        let group = self.groups.get_mut(&self.primary_node_id)?;
        if !group
            .poisoned_waiters()
            .proposals
            .iter()
            .any(|(id, _)| *id == local_proposal_id)
        {
            return None;
        }
        let waiters = group.drain_poisoned_waiters();
        let client_request_id = waiters
            .proposals
            .into_iter()
            .find_map(|(id, client_request_id)| {
                (id == local_proposal_id).then_some(client_request_id)
            })
            .flatten()
            .or(options.client_request_id);
        Some(WriteError::UnknownOutcome {
            local_proposal_id,
            client_request_id,
            reason: UnknownOutcomeReason::GroupPoisoned,
        })
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
        let _ = group.drain_poisoned_waiters();
        Some(ReadError::Poisoned { reason })
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
        Err(ManagedOperationError::Transport(format!(
            "managed driver did not drain after {} steps",
            self.max_drive_steps
        )))
    }

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

    pub(super) fn next_local_proposal_id(&mut self) -> Result<LocalProposalId, WriteError> {
        let id = self
            .next_proposal_id
            .ok_or(WriteError::LocalProposalIdExhausted)?;
        self.next_proposal_id = id.checked_add(1);
        Ok(LocalProposalId(id))
    }

    pub(super) fn next_read_id(&mut self) -> Result<ReadId, ReadError> {
        let id = self.next_read_id.ok_or(ReadError::ReadIdExhausted)?;
        self.next_read_id = id.checked_add(1);
        Ok(ReadId(id))
    }
}

pub(super) fn lock_state<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
