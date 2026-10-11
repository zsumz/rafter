//! Bounded proposal batches and durable completion correlation.

use super::{
    ApplicationEntry, DurableApplication, DurableClientOwner, RaftHardStateStore, RaftLogSegment,
    RaftSnapshotStore, ServiceError, ServiceInput, ServicePoll, ServiceState, ServiceStepError,
    SnapshotChunkSource,
};
use crate::{
    durable::{
        lock,
        shared::{unknown, Entry},
    },
    UnknownOutcomeReason, WriteError, WriteReceipt,
};
use rafter::{ClientProposalInput, LocalProposalId, Output};
use rafter_runtime::service::ServiceEvent;
use std::sync::atomic::Ordering;

impl<H, L, S, T, A, R: Send + 'static> DurableClientOwner<H, L, S, T, A, R>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
    A::Outcome: Clone,
{
    /// Advances the service and resolves clients only from durable outcomes.
    /// Existing transport/snapshot/application events are returned unchanged;
    /// matching client outcomes are cloned. Bound any events retained by the
    /// embedding and keep polling through `Progress` before sleeping.
    /// # Errors
    /// On a fatal service error, new admission is closed. Discard the owner
    /// before recovery; its destruction releases the backend and all waiters.
    /// # Panics
    /// Backend callbacks, application outcome cloning, or client wakers may
    /// panic. Even if caught, the owner and both client endpoints are then
    /// fenced and the backend must be discarded before recovery.
    pub fn poll_next(&mut self) -> Result<ServicePoll<T, A>, ServiceError> {
        self.poll_admitting(true)
    }

    // A managed control request needs to observe the end of its immediate
    // effects before a replenished client queue can start another operation.
    // Already-admitted backend work still completes through the same fences.
    pub(in crate::durable) fn poll_backend(&mut self) -> Result<ServicePoll<T, A>, ServiceError> {
        self.poll_admitting(false)
    }

    fn poll_admitting(&mut self, admit_clients: bool) -> Result<ServicePoll<T, A>, ServiceError> {
        if self.failed {
            return Err(ServiceError::Failed);
        }
        self.shared.discard_canceled_queued();
        if let Some(reads) = &self.reads {
            reads.shared.discard_canceled();
        }
        let mut fence = self.callback_fence();
        let Some(service) = self.service.as_mut() else {
            return Ok(ServicePoll::Pending);
        };
        self.failed = true;
        let polled = service.poll_next()?;
        if let ServicePoll::Event(event) = &polled {
            self.observe(event);
            self.observe_reads(event)?;
        }
        fence.armed = false;
        self.failed = false;
        if !admit_clients
            || !matches!(polled, ServicePoll::Pending)
            || self.state() != ServiceState::Ready
        {
            return Ok(polled);
        }
        self.prefer_reads = !self.prefer_reads;
        if !self.prefer_reads {
            if let Some(polled) = self.drive_read()? {
                return Ok(polled);
            }
        }
        let batch = match self.next_batch() {
            Ok(batch) => batch,
            Err(error) => {
                self.failed = true;
                self.close_clients();
                return Err(error);
            }
        };
        if batch.is_empty() {
            return Ok(if self.prefer_reads {
                self.drive_read()?.unwrap_or(ServicePoll::Pending)
            } else {
                ServicePoll::Pending
            });
        }
        let Some(service) = self.service.as_mut() else {
            self.failed = true;
            self.close_clients();
            return Err(ServiceError::Failed);
        };
        self.failed = true;
        fence.armed = true;
        let result = service.try_step(ServiceInput::Proposals(batch));
        match result {
            Ok(()) => {
                self.failed = false;
                fence.armed = false;
                Ok(ServicePoll::Progress)
            }
            Err(
                ServiceStepError::Unavailable { input, .. }
                | ServiceStepError::ProposalLimit { input },
            ) => {
                self.restore(input)?;
                self.failed = false;
                fence.armed = false;
                Ok(ServicePoll::Pending)
            }
            Err(ServiceStepError::Fatal(error)) => Err(error),
            Err(_) => Err(ServiceError::Failed),
        }
    }

    fn observe(&self, event: &ServiceEvent<T, A>) {
        match event {
            ServiceEvent::Output(Output::LocalProposalAppended {
                proposal_id,
                index,
                term,
            }) => {
                let mut state = lock(&self.shared.state);
                if let Some(entry) = state.entries.get_mut(proposal_id) {
                    entry.appended = Some((*index, *term));
                    state.indexes.insert(*index, *proposal_id);
                }
            }
            ServiceEvent::Output(Output::LocalProposalDropped { proposal_id, .. }) => {
                let options = lock(&self.shared.state)
                    .entries
                    .get(proposal_id)
                    .map(|e| e.options);
                if let Some(options) = options {
                    self.shared.finish(
                        *proposal_id,
                        Err(unknown(
                            *proposal_id,
                            options,
                            UnknownOutcomeReason::RuntimeDroppedProposal,
                        )),
                    );
                }
            }
            ServiceEvent::Output(Output::RejectProposal {
                proposal_id: Some(id),
                reason,
            }) => {
                self.shared.finish(
                    *id,
                    Err(WriteError::Rejected {
                        reason: reason.clone(),
                    }),
                );
            }
            ServiceEvent::Applied(done) => {
                for (entry, outcome) in done.entries().iter().zip(done.outcomes()) {
                    let index = entry.log_index();
                    let matched = {
                        let state = lock(&self.shared.state);
                        state.indexes.get(&index).and_then(|id| {
                            state
                                .entries
                                .get(id)
                                .and_then(|e| e.appended.map(|(_, term)| (*id, term)))
                        })
                    };
                    if let Some((id, term)) = matched {
                        self.shared.finish(
                            id,
                            Ok(WriteReceipt {
                                index,
                                term,
                                result: outcome.clone(),
                            }),
                        );
                    }
                }
            }
            ServiceEvent::ApplicationFailed(_) => self.close_clients(),
            _ => {}
        }
    }

    fn next_batch(&self) -> Result<Vec<ClientProposalInput>, ServiceError> {
        let mut discarded: Vec<Entry<A::Outcome>> = Vec::new();
        let mut batch = Vec::new();
        let mut bytes = 0_usize;
        {
            let mut state = lock(&self.shared.state);
            while batch.len() < self.options.max_proposals {
                let Some(id) = state.queue.front().copied() else {
                    break;
                };
                let entry = state.entries.get(&id).ok_or(ServiceError::Failed)?;
                if entry.reply.canceled.load(Ordering::Acquire) {
                    state.queue.pop_front();
                    discarded.extend(state.entries.remove(&id));
                    continue;
                }
                let length = entry.payload.as_ref().ok_or(ServiceError::Failed)?.len();
                if length > self.options.max_proposal_bytes - bytes {
                    break;
                }
                bytes += length;
                state.queue.pop_front();
                let payload = state
                    .entries
                    .get_mut(&id)
                    .ok_or(ServiceError::Failed)?
                    .payload
                    .take()
                    .ok_or(ServiceError::Failed)?;
                batch.push(ClientProposalInput {
                    proposal_id: Some(id),
                    payload,
                });
            }
        }
        drop(discarded);
        Ok(batch)
    }

    fn restore(&self, input: ServiceInput) -> Result<(), ServiceError> {
        let ServiceInput::Proposals(batch) = input else {
            return Err(ServiceError::Failed);
        };
        let mut state = lock(&self.shared.state);
        for proposal in batch.into_iter().rev() {
            let id: LocalProposalId = proposal.proposal_id.ok_or(ServiceError::Failed)?;
            state
                .entries
                .get_mut(&id)
                .ok_or(ServiceError::Failed)?
                .payload = Some(proposal.payload);
            state.queue.push_front(id);
        }
        Ok(())
    }
}
