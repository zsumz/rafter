//! Route immediate effects before resolving a managed transfer request.
use super::{
    lifecycle, lock, ApplicationEntry, DurableApplication, DurableClientStepError,
    DurableRaftDriver, DurableRaftNode, Input, RaftHardStateStore, RaftLogSegment,
    RaftSnapshotStore, RaftTransport, ServiceError, ServicePoll, ServiceState, ServiceStepError,
    SnapshotChunkSource,
};
use crate::{PeerEnvelope, SnapshotChunkEnvelope, TransferLeadershipError};
use rafter::Output;
use rafter_runtime::service::ServiceEvent;
use std::sync::atomic::Ordering;

impl<G, H, L, S, T, A, Tr, R> DurableRaftDriver<G, H, L, S, T, A, Tr, R>
where
    G: Clone + Eq,
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
    A::Outcome: Clone,
    Tr: RaftTransport<G>,
    R: Send + 'static,
{
    /// Advances a bounded service turn, routing peer outputs to the transport.
    ///
    /// Other events are returned unchanged for application/snapshot handling.
    /// `Progress` includes a routed send, so callers must continue polling before
    /// waiting. Transport refusals are counted as Raft message loss. A transfer
    /// resolves after its immediate effects drain, not after another election.
    /// Queued clients receive an admission turn after each completed transfer,
    /// even when another transfer is already queued.
    /// Requested shutdown drains work and releases the backend before completing
    /// shutdown futures or closing metrics; duplicate shutdown calls also wait.
    /// # Errors
    /// A fatal backend error closes admission; discard this driver and recover.
    /// # Panics
    /// A caught backend, transport, or subscriber panic fences the driver.
    pub fn poll_next(&mut self) -> Result<ServicePoll<T, A>, ServiceError> {
        if self.failed {
            self.publish_failure();
            return Err(ServiceError::Failed);
        }
        let mut fence = lifecycle::Fence::new(&self.shared);
        self.failed = true;
        let result = self.drive();
        match &result {
            Ok(_)
                if self
                    .owner
                    .as_ref()
                    .is_none_or(|o| o.state() != ServiceState::Failed) =>
            {
                self.publish_metrics();
                self.failed = false;
                fence.armed = false;
            }
            _ => self.publish_failure(),
        }
        result
    }
    fn drive(&mut self) -> Result<ServicePoll<T, A>, ServiceError> {
        let Some(owner) = self.owner.as_mut() else {
            return Ok(ServicePoll::Pending);
        };
        let shutdown = lock(&self.shared.state).shutdown;
        if shutdown {
            owner.begin_shutdown();
        } else if owner.state() == ServiceState::Ready && self.start_transfer()? {
            return Ok(ServicePoll::Progress);
        }
        let Some(owner) = self.owner.as_mut() else {
            return Err(ServiceError::Failed);
        };
        let transfer_pending = lock(&self.shared.state)
            .transfer
            .as_ref()
            .is_some_and(|transfer| transfer.submitted);
        let mut polled = if transfer_pending {
            owner.poll_backend()?
        } else {
            owner.poll_next()?
        };
        let finished_transfer = !shutdown
            && matches!(polled, ServicePoll::Pending)
            && self
                .owner
                .as_ref()
                .is_some_and(|owner| owner.state() == ServiceState::Ready)
            && self.finish_transfer();
        if finished_transfer {
            // The transfer's immediate effects are drained. Give clients one
            // bounded turn before dispatching another request, including one queued
            // by a completion callback. Route events through the same path below.
            polled = self
                .owner
                .as_mut()
                .ok_or(ServiceError::Failed)?
                .poll_next()?;
        }
        match polled {
            ServicePoll::Event(ServiceEvent::Output(Output::Send { to, message })) => {
                let from = self.shared.metrics.current().node_id;
                if self
                    .transport
                    .send(PeerEnvelope {
                        group_id: self.shared.group.clone(),
                        from,
                        to,
                        message,
                    })
                    .is_err()
                {
                    self.refused_sends = self.refused_sends.saturating_add(1);
                }
                Ok(ServicePoll::Progress)
            }
            ServicePoll::Event(ServiceEvent::Output(Output::SendSnapshotChunk { to, chunk })) => {
                let from = self.shared.metrics.current().node_id;
                if self
                    .transport
                    .send_snapshot_chunk(SnapshotChunkEnvelope {
                        group_id: self.shared.group.clone(),
                        from,
                        to,
                        chunk,
                    })
                    .is_err()
                {
                    self.refused_sends = self.refused_sends.saturating_add(1);
                }
                Ok(ServicePoll::Progress)
            }
            ServicePoll::Event(ServiceEvent::Output(Output::LeadershipTransferRejected {
                target,
                reason,
            })) => {
                if let Some(transfer) = lock(&self.shared.state).transfer.as_mut() {
                    if transfer.submitted && transfer.target == target {
                        transfer.rejection = Some(reason);
                    }
                }
                Ok(ServicePoll::Event(ServiceEvent::Output(
                    Output::LeadershipTransferRejected { target, reason },
                )))
            }
            ServicePoll::Pending if shutdown => self.drain_shutdown(),
            ServicePoll::Pending if finished_transfer => Ok(ServicePoll::Progress),
            other => Ok(other),
        }
    }
    fn finish_transfer(&mut self) -> bool {
        let transfer = {
            let mut state = lock(&self.shared.state);
            if state.transfer.as_ref().is_some_and(|t| t.submitted) {
                state.transfer.take()
            } else {
                None
            }
        };
        let Some(transfer) = transfer else {
            return false;
        };
        let result = transfer.rejection.map_or(Ok(()), |reason| {
            Err(TransferLeadershipError::Rejected {
                reason,
                leader_hint: self.ready_node().and_then(DurableRaftNode::leader_hint),
            })
        });
        transfer.reply.finish(result);
        true
    }
    fn start_transfer(&mut self) -> Result<bool, ServiceError> {
        let (target, canceled) = {
            let mut state = lock(&self.shared.state);
            if let Some(transfer) = state.transfer.as_mut().filter(|t| !t.submitted) {
                if transfer.reply.canceled.load(Ordering::Acquire) {
                    (None, state.transfer.take())
                } else {
                    transfer.submitted = true;
                    (Some(transfer.target), None)
                }
            } else {
                (None, None)
            }
        };
        drop(canceled);
        let Some(target) = target else {
            // Discarding a queued cancellation must still give clients a turn;
            // a stream of canceled controls performs no backend work to drain.
            return Ok(false);
        };
        let owner = self.owner.as_mut().ok_or(ServiceError::Failed)?;
        match owner.try_step(Input::TransferLeadership { target }) {
            Ok(()) => Ok(true),
            Err(DurableClientStepError::Service(ServiceStepError::Unavailable { .. })) => {
                if let Some(transfer) = lock(&self.shared.state).transfer.as_mut() {
                    transfer.submitted = false;
                }
                Ok(false)
            }
            Err(DurableClientStepError::Service(ServiceStepError::Fatal(error))) => Err(error),
            Err(_) => Err(ServiceError::Failed),
        }
    }
    fn drain_shutdown(&mut self) -> Result<ServicePoll<T, A>, ServiceError> {
        self.publish_metrics();
        match self
            .owner
            .as_mut()
            .ok_or(ServiceError::Failed)?
            .finish_shutdown()
        {
            Ok(application) => {
                lifecycle::release(&self.shared, self.owner.take(), application);
                Ok(ServicePoll::Progress)
            }
            Err(ServiceError::NotReady(_)) => Ok(ServicePoll::Pending),
            Err(error) => Err(error),
        }
    }
}
