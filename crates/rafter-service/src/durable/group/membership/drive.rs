//! Correlate one proposal while preserving the original request for busy retry.
use super::{DurableMembershipError, DurableMembershipReceipt};
use crate::durable::group::{
    lock, ApplicationEntry, DurableApplication, DurableClientStepError, DurableRaftDriver,
    RaftHardStateStore, RaftLogSegment, RaftSnapshotStore, RaftTransport, ServiceError,
    ServiceStepError, SnapshotChunkSource,
};
use rafter::{ConfigurationProposalRejection, ProposalRejection};
use std::sync::atomic::Ordering;

impl<G, H, L, S, T, A, Tr, R> DurableRaftDriver<G, H, L, S, T, A, Tr, R>
where
    G: Clone + Eq,
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
    Tr: RaftTransport<G>,
    R: Send + 'static,
{
    pub(in crate::durable::group) fn observe_membership_rejection(
        &self,
        reason: &ProposalRejection,
    ) {
        if let Some(pending) = lock(&self.shared.state).membership.as_mut() {
            if pending.submitted {
                pending.rejection = Some(reason.clone());
            }
        }
    }

    pub(in crate::durable::group) fn start_membership(&mut self) -> Result<bool, ServiceError> {
        let (change, canceled) = {
            let mut state = lock(&self.shared.state);
            if let Some(pending) = state.membership.as_mut().filter(|m| !m.submitted) {
                if pending.reply.canceled.load(Ordering::Acquire) {
                    (None, state.membership.take())
                } else {
                    pending.submitted = true;
                    (pending.change.clone(), None)
                }
            } else {
                (None, None)
            }
        };
        drop(canceled);
        let Some(change) = change else {
            return Ok(false);
        };
        let owner = self.owner.as_mut().ok_or(ServiceError::Failed)?;
        let node = owner.ready_node().ok_or(ServiceError::Failed)?;
        let current = node.effective_membership();
        let leader_hint = node.leader_hint();
        let input = match change.into_raft_input(&current) {
            Ok(input) => input,
            Err(error) => {
                if let Some(pending) = lock(&self.shared.state).membership.as_mut() {
                    pending.change = None;
                    pending.leader_hint = leader_hint;
                    pending.rejection = Some(ProposalRejection::Configuration(
                        ConfigurationProposalRejection::InvalidMembership { error },
                    ));
                }
                return Ok(true);
            }
        };
        match owner.try_step(input) {
            Ok(()) => {
                // Membership consensus inputs use the synchronous persistence
                // fence. Capture this step's position before any later input.
                let node = owner.ready_node().ok_or(ServiceError::Failed)?;
                let receipt = DurableMembershipReceipt {
                    index: node.last_log_index(),
                    term: node.current_term(),
                };
                if let Some(pending) = lock(&self.shared.state).membership.as_mut() {
                    pending.change = None;
                    pending.receipt = Some(receipt);
                    pending.leader_hint = node.leader_hint();
                }
                Ok(true)
            }
            Err(DurableClientStepError::Service(ServiceStepError::Unavailable { .. })) => {
                if let Some(pending) = lock(&self.shared.state).membership.as_mut() {
                    pending.submitted = false;
                }
                Ok(false)
            }
            Err(DurableClientStepError::Service(ServiceStepError::Fatal(error))) => Err(error),
            Err(_) => Err(ServiceError::Failed),
        }
    }

    pub(in crate::durable::group) fn finish_membership(&mut self) -> Result<bool, ServiceError> {
        let pending = {
            let mut state = lock(&self.shared.state);
            if state.membership.as_ref().is_some_and(|m| m.submitted) {
                state.membership.take()
            } else {
                None
            }
        };
        let Some(mut pending) = pending else {
            return Ok(false);
        };
        let result = if let Some(reason) = pending.rejection.take() {
            Err(DurableMembershipError::Rejected {
                reason,
                leader_hint: pending.leader_hint,
            })
        } else if let Some(receipt) = pending.receipt {
            Ok(receipt)
        } else {
            // Preserve ownership until backend release even if this internal
            // correlation invariant fails; never orphan the caller's future.
            lock(&self.shared.state).membership = Some(pending);
            return Err(ServiceError::Failed);
        };
        pending.reply.finish(result);
        Ok(true)
    }
}
