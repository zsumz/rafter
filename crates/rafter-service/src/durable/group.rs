//! A managed handle and the existing transport around one durable incarnation.
use super::{
    lock, DurableClientOwner, DurableClientStepError, DurableReadSender, DurableWriteSender,
};
use crate::{MetricsPublisher, RaftHandle, RaftTransport};
use rafter::{Input, LogIndex, SnapshotChunkSource};
use rafter_runtime::{
    application::{ApplicationEntry, DurableApplication},
    service::{ServiceError, ServicePoll, ServiceState, ServiceStepError},
    DurableRaftNode,
};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};
use std::sync::Arc;
mod attachment;
mod compaction;
mod drive;
mod lifecycle;
mod membership;
mod observe;
mod sender;
mod shared;
pub use attachment::DurableDriverAttachError;
pub use membership::{DurableMembershipError, DurableMembershipLimits, DurableMembershipReceipt};
pub use sender::DurableCommandSender;

/// A managed encoded-command/query handle for one durable group.
pub type DurableRaftHandle<G, O, R = ()> =
    RaftHandle<G, Vec<u8>, Vec<u8>, O, R, DurableCommandSender<G, O, R>>;

/// Uniquely owns the durable backend while cloneable handles submit commands.
///
/// Existing transport sends are attempted synchronously; failures are counted
/// as loss and retried by Raft. The embedding still owns inbound authentication,
/// membership policy, snapshot installation, link lifecycle, and event bounds.
/// Drive through `Progress` before awaiting the original owner's wake callback.
/// A fatal error or caught panic requires discarding this incarnation.
pub struct DurableRaftDriver<
    G,
    H,
    L,
    S,
    T: ApplicationEntry,
    A: DurableApplication<T>,
    Tr,
    R: Send + 'static = (),
> {
    owner: Option<DurableClientOwner<H, L, S, T, A, R>>,
    shared: Arc<shared::Shared<G, A::Outcome, R>>,
    transport: Tr,
    refused_sends: u64,
    failed: bool,
}

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
    /// Returns the group-validating endpoint without retaining the backend.
    #[must_use]
    pub fn sender(&self) -> DurableCommandSender<G, A::Outcome, R> {
        DurableCommandSender {
            shared: Arc::clone(&self.shared),
        }
    }
    /// Creates the existing public managed-handle surface for this group.
    #[must_use]
    pub fn handle(&self) -> DurableRaftHandle<G, A::Outcome, R> {
        RaftHandle::new(self.shared.group.clone(), self.sender())
    }
    /// Returns current backend readiness, or `Stopped` after backend release.
    #[must_use]
    pub fn state(&self) -> ServiceState {
        if self.failed {
            ServiceState::Failed
        } else {
            self.owner
                .as_ref()
                .map_or(ServiceState::Stopped, DurableClientOwner::state)
        }
    }
    /// Inspects the node or snapshot source when the worker does not own it.
    #[must_use]
    pub fn ready_node(&self) -> Option<&DurableRaftNode<H, L, S>> {
        self.owner.as_ref().and_then(DurableClientOwner::ready_node)
    }
    /// Observes application durability when that store is still owned.
    #[must_use]
    pub fn durable_through(&self) -> Option<LogIndex> {
        self.owner
            .as_ref()
            .and_then(DurableClientOwner::durable_through)
    }
    /// Counts transport refusals; success at this boundary means enqueue only.
    #[must_use]
    pub const fn refused_sends(&self) -> u64 {
        self.refused_sends
    }
    /// Admits clocks and already authenticated/group-validated peer messages.
    /// Membership authorization remains the embedding's responsibility.
    /// # Errors
    /// Preserves owner refusals; raw leadership transfer must use the handle.
    /// With managed membership enabled, membership inputs must use the sender.
    /// # Panics
    /// A caught backend or metrics callback panic fences admission.
    pub fn try_step(&mut self, input: Input) -> Result<(), DurableClientStepError> {
        if matches!(input, Input::TransferLeadership { .. }) {
            return Err(DurableClientStepError::UnmanagedTransfer(Box::new(input)));
        }
        if self.shared.membership_limits.is_some()
            && matches!(
                input,
                Input::AddLearner { .. }
                    | Input::PromoteLearner { .. }
                    | Input::RemoveVoter { .. }
                    | Input::EnterJoint { .. }
                    | Input::LeaveJoint
                    | Input::ChangeMembership { .. }
                    | Input::DangerousRawConfigurationProposal { .. }
            )
        {
            return Err(DurableClientStepError::UnmanagedMembership(Box::new(input)));
        }
        if self.failed {
            return Err(DurableClientStepError::Service(ServiceStepError::Fatal(
                ServiceError::Failed,
            )));
        }
        let mut fence = lifecycle::Fence::new(&self.shared);
        self.failed = true;
        let result = match self.owner.as_mut() {
            Some(owner) => {
                if lock(&self.shared.state).shutdown {
                    owner.begin_shutdown();
                }
                owner.try_step(input)
            }
            None => Err(DurableClientStepError::Service(
                ServiceStepError::Unavailable {
                    state: ServiceState::Stopped,
                    input: input.into(),
                },
            )),
        };
        if matches!(
            result,
            Err(DurableClientStepError::Service(ServiceStepError::Fatal(_)))
        ) {
            self.publish_failure();
        } else {
            self.publish_metrics();
            self.failed = false;
            fence.armed = false;
        }
        result
    }
    /// Returns a durably installed application after a snapshot handoff.
    /// # Errors
    /// Preserves exact owner floor checks; errors from a live owner require
    /// discarding the driver. A late return after backend release reports
    /// `NotReady(Stopped)` and preserves the completed shutdown state.
    /// # Panics
    /// A caught application or metrics callback panic fences admission.
    pub fn resume_snapshot(&mut self, application: A) -> Result<(), ServiceError> {
        if self.failed {
            return Err(ServiceError::Failed);
        }
        if self.owner.is_none() {
            return Err(ServiceError::NotReady(ServiceState::Stopped));
        }
        let mut fence = lifecycle::Fence::new(&self.shared);
        self.failed = true;
        let result = self
            .owner
            .as_mut()
            .ok_or(ServiceError::NotReady(ServiceState::Stopped))?
            .resume_snapshot(application);
        if result.is_ok() {
            self.publish_metrics();
            self.failed = false;
            fence.armed = false;
        } else {
            self.publish_failure();
        }
        result
    }
}
impl<G, H, L, S, T: ApplicationEntry, A: DurableApplication<T>, Tr, R: Send + 'static>
    std::fmt::Debug for DurableRaftDriver<G, H, L, S, T, A, Tr, R>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableRaftDriver")
            .field("failed", &self.failed)
            .field("refused_sends", &self.refused_sends)
            .finish_non_exhaustive()
    }
}
