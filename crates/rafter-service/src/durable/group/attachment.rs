//! Failed attachment preserves both resources for retry or explicit disposal.
use rafter_runtime::service::ServiceState;

/// A backend cannot yet supply the initial managed metrics observation.
/// Neither the owner nor the configured transport is discarded on refusal.
pub struct DurableDriverAttachError<O, Tr> {
    owner: Box<O>,
    transport: Tr,
    state: ServiceState,
}
impl<O, Tr> DurableDriverAttachError<O, Tr> {
    pub(super) fn new(owner: O, transport: Tr, state: ServiceState) -> Self {
        Self {
            owner: Box::new(owner),
            transport,
            state,
        }
    }
    /// Backend state observed at the refused attachment.
    #[must_use]
    pub const fn state(&self) -> ServiceState {
        self.state
    }
    /// Returns the unchanged owner and transport for retry or disposal.
    #[must_use]
    pub fn into_parts(self) -> (O, Tr) {
        (*self.owner, self.transport)
    }
}
impl<O, Tr> std::fmt::Debug for DurableDriverAttachError<O, Tr> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableDriverAttachError")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
impl<O, Tr> std::fmt::Display for DurableDriverAttachError<O, Tr> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "durable driver attachment requires observable live state; current state: {:?}",
            self.state
        )
    }
}
impl<O, Tr> std::error::Error for DurableDriverAttachError<O, Tr> {}

use super::observe;
use super::{
    shared, ApplicationEntry, Arc, DurableApplication, DurableClientOwner, DurableMembershipLimits,
    DurableRaftDriver, MetricsPublisher, RaftHardStateStore, RaftLogSegment, RaftSnapshotStore,
    RaftTransport, SnapshotChunkSource,
};

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
    /// Attaches a group identity and an already configured transport.
    ///
    /// The original owner remains exclusive. The existing write/read clients
    /// continue to refer to that incarnation. Callers must use this driver for
    /// all later inputs and its handle for leadership transfer and shutdown.
    /// Membership inputs remain manually driven; use
    /// [`Self::attach_with_membership`] for managed proposal admission.
    ///
    /// # Errors
    /// Returns the unchanged owner and transport if no initial metrics snapshot can be
    /// observed, or if it has already begun shutdown or failed. Continue driving
    /// recovery/ownership handoff and retry attachment when state is observable.
    #[allow(
        clippy::type_complexity,
        reason = "attachment failure returns the concrete owner and transport without erasing their types"
    )]
    pub fn attach(
        group: G,
        owner: DurableClientOwner<H, L, S, T, A, R>,
        transport: Tr,
    ) -> Result<Self, DurableDriverAttachError<DurableClientOwner<H, L, S, T, A, R>, Tr>> {
        Self::attach_inner(group, owner, transport, None)
    }

    /// Attaches the existing transport with bounded managed membership admission.
    ///
    /// Raw membership inputs are refused after this attachment. Submit app-layer
    /// requests through the sender's `propose_membership` method. The embedding
    /// still owns peer authorization, policy updates and retirement fencing.
    /// Proposal receipts confirm admission, not committed membership changes.
    /// Membership and leadership transfer share one retained control credit.
    ///
    /// # Errors
    /// Returns the unchanged owner and transport under the same readiness and
    /// lifecycle conditions as [`Self::attach`].
    #[allow(
        clippy::type_complexity,
        reason = "attachment failure preserves the concrete owner and transport"
    )]
    pub fn attach_with_membership(
        group: G,
        owner: DurableClientOwner<H, L, S, T, A, R>,
        transport: Tr,
        limits: DurableMembershipLimits,
    ) -> Result<Self, DurableDriverAttachError<DurableClientOwner<H, L, S, T, A, R>, Tr>> {
        Self::attach_inner(group, owner, transport, Some(limits))
    }

    #[allow(
        clippy::type_complexity,
        reason = "attachment failure preserves the concrete owner and transport"
    )]
    fn attach_inner(
        group: G,
        owner: DurableClientOwner<H, L, S, T, A, R>,
        transport: Tr,
        membership_limits: Option<DurableMembershipLimits>,
    ) -> Result<Self, DurableDriverAttachError<DurableClientOwner<H, L, S, T, A, R>, Tr>> {
        let state = owner.state();
        if matches!(
            state,
            ServiceState::Failed | ServiceState::Draining | ServiceState::Stopped
        ) {
            return Err(DurableDriverAttachError::new(owner, transport, state));
        }
        let Some(metrics) = owner.metrics() else {
            return Err(DurableDriverAttachError::new(owner, transport, state));
        };
        let writes = owner.sender();
        let shared = Arc::new(shared::Shared {
            group: group.clone(),
            reads: owner.read_sender(),
            wake: Arc::clone(&writes.shared.wake),
            writes,
            metrics: MetricsPublisher::new(observe::grouped(metrics, group)),
            state: std::sync::Mutex::new(shared::State::default()),
            budget: Arc::new(crate::durable::budget::Budget::default()),
            membership_limits,
        });
        Ok(Self {
            owner: Some(owner),
            shared,
            transport,
            refused_sends: 0,
            failed: false,
        })
    }
}
