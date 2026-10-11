//! The backend remains uniquely owned until all terminal client decisions.

use super::{shared::Shared, DurableWriteLimits, DurableWriteSender};
use rafter::{Input, LogIndex, NodeConfig, SnapshotChunkSource};
use rafter_runtime::{
    application::{ApplicationEntry, DurableApplication},
    service::{
        CommittedCommand, DurableService, ServiceAdapterError, ServiceError, ServiceInput,
        ServiceOptions, ServicePoll, ServiceState, ServiceStepError,
    },
    DurableRaftNode, DurableRaftNodeStorage,
};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};
use std::{fmt, sync::Arc};

mod dispatch;

/// Owns the durable backend and its bounded write clients together.
///
/// Construct a fresh incarnation through `recover`; there is no adoption of a
/// running backend whose local proposal IDs could collide. Cloned senders do
/// not keep the backend alive. On drop, workers and protocol state are released
/// before the remaining waiters receive terminal outcomes.
pub struct DurableClientOwner<H, L, S, T: ApplicationEntry, A: DurableApplication<T>> {
    service: Option<DurableService<H, L, S, T, A>>,
    shared: Arc<Shared<A::Outcome>>,
    options: ServiceOptions,
    failed: bool,
}

impl<H, L, S, T, A> DurableClientOwner<H, L, S, T, A>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Recovers a fresh owner and installs the same notification for clients
    /// and workers. The callback must be bounded, nonblocking, non-panicking,
    /// and must not reenter this owner. Application adaptation retains the
    /// underlying service's range, semantics, and memory-estimate obligations.
    /// # Errors
    /// Consumes the stores and returns the underlying recovery/start error.
    pub fn recover(
        config: NodeConfig,
        storage: DurableRaftNodeStorage<H, L, S>,
        application: A,
        options: ServiceOptions,
        limits: DurableWriteLimits,
        adapter: impl FnMut(CommittedCommand) -> Result<T, ServiceAdapterError> + Send + 'static,
        wake: impl Fn() + Send + Sync + 'static,
    ) -> Result<Self, ServiceError> {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(wake);
        let shared = Shared::new(limits, options.max_proposal_bytes, Arc::clone(&wake));
        let service = DurableService::recover_with_wake(
            config,
            storage,
            application,
            options,
            adapter,
            move || wake(),
        )?;
        Ok(Self {
            service: Some(service),
            shared,
            options,
            failed: false,
        })
    }

    /// Returns a cloneable encoded-write endpoint for this incarnation.
    #[must_use]
    pub fn sender(&self) -> DurableWriteSender<A::Outcome> {
        DurableWriteSender {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Returns current backend readiness, or `Stopped` after shutdown.
    #[must_use]
    pub fn state(&self) -> ServiceState {
        if self.failed {
            return ServiceState::Failed;
        }
        self.service
            .as_ref()
            .map_or(ServiceState::Stopped, DurableService::state)
    }

    /// Inspects the runtime or reads stored snapshot chunks when available.
    #[must_use]
    pub fn ready_node(&self) -> Option<&DurableRaftNode<H, L, S>> {
        self.service.as_ref().and_then(DurableService::ready_node)
    }

    /// Returns the verified application durable floor when its store is owned.
    #[must_use]
    pub fn durable_through(&self) -> Option<LogIndex> {
        self.service
            .as_ref()
            .and_then(DurableService::durable_through)
    }

    /// Admits clock, authenticated peer, and control inputs under the existing
    /// service durability contract. Peer authentication and group routing stay
    /// with the embedding. Use the sender for all local proposals.
    /// # Errors
    /// Returns unmanaged proposal inputs unchanged, or the service's refusal
    /// or fatal error. On a fatal error, discard this owner and recover.
    pub fn try_step(&mut self, input: Input) -> Result<(), DurableClientStepError> {
        if self.failed {
            return Err(DurableClientStepError::Service(ServiceStepError::Fatal(
                ServiceError::Failed,
            )));
        }
        if matches!(
            input,
            Input::ClientProposal { .. } | Input::TrackedClientProposal { .. }
        ) {
            return Err(DurableClientStepError::UnmanagedProposal(Box::new(input)));
        }
        let input = ServiceInput::from(input);
        let Some(service) = self.service.as_mut() else {
            return Err(DurableClientStepError::Service(
                ServiceStepError::Unavailable {
                    state: ServiceState::Stopped,
                    input,
                },
            ));
        };
        let result = service.try_step(input);
        if matches!(result, Err(ServiceStepError::Fatal(_))) {
            self.shared.close();
        }
        result.map_err(DurableClientStepError::Service)
    }

    /// Returns an installed application store after an `InstallSnapshot` event.
    /// # Errors
    /// Preserves the underlying service's exact snapshot-floor checks.
    pub fn resume_snapshot(&mut self, application: A) -> Result<(), ServiceError> {
        self.service
            .as_mut()
            .ok_or(ServiceError::NotReady(ServiceState::Stopped))?
            .resume_snapshot(application)
    }

    /// Closes new client and consensus admission. Poll existing work and finish
    /// any snapshot handoff before attempting `finish_shutdown`.
    pub fn begin_shutdown(&mut self) {
        self.shared.close();
        if let Some(service) = self.service.as_mut() {
            service.begin_shutdown();
        }
    }

    /// Joins drained workers, releases the backend, and resolves any remaining
    /// writes with proven refusal or unknown outcome before returning the store.
    /// # Errors
    /// Returns `NotReady` while owned work remains; continue polling. Fatal
    /// failures require discarding this owner and reopening the stores.
    pub fn finish_shutdown(&mut self) -> Result<A, ServiceError> {
        if self.failed {
            return Err(ServiceError::Failed);
        }
        let application = self
            .service
            .as_mut()
            .ok_or(ServiceError::NotReady(ServiceState::Stopped))?
            .finish_shutdown()?;
        self.shared.close();
        drop(self.service.take());
        self.shared.terminate();
        Ok(application)
    }
}

impl<H, L, S, T, A> Drop for DurableClientOwner<H, L, S, T, A>
where
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    fn drop(&mut self) {
        self.shared.close();
        let finish = Finish(Arc::clone(&self.shared));
        drop(self.service.take());
        drop(finish);
    }
}
struct Finish<O>(Arc<Shared<O>>);
impl<O> Drop for Finish<O> {
    fn drop(&mut self) {
        self.0.terminate();
    }
}

impl<H, L, S, T, A> fmt::Debug for DurableClientOwner<H, L, S, T, A>
where
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableClientOwner")
            .field("retained", &self.shared.stats())
            .finish_non_exhaustive()
    }
}

/// A caller bypassed managed proposal admission, or the backend refused input.
#[derive(Debug)]
#[non_exhaustive]
pub enum DurableClientStepError {
    /// Use the sender; raw proposals could collide with this owner's IDs.
    UnmanagedProposal(Box<Input>),
    /// The underlying service's unchanged refusal or fatal failure.
    Service(ServiceStepError),
}
impl fmt::Display for DurableClientStepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable client input: {self:?}")
    }
}
impl std::error::Error for DurableClientStepError {}
