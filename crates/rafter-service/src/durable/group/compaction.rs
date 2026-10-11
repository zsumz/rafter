//! Managed maintenance honors shutdown requested through cloned handles.
use super::{
    lifecycle, lock, ApplicationEntry, DurableApplication, DurableRaftDriver, DurableRaftNode,
    RaftHardStateStore, RaftLogSegment, RaftSnapshotStore, RaftTransport, ServiceError,
    ServiceState, SnapshotChunkSource,
};
use rafter::{RaftSnapshot, RetiredLogEntries};
use rafter_runtime::service::ServiceAdapterError;

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
    /// Builds and streams a snapshot through the quiescent durable owner.
    ///
    /// The builder must preserve application state and its verified floor.
    /// Creation, publication, and worker restart are synchronous maintenance;
    /// bound snapshot staging/chunks and disposal of the returned retired entries.
    /// Existing clients retain their admission credits across the operation.
    /// # Errors
    /// Preserves owner refusals, including shutdown requested by any handle.
    /// Fatal failures close admission and require discarding the driver.
    /// # Panics
    /// A caught maintenance or metrics callback panic fences the driver.
    pub fn compact_snapshot_with<C: SnapshotChunkSource>(
        &mut self,
        build: impl FnOnce(
            &mut A,
            &DurableRaftNode<H, L, S>,
        ) -> Result<(RaftSnapshot, C), ServiceAdapterError>,
    ) -> Result<RetiredLogEntries, ServiceError> {
        if self.failed {
            return Err(ServiceError::Failed);
        }
        let mut fence = lifecycle::Fence::new(&self.shared);
        let owner = self
            .owner
            .as_mut()
            .ok_or(ServiceError::NotReady(ServiceState::Stopped))?;
        self.failed = true;
        if lock(&self.shared.state).shutdown {
            owner.begin_shutdown();
        }
        let result = owner.compact_snapshot_with(build);
        if owner.state() == ServiceState::Failed {
            self.publish_failure();
        } else {
            self.publish_metrics();
            self.failed = false;
            fence.armed = false;
        }
        result
    }
}
