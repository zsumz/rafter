//! Local maintenance preserves the same client lifetime and failure fence.
use super::{
    ApplicationEntry, DurableApplication, DurableClientOwner, DurableRaftNode, RaftHardStateStore,
    RaftLogSegment, RaftSnapshotStore, ServiceAdapterError, ServiceError, ServiceState,
    SnapshotChunkSource,
};
use rafter::{RaftSnapshot, RetiredLogEntries};

impl<H, L, S, T, A, R: Send + 'static> DurableClientOwner<H, L, S, T, A, R>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Builds and streams a snapshot at the quiescent durable application floor.
    ///
    /// All earlier backend outputs and application completions must have been
    /// consumed. Queued client requests and uncollected results retain their
    /// credits and identity across maintenance. The builder must preserve the
    /// application state and floor, and return a stable, bounded chunk source.
    /// Snapshot creation/publication and worker restart are synchronous. Return
    /// retired entries to a bounded retirement worker or dispose of them under
    /// the embedding's own memory/scheduling budget.
    ///
    /// # Errors
    /// Preserves the underlying service's quiescence and snapshot checks.
    /// Ordinary admission/descriptor-floor/boundary refusals keep clients live. A backend
    /// failure closes admission and requires discarding this incarnation.
    /// # Panics
    /// A caught builder, storage, or source-drop panic fences all clients;
    /// outstanding requests become terminal only after backend release.
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
        let mut fence = self.callback_fence();
        let service = self
            .service
            .as_mut()
            .ok_or(ServiceError::NotReady(ServiceState::Stopped))?;
        self.failed = true;
        let result = service.compact_snapshot_with(build);
        if service.state() != ServiceState::Failed {
            self.failed = false;
            fence.armed = false;
        }
        result
    }
}
