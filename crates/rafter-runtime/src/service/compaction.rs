//! Quiescent application snapshot construction and streamed local publication.

use super::driver::Terminal;
use super::{DurableService, ServiceAdapterError, ServiceError, ServiceState};
use crate::application::{ApplicationEntry, ApplicationWorker, DurableApplication};
use crate::DurableRaftNode;
use rafter::{RaftSnapshot, RetiredLogEntries, SnapshotChunkSource};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

impl<H, L, S, T, A> DurableService<H, L, S, T, A>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Builds and publishes a local snapshot at the quiescent application floor.
    ///
    /// All earlier outputs and application completions must have been consumed.
    /// This joins the idle application worker, lends its store to `build`, then
    /// uses the existing streamed runtime compaction path. The callback returns
    /// an exact descriptor and an owned, stable chunk source. It may use the
    /// read-only runtime to derive metadata, but must not change application
    /// state or its durable floor. Floor changes are checked before publication.
    /// The snapshot must name exactly that floor; boundary term, membership, and
    /// author checks remain the runtime's authority.
    ///
    /// Building, streaming, and durable publication are synchronous maintenance.
    /// There is no full-payload buffer imposed by this API. The selected snapshot
    /// store determines its streaming memory behavior; the file-backed store uses
    /// bounded chunks, while an in-memory store retains the complete payload.
    /// The application must bound its own source creation and chunk allocations.
    /// Retired entries are returned for the existing bounded `LogRetirementWorker`.
    /// Successful publication restarts the application worker at the same floor.
    ///
    /// # Errors
    /// Admission/recovery/drain states and outstanding application work are
    /// refused without calling `build`. A mismatched descriptor floor or kernel
    /// boundary refusal writes nothing and restarts the unchanged application.
    /// Build failures, floor mutation, storage failures, panics, or worker-start
    /// failures fence this incarnation for recovery. Publication may already be
    /// durable if restarting the worker fails; reopen instead of blindly retrying.
    pub fn compact_snapshot_with<C: SnapshotChunkSource>(
        &mut self,
        build: impl FnOnce(
            &mut A,
            &DurableRaftNode<H, L, S>,
        ) -> Result<(RaftSnapshot, C), ServiceAdapterError>,
    ) -> Result<RetiredLogEntries, ServiceError> {
        let state = self.state();
        if state != ServiceState::Ready {
            return Err(ServiceError::NotReady(state));
        }
        if !self.idle() {
            return Err(ServiceError::NotQuiescent);
        }
        // Keep a caught callback/storage/source-drop panic terminal as well.
        self.terminal = Some(Terminal::Failed);
        let Some(mut worker) = self.application.take() else {
            return self.poison(ServiceError::ApplicationStopped);
        };
        let expected = worker.durable_through();
        let mut application = worker
            .shutdown_into_store()
            .map_err(ServiceError::ApplicationShutdown)?;
        let Some(node) = self.node.ready_node() else {
            return self.poison(ServiceError::OutputOrder);
        };
        let candidate = build(&mut application, node);
        let actual = application.applied_through();
        if actual != expected {
            return self.poison(ServiceError::ApplicationFloor { expected, actual });
        }
        let (snapshot, source) = candidate.map_err(ServiceError::SnapshotBuild)?;
        let actual = snapshot.metadata.last_included_index;
        let result = if actual == expected {
            let Some(node) = self.node.ready_node_mut() else {
                return self.poison(ServiceError::OutputOrder);
            };
            let result = node.compact_log_with_streamed_snapshot_deferred_drop(snapshot, &source);
            // Use the runtime's retained disposition, not a second error taxonomy.
            if node.fatal_error.is_some() {
                return result.map_err(ServiceError::Snapshot);
            }
            result.map_err(ServiceError::Snapshot)
        } else {
            Err(ServiceError::SnapshotFloor { expected, actual })
        };
        drop(source);
        let wake = std::sync::Arc::clone(&self.wake);
        let worker =
            ApplicationWorker::start(application, self.options.application, move || wake())
                .map_err(ServiceError::Start)?;
        self.application = Some(worker);
        self.terminal = None;
        result
    }
}
