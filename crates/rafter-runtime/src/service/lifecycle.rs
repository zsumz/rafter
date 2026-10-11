//! Snapshot handoff and completion-preserving shutdown.

use super::driver::Terminal;
use super::{DurableService, ServiceError, ServiceEvent, ServiceState};
use crate::application::{ApplicationEntry, ApplicationWorker, DurableApplication};
use rafter::{Output, RaftSnapshot, SnapshotChunkSource};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

impl<H, L, S, T, A> DurableService<H, L, S, T, A>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    pub(super) fn handoff_snapshot(
        &mut self,
        snapshot: RaftSnapshot,
    ) -> Result<Option<ServiceEvent<T, A>>, ServiceError> {
        let Some(application) = self.application.as_mut() else {
            return self.poison(ServiceError::ApplicationStopped);
        };
        if application.is_busy() {
            self.outputs.push_front(Output::ApplySnapshot { snapshot });
            return Ok(None);
        }
        let application = match application.shutdown_into_store() {
            Ok(application) => application,
            Err(error) => return self.poison(ServiceError::ApplicationShutdown(error)),
        };
        self.application = None;
        self.snapshot_floor = Some(snapshot.metadata.last_included_index);
        Ok(Some(ServiceEvent::InstallSnapshot {
            snapshot,
            application,
        }))
    }

    /// Returns the store after atomically installing the handed-off snapshot.
    ///
    /// The embedding must validate and durably install the descriptor's exact
    /// bytes and its floor as one crash-consistency unit. Only the floor can be
    /// verified here. Snapshot failure should abandon this incarnation and
    /// recover both stores; do not return a partially installed live store.
    ///
    /// # Errors
    /// A missing handoff, mismatched floor, or worker-start failure consumes the
    /// supplied store and terminally fences the service; recover from storage.
    pub fn resume_snapshot(&mut self, application: A) -> Result<(), ServiceError> {
        if self.terminal == Some(Terminal::Failed) {
            return Err(ServiceError::Failed);
        }
        let Some(expected) = self.snapshot_floor else {
            return self.poison(ServiceError::NotReady(self.state()));
        };
        let actual = application.applied_through();
        if actual != expected {
            return self.poison(ServiceError::ApplicationFloor { expected, actual });
        }
        let worker = match ApplicationWorker::start(application, self.options.application, || {}) {
            Ok(worker) => worker,
            Err(error) => return self.poison(ServiceError::Start(error)),
        };
        self.application = Some(worker);
        self.dispatched = expected;
        self.snapshot_floor = None;
        Ok(())
    }

    /// Closes admission immediately. Continue polling all pending events, and
    /// complete any snapshot handoff, before calling `finish_shutdown`.
    pub fn begin_shutdown(&mut self) {
        self.draining = true;
    }

    /// Joins idle workers and returns the application store exactly once.
    ///
    /// # Errors
    /// Refuses without waiting while outputs, persistence, application work, or
    /// completions remain. A join failure poisons the service. `NotReady` is a
    /// lifecycle refusal and does not poison it. Joining idle threads may wait
    /// for scheduling but never for a submitted durable operation.
    pub fn finish_shutdown(&mut self) -> Result<A, ServiceError> {
        if self.terminal == Some(Terminal::Failed) {
            return Err(ServiceError::Failed);
        }
        if !self.draining
            || self.terminal == Some(Terminal::Stopped)
            || self.snapshot_floor.is_some()
            || !self.idle()
        {
            return Err(ServiceError::NotReady(self.state()));
        }
        if let Err(error) = self.node.shutdown_worker() {
            return self.poison(ServiceError::PersistenceShutdown(error));
        }
        let Some(application) = self.application.as_mut() else {
            return self.poison(ServiceError::ApplicationStopped);
        };
        let application = match application.shutdown_into_store() {
            Ok(application) => application,
            Err(error) => return self.poison(ServiceError::ApplicationShutdown(error)),
        };
        self.application = None;
        self.terminal = Some(Terminal::Stopped);
        self.recovering = false;
        debug_assert_eq!(self.state(), ServiceState::Stopped);
        Ok(application)
    }
}
