//! Completion consumption and bounded worker observation.

use super::{ApplicationWorker, ApplicationWorkerStopped, WorkerEvent};
use crate::application::{ApplicationEntry, DurableApplication};
use rafter::LogIndex;
use std::sync::{atomic::Ordering, mpsc::TryRecvError};

impl<T, S> ApplicationWorker<T, S>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
{
    /// Polls one ordered application event and releases all of its credits.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationWorkerStopped`] once the thread has ended and no
    /// event remains. A failed event is data, not this transport-level error.
    pub fn try_complete(&self) -> Result<Option<WorkerEvent<T, S>>, ApplicationWorkerStopped> {
        match self.events.try_recv() {
            Ok(event) => {
                self.release(&event);
                Ok(Some(event))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                self.release_stopped();
                Err(ApplicationWorkerStopped)
            }
        }
    }

    /// Waits for one ordered application event and releases its credits.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationWorkerStopped`] when the worker ended without an
    /// event. A failed application operation is returned as an event.
    pub fn complete(&self) -> Result<WorkerEvent<T, S>, ApplicationWorkerStopped> {
        let event = self.events.recv().map_err(|_| {
            self.release_stopped();
            ApplicationWorkerStopped
        })?;
        self.release(&event);
        Ok(event)
    }

    /// Returns the latest floor reported durable after a verified batch.
    #[must_use]
    pub fn durable_through(&self) -> LogIndex {
        LogIndex(self.durable_through.load(Ordering::Acquire))
    }

    /// Returns the final index of the batch currently inside `apply`, or zero.
    #[must_use]
    pub fn applying_through(&self) -> LogIndex {
        LogIndex(self.applying_through.load(Ordering::Acquire))
    }

    /// Returns the entry and retained-byte credits currently available.
    #[must_use]
    pub fn available(&self) -> (usize, usize) {
        let shared = self.lock_shared();
        (
            self.options
                .inflight_entries
                .saturating_sub(shared.inflight_entries),
            self.options
                .inflight_bytes
                .saturating_sub(shared.inflight_bytes),
        )
    }

    /// Returns whether entries are queued, executing, or awaiting consumption.
    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.lock_shared().inflight_entries != 0
    }

    /// Returns whether the worker can accept another otherwise-valid batch.
    #[must_use]
    pub fn is_accepting(&self) -> bool {
        self.lock_shared().accepting
    }

    fn release(&self, event: &WorkerEvent<T, S>) {
        let mut shared = self.lock_shared();
        shared.inflight_entries -= event.entry_count();
        shared.inflight_bytes -= event.retained_bytes();
    }

    fn release_stopped(&self) {
        let mut shared = self.lock_shared();
        shared.accepting = false;
        shared.inflight_entries = 0;
        shared.inflight_bytes = 0;
    }
}
