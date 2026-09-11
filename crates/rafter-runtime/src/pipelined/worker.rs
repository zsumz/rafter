//! One-credit persistence executor for a pipelined durable node.
//!
//! The worker owns no consensus policy. It moves one [`PersistenceWork`] onto
//! one I/O thread and returns the resulting [`PersistenceCompletion`] to the
//! node owner, which still has to pass it to the originating
//! [`super::PipelinedRaftNode`]. The credit covers queued, executing, and
//! completed-but-unconsumed work, so a caller cannot accidentally place two
//! durable nodes behind one completion slot.

use super::{PersistenceCompletion, PersistenceWork};
use rafter::SnapshotChunkSource;
use rafter_storage::{
    telemetry::{self, Metric},
    RaftHardStateStore, RaftLogSegment, RaftSnapshotStore,
};
use std::{
    error::Error,
    fmt, io,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    },
    thread::{self, JoinHandle},
};

/// Cumulative named storage counters returned with a worker completion.
pub type PersistenceWorkerTelemetry = Vec<(&'static str, Metric)>;

/// A nonblocking submission refusal that returns its owned persistence work.
pub type PersistenceWorkerSubmitError<H, L, S> = TrySendError<Box<PersistenceWork<H, L, S>>>;

/// Options for one bounded persistence worker.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct PersistenceWorkerOptions {
    collect_storage_telemetry: bool,
}

impl PersistenceWorkerOptions {
    /// Returns the shipped options: telemetry disabled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            collect_storage_telemetry: false,
        }
    }

    /// Enables the storage crate's per-thread diagnostic counters.
    ///
    /// Leave this disabled for timing runs. The counters are returned beside
    /// each completion and cover the worker thread cumulatively.
    #[must_use]
    pub const fn with_storage_telemetry(mut self, enabled: bool) -> Self {
        self.collect_storage_telemetry = enabled;
        self
    }
}

/// One completed persistence operation and optional worker-thread telemetry.
#[derive(Debug)]
pub struct PersistenceWorkerCompletion<H, L, S> {
    completion: PersistenceCompletion<H, L, S>,
    storage_telemetry: PersistenceWorkerTelemetry,
}

impl<H, L, S> PersistenceWorkerCompletion<H, L, S> {
    /// Returns the identity-bearing completion accepted only by its owner.
    #[must_use]
    pub const fn completion(&self) -> &PersistenceCompletion<H, L, S> {
        &self.completion
    }

    /// Returns cumulative persistence counters from the worker thread.
    #[must_use]
    pub fn storage_telemetry(&self) -> &[(&'static str, Metric)] {
        &self.storage_telemetry
    }

    /// Splits the owned completion from its diagnostic counters.
    #[must_use]
    pub fn into_parts(self) -> (PersistenceCompletion<H, L, S>, PersistenceWorkerTelemetry) {
        (self.completion, self.storage_telemetry)
    }
}

/// The worker stopped before returning an outstanding completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistenceWorkerStopped;

impl fmt::Display for PersistenceWorkerStopped {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("persistence worker stopped before completion")
    }
}

impl Error for PersistenceWorkerStopped {}

/// Why an explicit worker shutdown was refused or failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PersistenceWorkerShutdownError {
    /// Queued, executing, or unconsumed work still owns a durable node.
    Busy,
    /// The persistence thread panicked.
    Panicked,
}

impl fmt::Display for PersistenceWorkerShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => formatter.write_str("persistence worker still owns an operation"),
            Self::Panicked => formatter.write_str("persistence worker thread panicked"),
        }
    }
}

impl Error for PersistenceWorkerShutdownError {}

/// One-thread, one-credit executor for [`PersistenceWork`].
///
/// The worker is intentionally narrower than a general thread pool. A
/// successful [`PersistenceWorker::try_submit`] reserves its only credit until
/// the caller consumes the completion. A full or disconnected submission
/// returns the exact owned work in [`TrySendError`], so refusal never silently
/// abandons the node. Dropping a busy worker does abandon in-memory ownership;
/// storage recovery is then the only supported way to reopen that replica.
pub struct PersistenceWorker<H, L, S> {
    requests: Option<SyncSender<Box<PersistenceWork<H, L, S>>>>,
    completions: Receiver<PersistenceWorkerCompletion<H, L, S>>,
    busy: AtomicBool,
    thread: Option<JoinHandle<()>>,
}

impl<H, L, S> fmt::Debug for PersistenceWorker<H, L, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistenceWorker")
            .field("busy", &self.busy.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl<H, L, S> PersistenceWorker<H, L, S>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
{
    /// Starts one named persistence thread with one end-to-end credit.
    ///
    /// # Errors
    ///
    /// Returns the operating-system thread creation error.
    pub fn start(options: PersistenceWorkerOptions) -> io::Result<Self> {
        let (requests, receive) = mpsc::sync_channel::<Box<PersistenceWork<H, L, S>>>(1);
        let (send, completions) = mpsc::sync_channel(1);
        let busy = AtomicBool::new(false);
        let collect_storage_telemetry = options.collect_storage_telemetry;
        let thread = thread::Builder::new()
            .name("rafter-persistence".to_owned())
            .spawn(move || {
                telemetry::set_enabled(collect_storage_telemetry);
                while let Ok(work) = receive.recv() {
                    let completion = work.persist();
                    let storage_telemetry = if collect_storage_telemetry {
                        telemetry::snapshot()
                    } else {
                        Vec::new()
                    };
                    if send
                        .try_send(PersistenceWorkerCompletion {
                            completion,
                            storage_telemetry,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            })?;
        Ok(Self {
            requests: Some(requests),
            completions,
            busy,
            thread: Some(thread),
        })
    }

    /// Submits one owned persistence operation without waiting for queue room.
    ///
    /// The credit remains occupied until [`PersistenceWorker::complete`] or
    /// [`PersistenceWorker::try_complete`] returns the completion. Both a busy
    /// worker and a full request channel return [`TrySendError::Full`] with the
    /// original work; a stopped worker returns [`TrySendError::Disconnected`]
    /// with it.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] while the one credit is occupied, or
    /// [`TrySendError::Disconnected`] after the worker has stopped. Both
    /// variants return the exact submitted work.
    pub fn try_submit(
        &self,
        work: Box<PersistenceWork<H, L, S>>,
    ) -> Result<(), PersistenceWorkerSubmitError<H, L, S>> {
        if self
            .busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(TrySendError::Full(work));
        }
        let Some(requests) = &self.requests else {
            self.busy.store(false, Ordering::Release);
            return Err(TrySendError::Disconnected(work));
        };
        if let Err(error) = requests.try_send(work) {
            self.busy.store(false, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    /// Waits for the outstanding operation and releases its worker credit.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceWorkerStopped`] when the thread ended without a
    /// completion. The originating node remains pending and must not process
    /// consensus input.
    pub fn complete(
        &self,
    ) -> Result<PersistenceWorkerCompletion<H, L, S>, PersistenceWorkerStopped> {
        let completion = self
            .completions
            .recv()
            .map_err(|_| PersistenceWorkerStopped)?;
        self.busy.store(false, Ordering::Release);
        Ok(completion)
    }

    /// Polls once for the outstanding completion without waiting.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceWorkerStopped`] when the thread ended without a
    /// completion.
    pub fn try_complete(
        &self,
    ) -> Result<Option<PersistenceWorkerCompletion<H, L, S>>, PersistenceWorkerStopped> {
        match self.completions.try_recv() {
            Ok(completion) => {
                self.busy.store(false, Ordering::Release);
                Ok(Some(completion))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(PersistenceWorkerStopped),
        }
    }

    /// Returns whether work is queued, executing, or awaiting consumption.
    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Acquire)
    }

    /// Joins an idle worker explicitly.
    ///
    /// A busy worker is left running and can still be completed. Dropping a
    /// worker also joins it, but an explicit shutdown makes a panic observable.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceWorkerShutdownError::Busy`] while one operation
    /// owns the credit, or [`PersistenceWorkerShutdownError::Panicked`] when
    /// the worker thread panicked.
    pub fn shutdown(&mut self) -> Result<(), PersistenceWorkerShutdownError> {
        if self.is_busy() {
            return Err(PersistenceWorkerShutdownError::Busy);
        }
        self.requests.take();
        if self
            .thread
            .take()
            .is_some_and(|thread| thread.join().is_err())
        {
            return Err(PersistenceWorkerShutdownError::Panicked);
        }
        Ok(())
    }
}

impl<H, L, S> Drop for PersistenceWorker<H, L, S> {
    fn drop(&mut self) {
        self.requests.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
