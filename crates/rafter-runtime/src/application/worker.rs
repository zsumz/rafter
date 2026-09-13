//! Worker ownership, admission, polling, and shutdown.

use super::{ApplicationEntry, ApplicationEvent, ApplicationWorkerOptions, DurableApplication};
use rafter::LogIndex;
use std::{
    error::Error,
    fmt, io,
    sync::{
        atomic::AtomicU64,
        mpsc::{self, Receiver, Sender},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    thread::{self, JoinHandle},
};

mod observe;
mod run;
mod submit;

pub(super) type WorkerEvent<T, S> =
    ApplicationEvent<T, <S as DurableApplication<T>>::Outcome, <S as DurableApplication<T>>::Error>;

#[derive(Debug)]
pub(super) struct Work<T> {
    pub entries: Vec<T>,
    pub retained_bytes: usize,
    pub last_index: LogIndex,
}

#[derive(Debug)]
pub(super) struct Shared {
    pub accepting: bool,
    pub accepted_through: LogIndex,
    pub inflight_entries: usize,
    pub inflight_bytes: usize,
}

/// The application thread stopped without another event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationWorkerStopped;

impl fmt::Display for ApplicationWorkerStopped {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application worker stopped before another event")
    }
}

impl Error for ApplicationWorkerStopped {}

/// Why explicit application-worker shutdown was refused or failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ApplicationWorkerShutdownError {
    /// Accepted entries are queued, executing, or awaiting event consumption.
    Busy,
    /// The application thread panicked.
    Panicked,
}

impl fmt::Display for ApplicationWorkerShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => formatter.write_str("application worker still owns entries"),
            Self::Panicked => formatter.write_str("application worker thread panicked"),
        }
    }
}

impl Error for ApplicationWorkerShutdownError {}

/// One-thread ordered executor for an embedding's durable application state.
///
/// Credits cover queued entries, the batch currently being applied, and every
/// completed event the owner has not consumed. The worker never waits to build
/// a batch: after receiving work, it combines only submissions already waiting
/// in its channel. A failed event permanently closes admission and returns both
/// the attempted batch and all accepted-but-unattempted entries.
///
/// Dropping the worker closes admission and joins its thread. Already accepted
/// entries may become durable during that join, but their client outcomes are
/// abandoned; restart recovery through the application's durable floor is the
/// only supported continuation. Prefer consuming every event and calling
/// [`ApplicationWorker::shutdown`] explicitly.
pub struct ApplicationWorker<T, S>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
{
    requests: Option<Sender<Work<T>>>,
    events: Receiver<WorkerEvent<T, S>>,
    shared: Arc<Mutex<Shared>>,
    durable_through: Arc<AtomicU64>,
    applying_through: Arc<AtomicU64>,
    options: ApplicationWorkerOptions,
    thread: Option<JoinHandle<()>>,
}

impl<T, S> fmt::Debug for ApplicationWorker<T, S>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let shared = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
        formatter
            .debug_struct("ApplicationWorker")
            .field("accepting", &shared.accepting)
            .field("accepted_through", &shared.accepted_through)
            .field("inflight_entries", &shared.inflight_entries)
            .field("inflight_bytes", &shared.inflight_bytes)
            .field("durable_through", &self.durable_through())
            .field("applying_through", &self.applying_through())
            .finish_non_exhaustive()
    }
}

impl<T, S> ApplicationWorker<T, S>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
{
    /// Starts one named application thread at the store's durable floor.
    ///
    /// `wake` runs after every event becomes available. It must remain bounded
    /// and must not call back into this worker.
    ///
    /// # Errors
    ///
    /// Returns an invalid-input error for zero or inconsistent limits, or the
    /// operating-system error from spawning the application thread.
    pub fn start(
        store: S,
        options: ApplicationWorkerOptions,
        wake: impl Fn() + Send + 'static,
    ) -> io::Result<Self> {
        let options = options.validate()?;
        let applied = store.applied_through();
        let shared = Arc::new(Mutex::new(Shared {
            accepting: true,
            accepted_through: applied,
            inflight_entries: 0,
            inflight_bytes: 0,
        }));
        let durable_through = Arc::new(AtomicU64::new(applied.0));
        let applying_through = Arc::new(AtomicU64::new(0));
        let (requests, receive) = mpsc::channel();
        let (send, events) = mpsc::channel();
        let state = run::State {
            store,
            receive,
            send,
            shared: Arc::clone(&shared),
            durable_through: Arc::clone(&durable_through),
            applying_through: Arc::clone(&applying_through),
            options,
            wake,
        };
        let thread = thread::Builder::new()
            .name("rafter-application".to_owned())
            .spawn(move || state.run())?;
        Ok(Self {
            requests: Some(requests),
            events,
            shared,
            durable_through,
            applying_through,
            options,
            thread: Some(thread),
        })
    }

    /// Joins an idle worker explicitly.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationWorkerShutdownError::Busy`] while accepted work
    /// owns credits, or [`ApplicationWorkerShutdownError::Panicked`] if the
    /// application thread panicked.
    pub fn shutdown(&mut self) -> Result<(), ApplicationWorkerShutdownError> {
        if self.is_busy() {
            return Err(ApplicationWorkerShutdownError::Busy);
        }
        self.requests.take();
        if self
            .thread
            .take()
            .is_some_and(|thread| thread.join().is_err())
        {
            return Err(ApplicationWorkerShutdownError::Panicked);
        }
        Ok(())
    }

    pub(super) fn lock_shared(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T, S> Drop for ApplicationWorker<T, S>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
{
    fn drop(&mut self) {
        self.requests.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
