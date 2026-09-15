//! Bounded off-owner destruction of log entries retired by local snapshots.

use rafter::RetiredLogEntries;
use std::{
    error::Error,
    fmt, io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Sender},
        Arc,
    },
    thread::{self, JoinHandle},
};

/// Entry and payload-byte credits for one log-retirement worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogRetirementWorkerOptions {
    /// Maximum queued or currently dropping retired entries.
    pub max_inflight_entries: usize,
    /// Maximum queued or currently dropping application payload bytes.
    pub max_inflight_payload_bytes: usize,
}

impl LogRetirementWorkerOptions {
    /// Builds explicit retirement credit limits.
    #[must_use]
    pub const fn new(max_inflight_entries: usize, max_inflight_payload_bytes: usize) -> Self {
        Self {
            max_inflight_entries,
            max_inflight_payload_bytes,
        }
    }

    fn validate(self) -> io::Result<Self> {
        if self.max_inflight_entries == 0 || self.max_inflight_payload_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "log-retirement entry and payload-byte limits must be nonzero",
            ));
        }
        Ok(self)
    }
}

/// Why a retired prefix could not be handed to the background worker.
#[non_exhaustive]
pub enum LogRetirementSubmitError {
    /// The prefix would exceed entry or payload-byte credits.
    Full(RetiredLogEntries),
    /// The worker is shut down or its thread has stopped.
    Stopped(RetiredLogEntries),
}

impl LogRetirementSubmitError {
    /// Returns the entries so the caller can drop them inline.
    pub fn into_retired_entries(self) -> RetiredLogEntries {
        match self {
            Self::Full(entries) | Self::Stopped(entries) => entries,
        }
    }
}

impl fmt::Debug for LogRetirementSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, entries) = match self {
            Self::Full(entries) => ("Full", entries),
            Self::Stopped(entries) => ("Stopped", entries),
        };
        formatter
            .debug_struct(kind)
            .field("entries", &entries.len())
            .field("payload_bytes", &entries.payload_bytes())
            .finish()
    }
}

impl fmt::Display for LogRetirementSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("log-retirement credits are full"),
            Self::Stopped(_) => formatter.write_str("log-retirement worker is stopped"),
        }
    }
}

impl Error for LogRetirementSubmitError {}

/// Failure while explicitly joining a log-retirement worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogRetirementWorkerPanicked;

impl fmt::Display for LogRetirementWorkerPanicked {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("log-retirement worker thread panicked")
    }
}

impl Error for LogRetirementWorkerPanicked {}

#[derive(Default)]
struct Credits {
    entries: AtomicUsize,
    payload_bytes: AtomicUsize,
}

struct Work {
    retired: RetiredLogEntries,
    entries: usize,
    payload_bytes: usize,
}

/// One bounded worker that drops retired log payloads away from a consensus owner.
///
/// Credits cover queued and currently dropping work. Submission never waits;
/// callers retain ownership on refusal and can drop inline. Dropping the worker
/// drains accepted work and joins its thread.
pub struct LogRetirementWorker {
    sender: Option<Sender<Work>>,
    credits: Arc<Credits>,
    options: LogRetirementWorkerOptions,
    thread: Option<JoinHandle<()>>,
}

impl LogRetirementWorker {
    /// Starts one named background drop thread with explicit credit limits.
    ///
    /// # Errors
    ///
    /// Returns invalid input for zero limits or the operating-system thread
    /// creation error.
    pub fn start(options: LogRetirementWorkerOptions) -> io::Result<Self> {
        let options = options.validate()?;
        let credits = Arc::new(Credits::default());
        let worker_credits = Arc::clone(&credits);
        let (sender, receiver) = mpsc::channel::<Work>();
        let thread = thread::Builder::new()
            .name("rafter-log-retirement".to_owned())
            .spawn(move || {
                while let Ok(work) = receiver.recv() {
                    drop(work.retired);
                    worker_credits
                        .entries
                        .fetch_sub(work.entries, Ordering::AcqRel);
                    worker_credits
                        .payload_bytes
                        .fetch_sub(work.payload_bytes, Ordering::AcqRel);
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            credits,
            options,
            thread: Some(thread),
        })
    }

    /// Submits one retired prefix without waiting for the drop thread.
    ///
    /// # Errors
    ///
    /// Returns the entries with [`LogRetirementSubmitError::Full`] when either
    /// credit limit would be exceeded, or with
    /// [`LogRetirementSubmitError::Stopped`] when the worker is unavailable.
    /// An empty prefix is an accepted no-op, including after shutdown.
    pub fn try_submit(&self, retired: RetiredLogEntries) -> Result<(), LogRetirementSubmitError> {
        let entries = retired.len();
        let payload_bytes = retired.payload_bytes();
        if retired.is_empty() {
            return Ok(());
        }
        if !reserve(
            &self.credits.entries,
            entries,
            self.options.max_inflight_entries,
        ) {
            return Err(LogRetirementSubmitError::Full(retired));
        }
        if !reserve(
            &self.credits.payload_bytes,
            payload_bytes,
            self.options.max_inflight_payload_bytes,
        ) {
            self.credits.entries.fetch_sub(entries, Ordering::AcqRel);
            return Err(LogRetirementSubmitError::Full(retired));
        }
        let work = Work {
            retired,
            entries,
            payload_bytes,
        };
        let Some(sender) = &self.sender else {
            self.release(&work);
            return Err(LogRetirementSubmitError::Stopped(work.retired));
        };
        if let Err(error) = sender.send(work) {
            let work = error.0;
            self.release(&work);
            return Err(LogRetirementSubmitError::Stopped(work.retired));
        }
        Ok(())
    }

    /// Returns queued plus currently dropping entry credits.
    #[must_use]
    pub fn inflight_entries(&self) -> usize {
        self.credits.entries.load(Ordering::Acquire)
    }

    /// Returns queued plus currently dropping payload-byte credits.
    #[must_use]
    pub fn inflight_payload_bytes(&self) -> usize {
        self.credits.payload_bytes.load(Ordering::Acquire)
    }

    /// Drains accepted work and joins the worker thread.
    ///
    /// # Errors
    ///
    /// Returns [`LogRetirementWorkerPanicked`] when the worker thread panicked.
    pub fn shutdown(&mut self) -> Result<(), LogRetirementWorkerPanicked> {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| LogRetirementWorkerPanicked)?;
        }
        Ok(())
    }

    fn release(&self, work: &Work) {
        self.credits
            .entries
            .fetch_sub(work.entries, Ordering::AcqRel);
        self.credits
            .payload_bytes
            .fetch_sub(work.payload_bytes, Ordering::AcqRel);
    }
}

impl fmt::Debug for LogRetirementWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogRetirementWorker")
            .field("options", &self.options)
            .field("inflight_entries", &self.inflight_entries())
            .field("inflight_payload_bytes", &self.inflight_payload_bytes())
            .finish_non_exhaustive()
    }
}

impl Drop for LogRetirementWorker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn reserve(counter: &AtomicUsize, amount: usize, limit: usize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(amount).filter(|next| *next <= limit)
        })
        .is_ok()
}

#[cfg(test)]
#[path = "log_retirement_test.rs"]
mod tests;
