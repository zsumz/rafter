//! Nonblocking immutable application borrows between validated writes.

use super::{ApplicationWorker, Shared, Work};
use crate::application::{
    ApplicationEntry, ApplicationRead, ApplicationReadError, ApplicationReadRejection,
    DurableApplication,
};
use rafter::LogIndex;
use std::sync::{mpsc::Sender, Mutex, PoisonError, TryLockError};

impl<T, S> ApplicationWorker<T, S>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
{
    /// Borrows a verified application view without waiting for application I/O.
    ///
    /// The callback runs on the caller's thread, excluding application writes.
    /// It must be bounded, read-only (including interior state), and must not
    /// reenter the worker. The result cannot borrow the application. No query
    /// queue, model copy, or additional `Sync` bound on the store is introduced.
    /// Completions need not be consumed first: validation and publication of
    /// the durable floor finish before the application view is made available.
    ///
    /// This is a local application read, not a Raft read-consistency proof.
    /// For a linearizable read the embedding must first obtain the appropriate
    /// read grant and supply its required *application* floor; a protocol-only
    /// log index need not correspond to an application update. The returned
    /// view may be newer than that minimum. Lifecycle admission, including
    /// recovery and snapshot handoff, remains the embedding's responsibility.
    ///
    /// # Errors
    /// Returns the unchanged callback if a write owns the view, the verified
    /// floor is too low, or the worker is stopped. Busy/behind reads can retry
    /// after worker progress; this method retains no query or wake obligation.
    ///
    /// # Panics
    /// A callback panic propagates, poisons the application view, and closes
    /// admission. Already accepted work has unknown outcome; drain termination
    /// and reopen the durable store before continuing. Catching the panic does
    /// not make this worker reusable.
    pub fn try_read<Q, R>(
        &mut self,
        required: LogIndex,
        query: Q,
    ) -> Result<ApplicationRead<R>, ApplicationReadError<Q>>
    where
        Q: FnOnce(&S) -> R,
    {
        let reject = |query, rejection| ApplicationReadError { query, rejection };
        let Some(store) = &self.store else {
            return Err(reject(query, ApplicationReadRejection::Stopped));
        };
        let store = match store.try_lock() {
            Ok(store) => store,
            Err(TryLockError::WouldBlock) => {
                return Err(reject(query, ApplicationReadRejection::Busy));
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(reject(query, ApplicationReadRejection::Stopped));
            }
        };
        if !self.is_accepting() {
            return Err(reject(query, ApplicationReadRejection::Stopped));
        }
        let durable_through = self.durable_through();
        if durable_through < required {
            return Err(reject(
                query,
                ApplicationReadRejection::Behind {
                    required,
                    durable_through,
                },
            ));
        }
        let _panic = ReadPanicGuard {
            requests: &mut self.requests,
            shared: &self.shared,
        };
        Ok(ApplicationRead {
            durable_through,
            output: query(&store),
        })
    }
}

struct ReadPanicGuard<'a, T> {
    requests: &'a mut Option<Sender<Work<T>>>,
    shared: &'a Mutex<Shared>,
}
impl<T> Drop for ReadPanicGuard<'_, T> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.shared
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .accepting = false;
            // Wake an idle receiver too, so termination remains observable
            // through the existing completion channel and notification hook.
            self.requests.take();
        }
    }
}
