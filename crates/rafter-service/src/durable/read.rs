//! Encoded queries never borrow the busy application to enter the queue.

mod contract;
mod shared;

pub use contract::{DurableClientOptions, DurableReadLimits, DurableReadStats};
pub(in crate::durable) use shared::{Phase, ReadShared, Work};

use crate::{DriverFuture, QueryReceipt, ReadError, ReadOptions};
use rafter_app::read::ReadConsistency;
use std::{fmt, sync::Arc};

/// A cloneable read endpoint that cannot keep the durable backend alive.
pub struct DurableReadSender<R> {
    pub(in crate::durable) shared: Arc<ReadShared<R>>,
}
impl<R> Clone for DurableReadSender<R> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}
impl<R> fmt::Debug for DurableReadSender<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableReadSender")
            .field("retained", &self.shared.stats())
            .finish_non_exhaustive()
    }
}
impl<R: Send + 'static> DurableReadSender<R> {
    /// Queues an encoded query with the caller's unmodified freshness floor.
    ///
    /// Linearizable reads require a real Raft barrier and a verified durable
    /// application view. Local reads carry no quorum proof; lease reads are
    /// refused. The proof's group is `()` because this endpoint belongs to one
    /// owner; authenticated group routing remains the embedding's obligation.
    /// Dropping a future abandons observation. An outstanding quorum round
    /// retains its credit until the kernel ends it or the backend is released.
    /// # Errors
    /// Refuses exhausted admission, unsupported consistency, unavailable
    /// freshness, protocol rejection/cancellation, or terminal owner state.
    /// [`ReadError::Backpressure`] means retained request or encoded `Vec`
    /// capacity would exceed the configured budget; no query or barrier starts.
    #[must_use]
    pub fn read(
        &self,
        query: Vec<u8>,
        consistency: ReadConsistency,
        options: ReadOptions,
    ) -> DriverFuture<Result<QueryReceipt<(), R>, ReadError>> {
        match self.shared.admit(query, consistency, options) {
            Ok(reply) => {
                let future = reply.future();
                (self.shared.wake)();
                future
            }
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }

    /// Observes credits for queued, owned, and completed uncollected queries.
    #[must_use]
    pub fn retained(&self) -> DurableReadStats {
        self.shared.stats()
    }
}
