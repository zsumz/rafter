//! Cloneable admission without retaining or borrowing the backend.
use super::shared::Shared;
use super::DurableWriteStats;
use crate::{DriverFuture, WriteError, WriteOptions, WriteReceipt};
use std::{fmt, sync::Arc};

/// Cloneable admission endpoint. It does not retain the durable backend.
pub struct DurableWriteSender<O> {
    pub(super) shared: Arc<Shared<O>>,
}
impl<O> DurableWriteSender<O> {
    /// Counts write count/capacity budget refusals since owner construction.
    ///
    /// Only admission `Backpressure` is counted. The observational counter
    /// saturates at `usize::MAX`, remains available after backend release, and
    /// is not persisted across recovery. Reading it drives no work or callbacks.
    #[must_use]
    pub fn backpressure_refusals(&self) -> usize {
        self.shared.backpressure_refusals()
    }
}
impl<O> Clone for DurableWriteSender<O> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}
impl<O> fmt::Debug for DurableWriteSender<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableWriteSender")
            .field("retained", &self.shared.stats())
            .finish_non_exhaustive()
    }
}

impl<O: Send + 'static> DurableWriteSender<O> {
    /// Admits already encoded application bytes. Encoding does not borrow the
    /// busy application worker. `client_request_id` is correlation only.
    ///
    /// Success means the application transaction is durable. Dropping the
    /// future only abandons observation; an accepted command may still commit.
    /// # Errors
    /// Reports proven refusal before admission, otherwise preserves uncertainty.
    /// [`WriteError::Backpressure`] means retained request or encoded `Vec`
    /// capacity would exceed the configured budget; nothing was appended.
    #[must_use]
    pub fn write(
        &self,
        payload: Vec<u8>,
        options: WriteOptions,
    ) -> DriverFuture<Result<WriteReceipt<O>, WriteError>> {
        match self.shared.admit(payload, options) {
            Ok(reply) => {
                let future = reply.future();
                (self.shared.wake)();
                future
            }
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }

    /// Reports retained client credits; the two counters are observational.
    #[must_use]
    pub fn retained(&self) -> DurableWriteStats {
        self.shared.stats()
    }
}
