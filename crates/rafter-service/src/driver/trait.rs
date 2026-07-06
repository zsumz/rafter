use std::{future::Future, pin::Pin};

use rafter::NodeId;
use rafter_app::read::ReadConsistency;

use crate::{
    error::{MetricsError, ReadError, ShutdownError, TransferLeadershipError, WriteError},
    watch::MetricsWatch,
};

use super::{QueryReceipt, WriteOptions, WriteReceipt};

/// Boxed future returned by managed driver senders.
pub type DriverFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Boundary between the cloneable user handle and the managed driver loop.
///
/// Implementations must complete writes only after the corresponding app-layer
/// proposal has committed and applied. Local append is not a successful write.
/// Shutdown must resolve or reject outstanding waiters instead of leaving them
/// pending forever.
pub trait DriverCommandSender<G, C, Q, R, QR>: Clone + Send + Sync + 'static {
    fn write(
        &self,
        group_id: G,
        command: C,
        options: WriteOptions,
    ) -> DriverFuture<Result<WriteReceipt<R>, WriteError>>;

    fn read(
        &self,
        group_id: G,
        query: Q,
        consistency: ReadConsistency,
    ) -> DriverFuture<Result<QueryReceipt<G, QR>, ReadError>>;

    /// Requests a leadership transfer.
    ///
    /// `Ok(())` means the driver accepted the transfer request and processed
    /// immediate side effects. It does not guarantee that `target` has already
    /// become leader; callers that need completion semantics should observe
    /// metrics until the target is reported as leader or their own deadline
    /// expires.
    fn transfer_leadership(
        &self,
        group_id: G,
        target: NodeId,
    ) -> DriverFuture<Result<(), TransferLeadershipError>>;

    /// Opens a metrics watch for `group_id`.
    ///
    /// Implementations should reject unknown or mismatched group IDs rather
    /// than returning metrics for another group.
    ///
    /// # Errors
    ///
    /// Returns [`MetricsError`] when `group_id` is not owned by this driver or
    /// the driver cannot open a metrics watch.
    fn metrics(&self, group_id: G) -> Result<MetricsWatch<G>, MetricsError>;

    fn shutdown(&self, group_id: G) -> DriverFuture<Result<(), ShutdownError>>;
}
