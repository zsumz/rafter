use std::{future::Future, pin::Pin};

use rafter::NodeId;
use rafter_app::read::ReadConsistency;

use crate::{
    error::{MetricsError, ReadError, ShutdownError, TransferLeadershipError, WriteError},
    watch::MetricsWatch,
};

use super::{QueryReceipt, ReadOptions, WriteOptions, WriteReceipt};

/// Boxed future returned by managed driver senders.
pub type DriverFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Boundary between the cloneable user handle and the managed driver loop.
///
/// Implementations must complete writes only after the corresponding app-layer
/// proposal has committed and applied. Local append is not a successful write.
/// Shutdown must resolve or reject outstanding waiters instead of leaving them
/// pending forever.
pub trait DriverCommandSender<G, C, Q, R, QR>: Clone + Send + Sync + 'static {
    /// Proposes `command` and resolves only after it has committed and applied.
    ///
    /// The returned future must not resolve `Ok` on a local append. An entry in
    /// the local log is not a committed entry, and a client told otherwise
    /// would treat an uncommitted write as durable.
    ///
    /// A failure must report the [`crate::WriteFate`] this implementation
    /// observed rather than one inferred from the failure's category, because
    /// the same fault on either side of the local append proves different
    /// things about the command. An implementation that cannot prove the
    /// command was refused reports [`crate::WriteFate::Unresolved`].
    fn write(
        &self,
        group_id: G,
        command: C,
        options: WriteOptions,
    ) -> DriverFuture<Result<WriteReceipt<R>, WriteError>>;

    /// Runs a query under `consistency`, honoring the caller's freshness floor.
    ///
    /// `options` carries [`ReadOptions::min_applied_index`], which the app
    /// layer honors verbatim. Implementations must pass it into the
    /// `ReadRequest` they build rather than substituting one of their own: a
    /// floor a driver lowered is a read-your-writes guarantee silently
    /// weakened.
    ///
    /// Which [`ReadConsistency`] levels an implementation serves is its own
    /// choice, and a level it does not serve must be refused with
    /// [`crate::ReadError::UnsupportedConsistency`] rather than silently
    /// answered at a weaker one. Both shipped drivers serve
    /// [`ReadConsistency::Linearizable`] and [`ReadConsistency::Local`], which
    /// are the two levels the app layer implements, and both refuse
    /// [`ReadConsistency::LeaseRead`] — [`crate::TransportRaftDriver`] here,
    /// [`crate::InMemoryRaftDriver`] by forwarding to a layer that refuses it.
    fn read(
        &self,
        group_id: G,
        query: Q,
        consistency: ReadConsistency,
        options: ReadOptions,
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

    /// Shuts `group_id` down, resolving or rejecting every waiter it holds.
    ///
    /// Shutdown is terminal. An implementation refuses every later operation
    /// and reports [`ShutdownError::AlreadyShutDown`] for a second call rather
    /// than succeeding again, so a supervisor can tell "I stopped this" from "it
    /// was already stopped".
    ///
    /// No waiter may be left pending. A write that cannot be resolved with a
    /// known outcome resolves as [`crate::WriteError::UnknownOutcome`], because
    /// an appended proposal may still commit under a later incarnation; a read
    /// resolves terminally, because a read takes no effect.
    fn shutdown(&self, group_id: G) -> DriverFuture<Result<(), ShutdownError>>;
}
