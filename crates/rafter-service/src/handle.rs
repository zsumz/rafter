//! User-facing managed Raft handle.

use std::marker::PhantomData;

use rafter::NodeId;
use rafter_app::read::ReadConsistency;

use crate::{
    driver::{DriverCommandSender, QueryReceipt, WriteOptions, WriteReceipt},
    error::{MetricsError, ReadError, ShutdownError, TransferLeadershipError, WriteError},
    membership::MembershipController,
    watch::MetricsWatch,
};

/// Cloneable managed Raft handle for one group.
#[derive(Clone, Debug)]
pub struct RaftHandle<G, C, Q, R = (), QR = (), S = UnavailableDriver> {
    group_id: G,
    tx: S,
    _types: PhantomData<(C, Q, R, QR)>,
}

/// Placeholder sender type used only as a default type parameter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UnavailableDriver;

impl<G, C, Q, R, QR, S> RaftHandle<G, C, Q, R, QR, S> {
    /// Creates a handle from a group ID and command sender.
    #[must_use]
    pub fn new(group_id: G, tx: S) -> Self {
        Self {
            group_id,
            tx,
            _types: PhantomData,
        }
    }

    /// Returns the group ID this handle targets.
    #[must_use]
    pub fn group_id(&self) -> &G {
        &self.group_id
    }
}

impl<G, C, Q, R, QR, S> RaftHandle<G, C, Q, R, QR, S>
where
    G: Clone + Send + 'static,
    C: Send + 'static,
    Q: Send + 'static,
    R: Send + 'static,
    QR: Send + 'static,
    S: DriverCommandSender<G, C, Q, R, QR>,
{
    /// Proposes `command` and resolves only after commit and local apply.
    ///
    /// A local append event is not managed write success.
    ///
    /// # Errors
    ///
    /// Returns [`WriteError`] when the driver rejects the write, loses the
    /// outcome, shuts down, or encounters storage/transport/apply failure.
    pub async fn write(&self, command: C) -> Result<WriteReceipt<R>, WriteError> {
        self.write_with_options(command, WriteOptions::default())
            .await
    }

    /// Proposes `command` with explicit write options.
    ///
    /// Implementations may use [`WriteOptions::client_request_id`] for
    /// application-level idempotency metadata, but Rafter does not generate or
    /// assign durable command identities.
    ///
    /// # Errors
    ///
    /// Returns [`WriteError`] when the driver rejects the write, loses the
    /// outcome, shuts down, or encounters storage/transport/apply failure.
    pub async fn write_with_options(
        &self,
        command: C,
        options: WriteOptions,
    ) -> Result<WriteReceipt<R>, WriteError> {
        self.tx.write(self.group_id.clone(), command, options).await
    }

    /// Runs an application query under the requested consistency mode.
    ///
    /// Managed local reads do not allocate a Raft read-index ID. Linearizable
    /// reads allocate a local [`rafter::ReadId`] internally; if the driver
    /// abandons a linearizable read before it completes, it cleans up that
    /// local state before returning a terminal [`ReadError`].
    ///
    /// # Errors
    ///
    /// Returns [`ReadError`] when the driver rejects the read, cannot satisfy
    /// the requested freshness, shuts down, or encounters storage/transport
    /// failure.
    pub async fn read(
        &self,
        query: Q,
        consistency: ReadConsistency,
    ) -> Result<QueryReceipt<G, QR>, ReadError> {
        self.tx
            .read(self.group_id.clone(), query, consistency)
            .await
    }

    /// Requests transfer of leadership to `target`.
    ///
    /// `Ok(())` means the request was accepted by the driver. It does not
    /// guarantee that `target` has already become leader; callers that need
    /// completion semantics should observe metrics for the target leadership
    /// transition under their own timeout.
    ///
    /// # Errors
    ///
    /// Returns [`TransferLeadershipError`] when the driver rejects the
    /// transfer, shuts down, or encounters storage/transport failure.
    pub async fn transfer_leadership(&self, target: NodeId) -> Result<(), TransferLeadershipError> {
        self.tx
            .transfer_leadership(self.group_id.clone(), target)
            .await
    }

    /// Returns a membership controller scoped to this handle's group.
    #[must_use]
    pub fn membership(&self) -> MembershipController<G> {
        MembershipController::new(self.group_id.clone())
    }

    /// Opens a watch over the current group metrics.
    ///
    /// # Errors
    ///
    /// Returns [`MetricsError`] when the driver rejects the handle's group ID
    /// or cannot open the metrics watch.
    pub fn metrics(&self) -> Result<MetricsWatch<G>, MetricsError> {
        self.tx.metrics(self.group_id.clone())
    }

    /// # Errors
    ///
    /// Returns [`ShutdownError`] if shutdown cannot complete or was already
    /// requested.
    pub async fn shutdown(&self) -> Result<(), ShutdownError> {
        self.tx.shutdown(self.group_id.clone()).await
    }
}

#[cfg(test)]
#[path = "handle/tests.rs"]
mod tests;
