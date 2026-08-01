//! Public resolver contract and source adapter.

use std::{convert::Infallible, error::Error};

use rafter::SnapshotChunkSource;

use super::SnapshotChunkResolveRequest;

/// Resolves queued snapshot directives on a sender worker.
///
/// [`rafter_service::RaftTransport::send_snapshot_chunk`] never invokes this
/// trait. It performs only bounded validation and queue admission while the
/// managed driver may hold its lock. The persistent sender worker invokes the
/// resolver later, outside that lock and before assigning a live connection
/// sequence.
///
/// `Ok(None)` is a source refusal, not a transport failure: the named snapshot
/// is unavailable or no longer current, so the directive is dropped like a
/// lost Raft message. `Err` records a resolver failure and drops this attempt;
/// a later kernel directive may retry the transfer.
///
/// Implementations own any storage I/O deadline. Runtime shutdown cannot
/// preempt caller code blocked inside `resolve`, so a production resolver must
/// return within a finite deployment-controlled bound.
pub trait SnapshotChunkResolver<G>: Send + Sync + 'static {
    /// Typed storage or application error raised while reading snapshot bytes.
    type Error: Error + Send + Sync + 'static;

    /// Returns exactly `request.chunk().len` bytes, or `None` when the source
    /// cannot serve the named transfer.
    ///
    /// # Errors
    ///
    /// Returns the resolver's typed error when the backing snapshot source
    /// cannot be read safely.
    fn resolve(
        &self,
        request: SnapshotChunkResolveRequest<'_, G>,
    ) -> Result<Option<Vec<u8>>, Self::Error>;
}

/// Adapts one ordinary [`SnapshotChunkSource`] to every group route.
///
/// Use a custom [`SnapshotChunkResolver`] when different groups use different
/// stores or when payload reads can fail with a typed operational error.
#[derive(Clone, Debug)]
pub struct SnapshotChunkSourceResolver<S> {
    source: S,
}

impl<S> SnapshotChunkSourceResolver<S> {
    /// Wraps a snapshot source without changing its ownership model.
    #[must_use]
    pub fn new(source: S) -> Self {
        Self { source }
    }

    /// Returns the wrapped source.
    #[must_use]
    pub fn source(&self) -> &S {
        &self.source
    }

    /// Consumes the adapter and returns the wrapped source.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.source
    }
}

impl<G, S> SnapshotChunkResolver<G> for SnapshotChunkSourceResolver<S>
where
    S: SnapshotChunkSource + Send + Sync + 'static,
{
    type Error = Infallible;

    fn resolve(
        &self,
        request: SnapshotChunkResolveRequest<'_, G>,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.source.snapshot_chunk(request.source_request()))
    }
}
