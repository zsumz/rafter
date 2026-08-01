//! Borrowed request handed to one snapshot resolver invocation.

use rafter::{NodeId, SnapshotChunkRequest, SnapshotChunkSend};

/// One queued snapshot directive being resolved by a sender worker.
///
/// The request borrows the caller-owned group route and the kernel's bounded
/// directive. A resolver returns payload bytes only; the transport validates
/// their exact length and constructs the authenticated Raft message itself.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotChunkResolveRequest<'a, G> {
    group_id: &'a G,
    from: NodeId,
    to: NodeId,
    chunk: &'a SnapshotChunkSend,
}

impl<'a, G> SnapshotChunkResolveRequest<'a, G> {
    pub(crate) fn new(
        group_id: &'a G,
        from: NodeId,
        to: NodeId,
        chunk: &'a SnapshotChunkSend,
    ) -> Self {
        Self {
            group_id,
            from,
            to,
            chunk,
        }
    }

    /// Caller-owned Raft group route.
    #[must_use]
    pub fn group_id(&self) -> &'a G {
        self.group_id
    }

    /// Leader-side Raft sender named by the outer envelope.
    #[must_use]
    pub fn from(&self) -> NodeId {
        self.from
    }

    /// Follower-side Raft recipient named by the outer envelope.
    #[must_use]
    pub fn to(&self) -> NodeId {
        self.to
    }

    /// Kernel-emitted bounded snapshot directive.
    #[must_use]
    pub fn chunk(&self) -> &'a SnapshotChunkSend {
        self.chunk
    }

    /// Converts the directive to the source-level bounded byte request.
    #[must_use]
    pub fn source_request(self) -> SnapshotChunkRequest<'a> {
        let chunk = self.chunk;
        SnapshotChunkRequest {
            transfer_id: chunk.transfer_id,
            metadata: &chunk.metadata,
            total_payload_len: chunk.total_payload_len,
            application_payload_crc32: chunk.application_payload_crc32,
            offset: chunk.offset,
            len: chunk.len,
        }
    }
}
