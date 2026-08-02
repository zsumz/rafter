//! Cloneable nonblocking `RaftTransport` admission handle.

use std::{collections::BTreeMap, fmt, sync::Arc};

use rafter::NodeId;
use rafter_service::{PeerEnvelope, PeerPolicy, RaftTransport, SnapshotChunkEnvelope};

use crate::diagnostics::{increment, Counters};
use crate::directory::OutboundRoute;
use crate::queue::{OutboundItem, OutboundQueue, OutboundQueueError, QueueFull};
use crate::runtime::RuntimeControl;
use crate::snapshot::SnapshotResolverHandle;
use crate::{
    BoxError, DirectoryError, EncodePeerFrameError, GroupIdCodec, PeerFrame, PeerFrameCodec,
    PeerFrameError, PeerFrameScratch, PeerId, TlsPeerDirectory, TlsTransportError, TrafficClass,
};

pub(crate) struct SenderCore<G, C> {
    pub(crate) local_peer_id: PeerId,
    pub(crate) directory: TlsPeerDirectory<G>,
    pub(crate) codec: Arc<PeerFrameCodec<G, C>>,
    pub(crate) queues: Arc<BTreeMap<PeerId, Arc<OutboundQueue<G>>>>,
    pub(crate) snapshot_resolver: Option<SnapshotResolverHandle<G>>,
    pub(crate) control: Arc<RuntimeControl>,
    pub(crate) counters: Arc<Counters>,
}

impl<G, C> fmt::Debug for SenderCore<G, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SenderCore")
            .field("local_peer_id", &self.local_peer_id)
            .field("queues", &self.queues.len())
            .field("snapshot_resolver", &self.snapshot_resolver.is_some())
            .finish_non_exhaustive()
    }
}

/// Cloneable synchronous admission handle for the blocking TLS runtime.
///
/// The managed driver may call this while holding its own lock. The handle does
/// no DNS, socket, TLS, disk, sleep, thread spawn, snapshot read, or queue wait.
/// It validates, encodes bounded metadata, attempts one finite queue insertion,
/// and returns. Bounded directory and queue mutex acquisition is the only
/// synchronization on the call path. A sender obtained from
/// [`crate::TlsPeerTransportBuilder::bind_paused`] accepts the same bounded
/// admission and policy updates while workers remain inert until activation.
pub struct TlsSender<G, C> {
    pub(crate) core: Arc<SenderCore<G, C>>,
}

impl<G, C> Clone for TlsSender<G, C> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<G, C> fmt::Debug for TlsSender<G, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsSender")
            .field("local_peer_id", &self.core.local_peer_id)
            .field("configured_peers", &self.core.queues.len())
            .field("snapshot_resolver", &self.core.snapshot_resolver.is_some())
            .finish_non_exhaustive()
    }
}

impl<G, C> TlsSender<G, C>
where
    G: Ord,
    C: GroupIdCodec<G>,
{
    fn send_envelope(&self, envelope: PeerEnvelope<G>) -> Result<(), TlsTransportError> {
        self.require_accepting()?;
        let (peer, authorization) = self.route(&envelope.group_id, envelope.from, envelope.to)?;
        let from = envelope.from;
        let to = envelope.to;
        let class = TrafficClass::for_message(&envelope.message);
        let frame = PeerFrame::new(
            crate::ConnectionSequence::FIRST,
            envelope.group_id,
            from,
            to,
            envelope.message,
        )
        .map_err(map_frame_error)?;
        let mut scratch = PeerFrameScratch::new();
        let prepared = self
            .core
            .codec
            .prepare(&frame, &mut scratch)
            .map_err(map_encode_error)?;
        self.enqueue(
            peer,
            OutboundItem::message(from, to, class, prepared, authorization),
        )
    }

    fn send_snapshot_envelope(
        &self,
        envelope: SnapshotChunkEnvelope<G>,
    ) -> Result<(), TlsTransportError> {
        self.require_accepting()?;
        if envelope.chunk.leader_id != envelope.from {
            return Err(TlsTransportError::SenderMismatch {
                envelope_from: envelope.from,
                message_from: envelope.chunk.leader_id,
            });
        }
        let (peer, authorization) = self.route(&envelope.group_id, envelope.from, envelope.to)?;
        if self.core.snapshot_resolver.is_none() {
            return Err(TlsTransportError::SnapshotResolverUnavailable);
        }

        let mut scratch = PeerFrameScratch::new();
        let reserved_bytes = self
            .core
            .codec
            .snapshot_wire_len(&envelope.group_id, &envelope.chunk, &mut scratch)
            .map_err(map_encode_error)?;
        self.enqueue(
            peer,
            OutboundItem::snapshot(
                envelope.group_id,
                envelope.from,
                envelope.to,
                reserved_bytes,
                envelope.chunk,
                authorization,
            ),
        )?;
        increment(&self.core.counters.snapshot_directives_enqueued);
        Ok(())
    }

    fn route(
        &self,
        group_id: &G,
        from: NodeId,
        to: NodeId,
    ) -> Result<(PeerId, crate::directory::RouteAuthorization), TlsTransportError> {
        match self
            .core
            .directory
            .outbound_route(group_id, &self.core.local_peer_id, from, to)
            .map_err(|source| self.directory_error(source))?
        {
            OutboundRoute::UnknownGroup => Err(TlsTransportError::UnknownGroup),
            OutboundRoute::LocalIdentityMismatch => {
                Err(TlsTransportError::LocalIdentityMismatch { node_id: from })
            }
            OutboundRoute::UnknownNode => Err(TlsTransportError::UnknownNode { node_id: to }),
            OutboundRoute::Unauthorized => Err(TlsTransportError::UnauthorizedPeer { node_id: to }),
            OutboundRoute::Retired => Err(TlsTransportError::RetiredPeer { node_id: to }),
            OutboundRoute::Authorized {
                peer,
                authorization,
            } => Ok((peer, authorization)),
        }
    }

    fn enqueue(&self, peer: PeerId, item: OutboundItem<G>) -> Result<(), TlsTransportError> {
        let class = item.class();
        let queue = self
            .core
            .queues
            .get(&peer)
            .ok_or_else(|| TlsTransportError::EndpointUnavailable { peer: peer.clone() })?;
        match queue.try_push(item) {
            Ok(()) => {
                increment(&self.core.counters.frames_enqueued);
                Ok(())
            }
            Err(OutboundQueueError::Full(QueueFull { usage })) => {
                increment(&self.core.counters.queue_full);
                Err(TlsTransportError::QueueFull {
                    peer,
                    class,
                    frames: usage.frames,
                    bytes: usage.bytes,
                })
            }
            Err(OutboundQueueError::Closed) if self.core.control.terminal() => {
                Err(TlsTransportError::TerminalFailure {
                    message: self.core.control.terminal_failure(),
                })
            }
            Err(OutboundQueueError::Closed) => Err(TlsTransportError::Stopped),
            Err(OutboundQueueError::Poisoned) => {
                self.core.control.fail("outbound queue state is poisoned");
                Err(TlsTransportError::InternalState)
            }
        }
    }

    fn directory_error(&self, source: DirectoryError) -> TlsTransportError {
        if matches!(source, DirectoryError::Poisoned) {
            self.core.control.fail("peer directory state is poisoned");
        }
        match source {
            DirectoryError::UnknownGroup => TlsTransportError::UnknownGroup,
            source => TlsTransportError::Directory { source },
        }
    }

    fn require_accepting(&self) -> Result<(), TlsTransportError> {
        if self.core.control.terminal() {
            return Err(TlsTransportError::TerminalFailure {
                message: self.core.control.terminal_failure(),
            });
        }
        if self.core.control.accepts_send() {
            Ok(())
        } else {
            Err(TlsTransportError::Stopped)
        }
    }
}

impl<G, C> RaftTransport<G> for TlsSender<G, C>
where
    G: Ord + Send + Sync + 'static,
    C: GroupIdCodec<G>,
{
    type PeerPrincipal = PeerId;
    type Error = TlsTransportError;

    fn send(&self, envelope: PeerEnvelope<G>) -> Result<(), Self::Error> {
        self.send_envelope(envelope)
    }

    fn send_snapshot_chunk(&self, envelope: SnapshotChunkEnvelope<G>) -> Result<(), Self::Error> {
        self.send_snapshot_envelope(envelope)
    }

    fn update_peers(
        &self,
        group_id: &G,
        policy: PeerPolicy<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error> {
        self.require_accepting()?;
        for peer in policy.peers() {
            if !self.core.queues.contains_key(peer) {
                return Err(TlsTransportError::EndpointUnavailable { peer: peer.clone() });
            }
        }
        self.core
            .directory
            .replace_policy(group_id, policy)
            .map_err(|source| self.directory_error(source))
    }
}

fn map_frame_error(error: PeerFrameError) -> TlsTransportError {
    match error {
        PeerFrameError::SenderMismatch {
            envelope_from,
            message_from,
        } => TlsTransportError::SenderMismatch {
            envelope_from,
            message_from,
        },
    }
}

fn map_encode_error<E>(error: EncodePeerFrameError<E>) -> TlsTransportError
where
    E: std::error::Error + Send + Sync + 'static,
{
    match error {
        EncodePeerFrameError::GroupEncode(source) => TlsTransportError::GroupEncode {
            source: Box::new(source) as BoxError,
        },
        EncodePeerFrameError::EmptyGroupId => TlsTransportError::EmptyGroupId,
        EncodePeerFrameError::GroupIdTooLarge { actual, maximum } => {
            TlsTransportError::GroupIdTooLarge { actual, maximum }
        }
        EncodePeerFrameError::MessageEncode(source) => TlsTransportError::MessageEncode { source },
        EncodePeerFrameError::MessageLengthOverflow => TlsTransportError::MessageLengthOverflow,
        EncodePeerFrameError::FrameLengthOverflow => TlsTransportError::FrameLengthOverflow,
        EncodePeerFrameError::FrameTooLarge { actual, maximum } => {
            TlsTransportError::FrameTooLarge { actual, maximum }
        }
    }
}
