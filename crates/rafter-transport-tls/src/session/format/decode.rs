//! Version-1 durable session-state decoding.

use std::{collections::BTreeMap, num::NonZeroU64, str};

use rafter_crc32::crc32;

use crate::{
    session::{ConnectionSession, PeerSessionState, TransportSessionState},
    ClusterId, PeerId, SessionStoreLimits,
};

use super::{
    DecodeTransportSessionStateError, PersistedTransportSessionState, Reader, SessionIdentityField,
    SESSION_STATE_MAGIC, SESSION_STATE_VERSION,
};

/// Decodes exactly one canonical version-1 durable session-state file.
///
/// # Errors
///
/// Returns [`DecodeTransportSessionStateError`] when bytes are truncated,
/// corrupt, noncanonical, semantically invalid, or use another version.
pub fn decode_transport_session_state(
    input: &[u8],
) -> Result<PersistedTransportSessionState, DecodeTransportSessionStateError> {
    let mut reader = Reader::new(input);
    let magic = reader.array::<8>()?;
    if magic != SESSION_STATE_MAGIC {
        return Err(DecodeTransportSessionStateError::InvalidMagic { actual: magic });
    }
    let version = reader.u16()?;
    if version != SESSION_STATE_VERSION {
        return Err(DecodeTransportSessionStateError::UnsupportedVersion { version });
    }

    let cluster_id = decode_cluster_id(&mut reader)?;
    let local_peer_id = decode_peer_id(&mut reader, SessionIdentityField::LocalPeer, None)?;
    let limits = SessionStoreLimits::new(usize::from(reader.u16()?))
        .map_err(|source| DecodeTransportSessionStateError::InvalidPeerLimit { source })?;
    let count = usize::from(reader.u16()?);
    if count > limits.max_peer_records() {
        return Err(DecodeTransportSessionStateError::PeerCountExceedsLimit {
            count,
            maximum: limits.max_peer_records(),
        });
    }

    let peers = decode_peer_records(&mut reader, count)?;
    let checksum_start = reader.position();
    let expected = reader.u32()?;
    let actual = crc32(&input[..checksum_start]);
    if expected != actual {
        return Err(DecodeTransportSessionStateError::ChecksumMismatch { expected, actual });
    }
    reader.finish()?;

    let state = TransportSessionState::from_canonical_peer_states(limits, peers);
    Ok(PersistedTransportSessionState::new(
        cluster_id,
        local_peer_id,
        state,
    ))
}

fn decode_cluster_id(
    reader: &mut Reader<'_>,
) -> Result<ClusterId, DecodeTransportSessionStateError> {
    let bytes = decode_identity_bytes(reader)?;
    let value =
        str::from_utf8(bytes).map_err(|_| DecodeTransportSessionStateError::InvalidUtf8 {
            field: SessionIdentityField::Cluster,
            record: None,
        })?;
    ClusterId::new(value).map_err(|source| DecodeTransportSessionStateError::InvalidIdentity {
        field: SessionIdentityField::Cluster,
        record: None,
        source,
    })
}

fn decode_peer_id(
    reader: &mut Reader<'_>,
    field: SessionIdentityField,
    record: Option<usize>,
) -> Result<PeerId, DecodeTransportSessionStateError> {
    let bytes = decode_identity_bytes(reader)?;
    let value = str::from_utf8(bytes)
        .map_err(|_| DecodeTransportSessionStateError::InvalidUtf8 { field, record })?;
    PeerId::new(value).map_err(|source| DecodeTransportSessionStateError::InvalidIdentity {
        field,
        record,
        source,
    })
}

fn decode_identity_bytes<'a>(
    reader: &mut Reader<'a>,
) -> Result<&'a [u8], DecodeTransportSessionStateError> {
    let len = usize::from(reader.u8()?);
    reader.bytes(len)
}

fn decode_peer_records(
    reader: &mut Reader<'_>,
    count: usize,
) -> Result<BTreeMap<PeerId, PeerSessionState>, DecodeTransportSessionStateError> {
    let mut peers = BTreeMap::new();
    let mut previous: Option<PeerId> = None;
    for record in 0..count {
        let peer = decode_peer_id(reader, SessionIdentityField::RemotePeer, Some(record))?;
        if let Some(previous) = &previous {
            if peer.as_str() <= previous.as_str() {
                return Err(DecodeTransportSessionStateError::NonCanonicalPeerOrder {
                    previous: previous.clone(),
                    actual: peer,
                });
            }
        }
        let outbound = decode_session(reader.u64()?);
        let inbound = decode_session(reader.u64()?);
        let state = PeerSessionState::new(outbound, inbound);
        if state.is_empty() {
            return Err(DecodeTransportSessionStateError::EmptyPeerRecord { peer });
        }
        previous = Some(peer.clone());
        peers.insert(peer, state);
    }
    Ok(peers)
}

fn decode_session(value: u64) -> Option<ConnectionSession> {
    NonZeroU64::new(value).map(ConnectionSession::from_nonzero)
}
