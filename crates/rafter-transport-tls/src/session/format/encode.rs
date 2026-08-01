//! Version-1 durable session-state encoding.

use rafter_crc32::crc32;

use crate::{session::ConnectionSession, SessionStoreLimits, MAX_ID_BYTES};

use super::{
    EncodeTransportSessionStateError, PersistedTransportSessionState, SessionIdentityField,
    SESSION_STATE_MAGIC, SESSION_STATE_VERSION,
};

const FIXED_MAX_BYTES: usize =
    SESSION_STATE_MAGIC.len() + 2 + 1 + MAX_ID_BYTES + 1 + MAX_ID_BYTES + 2 + 2 + 4;
const RECORD_MAX_BYTES: usize = 1 + MAX_ID_BYTES + 8 + 8;

/// Returns the largest version-1 file accepted for `limits`.
#[must_use]
pub const fn max_transport_session_state_bytes(limits: SessionStoreLimits) -> usize {
    FIXED_MAX_BYTES.saturating_add(RECORD_MAX_BYTES.saturating_mul(limits.max_peer_records()))
}

/// Encodes one complete version-1 durable session-state file.
///
/// # Errors
///
/// Returns [`EncodeTransportSessionStateError`] when the logical state cannot
/// be represented canonically by version 1.
pub fn encode_transport_session_state(
    state: &PersistedTransportSessionState,
) -> Result<Vec<u8>, EncodeTransportSessionStateError> {
    let mut output = Vec::new();
    encode_transport_session_state_into(&mut output, state)?;
    Ok(output)
}

/// Encodes one complete version-1 state into a reusable caller-owned buffer.
///
/// The buffer is cleared before encoding and remains empty when validation
/// fails.
///
/// # Errors
///
/// Returns [`EncodeTransportSessionStateError`] when the logical state cannot
/// be represented canonically by version 1.
pub fn encode_transport_session_state_into(
    output: &mut Vec<u8>,
    state: &PersistedTransportSessionState,
) -> Result<(), EncodeTransportSessionStateError> {
    output.clear();
    if let Err(error) = encode_body(output, state) {
        output.clear();
        return Err(error);
    }
    let checksum = crc32(output);
    output.extend_from_slice(&checksum.to_be_bytes());
    Ok(())
}

fn encode_body(
    output: &mut Vec<u8>,
    state: &PersistedTransportSessionState,
) -> Result<(), EncodeTransportSessionStateError> {
    let cluster_len = identity_len(SessionIdentityField::Cluster, state.cluster_id().as_str())?;
    let local_peer_len = identity_len(
        SessionIdentityField::LocalPeer,
        state.local_peer_id().as_str(),
    )?;
    let maximum = u16::try_from(state.limits().max_peer_records()).map_err(|_| {
        EncodeTransportSessionStateError::PeerLimitTooLarge {
            value: state.limits().max_peer_records(),
        }
    })?;
    let count = u16::try_from(state.peer_count()).map_err(|_| {
        EncodeTransportSessionStateError::PeerCountTooLarge {
            value: state.peer_count(),
        }
    })?;
    if state.peer_count() > state.limits().max_peer_records() {
        return Err(EncodeTransportSessionStateError::PeerCountExceedsLimit {
            count: state.peer_count(),
            maximum: state.limits().max_peer_records(),
        });
    }

    output.extend_from_slice(&SESSION_STATE_MAGIC);
    output.extend_from_slice(&SESSION_STATE_VERSION.to_be_bytes());
    output.push(cluster_len);
    output.extend_from_slice(state.cluster_id().as_str().as_bytes());
    output.push(local_peer_len);
    output.extend_from_slice(state.local_peer_id().as_str().as_bytes());
    output.extend_from_slice(&maximum.to_be_bytes());
    output.extend_from_slice(&count.to_be_bytes());

    for (peer, peer_state) in state.state().peer_states() {
        if peer_state.is_empty() {
            return Err(EncodeTransportSessionStateError::EmptyPeerRecord);
        }
        let peer_len = identity_len(SessionIdentityField::RemotePeer, peer.as_str())?;
        output.push(peer_len);
        output.extend_from_slice(peer.as_str().as_bytes());
        let outbound = peer_state
            .highest_outbound()
            .map_or(0, ConnectionSession::get);
        let inbound = peer_state
            .highest_inbound()
            .map_or(0, ConnectionSession::get);
        output.extend_from_slice(&outbound.to_be_bytes());
        output.extend_from_slice(&inbound.to_be_bytes());
    }
    Ok(())
}

fn identity_len(
    field: SessionIdentityField,
    value: &str,
) -> Result<u8, EncodeTransportSessionStateError> {
    u8::try_from(value.len()).map_err(|_| EncodeTransportSessionStateError::IdentityTooLong {
        field,
        len: value.len(),
    })
}
