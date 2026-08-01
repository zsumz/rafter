//! Endpoint selection, TLS authentication, and Rafter client negotiation.

use std::{
    net::TcpStream,
    sync::{atomic::Ordering, Arc},
};

use rustls::{ClientConnection, StreamOwned};

use crate::diagnostics::increment;
use crate::{
    authenticate_client_connection, ServerHelloStatus, ServerRefusal, TlsClientHandshakeError,
    TlsPeerAuthenticationError,
};

use super::{
    io::{complete_client_tls, read_server_hello, write_client_hello},
    sender::SenderContext,
};

#[derive(Debug)]
pub(crate) enum DialError {
    Retry,
    Terminal(String),
}

pub(crate) struct OutboundConnection {
    pub(crate) stream: StreamOwned<ClientConnection, TcpStream>,
    pub(crate) sequence: crate::OutboundSequence,
    pub(crate) frame_bytes: usize,
    _presence: OutboundPresence,
}

impl std::fmt::Debug for OutboundConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OutboundConnection")
            .field("frame_bytes", &self.frame_bytes)
            .field("sequence", &self.sequence)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct OutboundPresence {
    peer: crate::PeerId,
    counters: Arc<crate::diagnostics::Counters>,
    peer_counters: Arc<crate::diagnostics::PeerCounters>,
    control: Arc<crate::runtime::RuntimeControl>,
}

impl Drop for OutboundPresence {
    fn drop(&mut self) {
        self.peer_counters.set_connected(false);
        let _ = self.counters.active_outbound.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| Some(value.saturating_sub(1)),
        );
        self.control.mark_degraded(&self.peer);
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn dial<G, C>(
    context: &SenderContext<G, C>,
    reconnect: bool,
) -> Result<OutboundConnection, DialError> {
    let snapshot = context.endpoints.snapshot(&context.peer).map_err(|error| {
        DialError::Terminal(format!(
            "endpoint book failed for {}: {error}",
            context.peer
        ))
    })?;
    let Some(snapshot) = snapshot else {
        endpoint_failed(context);
        return Err(DialError::Retry);
    };

    for endpoint in snapshot.endpoints() {
        if context.control.terminal() || context.control.shutdown_grace_expired() {
            return Err(DialError::Retry);
        }
        let address = endpoint.address();
        let Ok(mut socket) = TcpStream::connect_timeout(&address, context.timeouts.connect())
        else {
            endpoint_failed(context);
            continue;
        };
        if configure_handshake_socket(&socket, context).is_err() {
            endpoint_failed(context);
            continue;
        }
        let Ok(mut connection) = context.identity.client_connection(endpoint.server_name()) else {
            tls_failed(context);
            continue;
        };
        if complete_client_tls(&mut connection, &mut socket).is_err() {
            tls_failed(context);
            continue;
        }
        increment(&context.counters.tls_handshakes);
        let authenticated = match authenticate_client_connection(&connection, &context.certificates)
        {
            Ok(authenticated) => authenticated,
            Err(error) => {
                classify_authentication(context, &error);
                continue;
            }
        };
        if authenticated.peer_id() != &context.peer {
            increment(&context.counters.identity_mismatches);
            endpoint_failed(context);
            continue;
        }

        let hello = context
            .handshake
            .begin_client_hello(&context.peer, &context.sessions)
            .map_err(|error| {
                increment(&context.counters.session_store_failures);
                DialError::Terminal(error.to_string())
            })?;
        let mut stream = StreamOwned::new(connection, socket);
        let mut scratch = Vec::new();
        if write_client_hello(&mut stream, &hello, &mut scratch).is_err() {
            tls_failed(context);
            continue;
        }
        let Ok(server_hello) = read_server_hello(&mut stream, &mut scratch) else {
            increment(&context.counters.malformed_frames);
            endpoint_failed(context);
            continue;
        };
        let negotiated = match context.handshake.validate_server_hello(
            &context.peer,
            &authenticated,
            &server_hello,
        ) {
            Ok(negotiated) => negotiated,
            Err(error) => {
                classify_client_handshake(context, &error, server_hello.status());
                continue;
            }
        };
        if configure_established_socket(&stream.sock, context).is_err() {
            tls_failed(context);
            continue;
        }

        let frame_bytes = usize::try_from(negotiated.frame_bytes()).map_err(|_| {
            DialError::Terminal(format!(
                "negotiated frame bound {} does not fit local address space",
                negotiated.frame_bytes()
            ))
        })?;
        context
            .counters
            .active_outbound
            .fetch_add(1, Ordering::Relaxed);
        context.peer_counters.set_connected(true);
        context.control.mark_connected(&context.peer);
        if reconnect {
            increment(&context.counters.reconnects);
            context.peer_counters.reconnected();
        }
        return Ok(OutboundConnection {
            stream,
            sequence: crate::OutboundSequence::new(),
            frame_bytes,
            _presence: OutboundPresence {
                peer: context.peer.clone(),
                counters: Arc::clone(&context.counters),
                peer_counters: Arc::clone(&context.peer_counters),
                control: Arc::clone(&context.control),
            },
        });
    }
    Err(DialError::Retry)
}

fn configure_handshake_socket<G, C>(
    socket: &TcpStream,
    context: &SenderContext<G, C>,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(context.timeouts.handshake()))?;
    socket.set_write_timeout(Some(context.timeouts.handshake()))
}

fn configure_established_socket<G, C>(
    socket: &TcpStream,
    context: &SenderContext<G, C>,
) -> std::io::Result<()> {
    socket.set_read_timeout(Some(context.timeouts.read()))?;
    socket.set_write_timeout(Some(context.timeouts.write()))
}

fn endpoint_failed<G, C>(context: &SenderContext<G, C>) {
    increment(&context.counters.endpoint_failures);
    context.peer_counters.endpoint_failed();
    context.control.mark_degraded(&context.peer);
}

fn tls_failed<G, C>(context: &SenderContext<G, C>) {
    increment(&context.counters.tls_failures);
    endpoint_failed(context);
}

fn classify_authentication<G, C>(
    context: &SenderContext<G, C>,
    error: &TlsPeerAuthenticationError,
) {
    match error {
        TlsPeerAuthenticationError::UnknownCertificate { .. } => {
            increment(&context.counters.unknown_certificates);
        }
        TlsPeerAuthenticationError::HandshakeIncomplete
        | TlsPeerAuthenticationError::MissingAlpn
        | TlsPeerAuthenticationError::UnexpectedAlpn { .. }
        | TlsPeerAuthenticationError::MissingPeerCertificate => {
            increment(&context.counters.tls_failures);
        }
    }
    endpoint_failed(context);
}

fn classify_client_handshake<G, C>(
    context: &SenderContext<G, C>,
    error: &TlsClientHandshakeError,
    status: ServerHelloStatus,
) {
    match error {
        TlsClientHandshakeError::AuthenticatedPeerMismatch { .. }
        | TlsClientHandshakeError::ServerIdentityMismatch { .. } => {
            increment(&context.counters.identity_mismatches);
        }
        TlsClientHandshakeError::ClusterMismatch { .. } => {
            increment(&context.counters.cluster_mismatches);
        }
        TlsClientHandshakeError::TransportVersionNotOffered { .. }
        | TlsClientHandshakeError::PeerCodecVersionNotOffered { .. } => {
            increment(&context.counters.version_mismatches);
        }
        TlsClientHandshakeError::FrameLimitInvalid { .. }
        | TlsClientHandshakeError::NonCanonicalAccepted => {
            increment(&context.counters.malformed_frames);
        }
        TlsClientHandshakeError::Refused { reason } => match reason {
            ServerRefusal::UnknownCertificate => {
                increment(&context.counters.unknown_certificates);
            }
            ServerRefusal::IdentityMismatch => {
                increment(&context.counters.identity_mismatches);
            }
            ServerRefusal::ClusterMismatch => {
                increment(&context.counters.cluster_mismatches);
            }
            ServerRefusal::TransportVersionMismatch | ServerRefusal::PeerCodecVersionMismatch => {
                increment(&context.counters.version_mismatches);
            }
            ServerRefusal::FrameLimitRejected => {
                increment(&context.counters.frame_too_large);
            }
            ServerRefusal::StaleSession => {
                increment(&context.counters.stale_sessions);
            }
            ServerRefusal::ServerBusy => {
                increment(&context.counters.connection_full);
            }
        },
    }
    if matches!(status, ServerHelloStatus::Accepted) {
        increment(&context.counters.malformed_frames);
    }
    endpoint_failed(context);
}
