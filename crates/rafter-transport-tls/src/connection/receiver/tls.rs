//! Mutual-TLS and durable Rafter server-handshake establishment.

use std::{net::TcpStream, sync::Arc};

use rustls::{ServerConnection, StreamOwned};

use crate::diagnostics::increment;
use crate::{authenticate_server_connection, PeerId, ServerHelloStatus, ServerRefusal};

use super::{classify::classify_authentication, ReceiverTemplate};
use crate::connection::io::{complete_server_tls, read_client_hello, write_server_hello};
use crate::runtime::InboundEpochGuard;

pub(super) struct EstablishedInbound {
    pub(super) stream: StreamOwned<ServerConnection, TcpStream>,
    pub(super) peer: PeerId,
    pub(super) frame_bytes: usize,
    pub(super) epoch: InboundEpochGuard,
}

#[allow(clippy::too_many_lines)]
pub(super) fn establish<G, C>(
    template: &ReceiverTemplate<G, C>,
    mut socket: TcpStream,
    shutdown_socket: Arc<TcpStream>,
) -> Option<EstablishedInbound> {
    if configure_handshake_socket(&socket, template).is_err() {
        increment(&template.counters.tls_failures);
        return None;
    }
    let Ok(mut connection) = template.identity.server_connection() else {
        increment(&template.counters.tls_failures);
        return None;
    };
    if complete_server_tls(&mut connection, &mut socket).is_err() {
        increment(&template.counters.tls_failures);
        return None;
    }
    increment(&template.counters.tls_handshakes);
    let authenticated = match authenticate_server_connection(&connection, &template.certificates) {
        Ok(authenticated) => authenticated,
        Err(error) => {
            classify_authentication(&template.counters, &error);
            return None;
        }
    };
    let mut stream = StreamOwned::new(connection, socket);
    let mut scratch = Vec::new();
    let Ok(client_hello) = read_client_hello(&mut stream, &mut scratch) else {
        increment(&template.counters.malformed_frames);
        return None;
    };
    let mut response = match template.handshake.accept_client_hello(
        &authenticated,
        &client_hello,
        &template.sessions,
    ) {
        Ok(response) => response,
        Err(error) => {
            increment(&template.counters.session_store_failures);
            template.control.fail(error.to_string());
            return None;
        }
    };

    let epoch = if matches!(response.status(), ServerHelloStatus::Accepted) {
        match template.epochs.install(
            authenticated.peer_id().clone(),
            client_hello.connection_session(),
            shutdown_socket,
        ) {
            Ok(Some(epoch)) => Some(epoch),
            Ok(None) => {
                increment(&template.counters.stale_sessions);
                response = template.handshake.refusal(ServerRefusal::StaleSession);
                None
            }
            Err(()) => {
                template.control.fail("inbound epoch state is poisoned");
                return None;
            }
        }
    } else {
        classify_refusal(template, response.status());
        None
    };
    if write_server_hello(&mut stream, &response, &mut scratch).is_err() {
        increment(&template.counters.tls_failures);
        return None;
    }
    let epoch = epoch?;
    let Some(frame_bytes) = response.accepted_frame_bytes() else {
        increment(&template.counters.malformed_frames);
        return None;
    };
    if configure_established_socket(&stream.sock, template).is_err() {
        increment(&template.counters.tls_failures);
        return None;
    }
    let Ok(frame_bytes) = usize::try_from(frame_bytes.get()) else {
        template
            .control
            .fail("negotiated frame bound does not fit local address space");
        return None;
    };
    Some(EstablishedInbound {
        stream,
        peer: authenticated.peer_id().clone(),
        frame_bytes,
        epoch,
    })
}

fn configure_handshake_socket<G, C>(
    socket: &TcpStream,
    template: &ReceiverTemplate<G, C>,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(template.timeouts.handshake()))?;
    socket.set_write_timeout(Some(template.timeouts.handshake()))
}

fn configure_established_socket<G, C>(
    socket: &TcpStream,
    template: &ReceiverTemplate<G, C>,
) -> std::io::Result<()> {
    socket.set_read_timeout(Some(template.timeouts.read()))?;
    socket.set_write_timeout(Some(template.timeouts.write()))
}

fn classify_refusal<G, C>(template: &ReceiverTemplate<G, C>, status: ServerHelloStatus) {
    let ServerHelloStatus::Refused(reason) = status else {
        return;
    };
    match reason {
        ServerRefusal::UnknownCertificate => {
            increment(&template.counters.unknown_certificates);
        }
        ServerRefusal::IdentityMismatch => {
            increment(&template.counters.identity_mismatches);
        }
        ServerRefusal::ClusterMismatch => {
            increment(&template.counters.cluster_mismatches);
        }
        ServerRefusal::TransportVersionMismatch | ServerRefusal::PeerCodecVersionMismatch => {
            increment(&template.counters.version_mismatches);
        }
        ServerRefusal::FrameLimitRejected => {
            increment(&template.counters.frame_too_large);
        }
        ServerRefusal::StaleSession => {
            increment(&template.counters.stale_sessions);
        }
        ServerRefusal::ServerBusy => {
            increment(&template.counters.connection_full);
        }
    }
}
