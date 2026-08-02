//! One endpoint's TCP, TLS, and Rafter handshake attempt.

use std::{
    net::{SocketAddr, TcpStream},
    sync::atomic::Ordering,
    sync::Arc,
};

use rustls::{ClientConnection, StreamOwned};

use crate::diagnostics::increment;
use crate::{
    authenticate_client_connection, AuthenticatedTlsPeer, EndpointGeneration, PeerEndpoint,
};

use super::classify::{
    classify_authentication, classify_client_handshake, classify_dial_error, endpoint_failed,
    tls_failed,
};
use super::{DialError, OutboundConnection, OutboundPresence};
use crate::connection::{
    deadline::HandshakeDeadline,
    io::{complete_client_tls, read_server_hello, write_client_hello},
    sender::SenderContext,
};

struct TlsChannel {
    address: SocketAddr,
    connection: ClientConnection,
    socket: TcpStream,
    deadline: HandshakeDeadline,
    authenticated: AuthenticatedTlsPeer,
}

pub(super) fn dial_endpoint<G>(
    context: &SenderContext<G>,
    endpoint: &PeerEndpoint,
    endpoint_generation: EndpointGeneration,
    reconnect: bool,
) -> Result<OutboundConnection, DialError> {
    let channel = establish_tls(context, endpoint, endpoint_generation)?;
    let (stream, frame_bytes) = negotiate_rafter(context, channel, endpoint_generation)?;

    let _ = context
        .counters
        .active_outbound
        .fetch_add(1, Ordering::Relaxed);
    context.peer_counters.set_connected(true);
    context.control.mark_connected(&context.peer);
    if reconnect {
        increment(&context.counters.reconnects);
        context.peer_counters.reconnected();
    }
    Ok(OutboundConnection {
        stream,
        sequence: crate::OutboundSequence::new(),
        frame_bytes,
        endpoint_generation,
        _presence: OutboundPresence {
            peer: context.peer.clone(),
            counters: Arc::clone(&context.counters),
            peer_counters: Arc::clone(&context.peer_counters),
            control: Arc::clone(&context.control),
        },
    })
}

fn establish_tls<G>(
    context: &SenderContext<G>,
    endpoint: &PeerEndpoint,
    generation: EndpointGeneration,
) -> Result<TlsChannel, DialError> {
    let address = endpoint.address();
    let Ok(mut socket) = TcpStream::connect_timeout(&address, context.timeouts.connect()) else {
        endpoint_failed(context);
        return Err(DialError::Retry(format!(
            "TCP connection to {address} failed"
        )));
    };
    let Ok(deadline) = configure_handshake_socket(&socket, context) else {
        endpoint_failed(context);
        return Err(DialError::Retry(format!(
            "could not configure handshake I/O for {address}"
        )));
    };
    let Ok(mut connection) = context.identity.client_connection(endpoint.server_name()) else {
        tls_failed(context);
        return Err(DialError::ConfigurationBlocked {
            generation,
            message: format!(
                "TLS client configuration rejected {}",
                endpoint.server_name()
            ),
        });
    };
    if complete_client_tls(&mut connection, &mut socket, deadline).is_err() {
        tls_failed(context);
        return Err(DialError::Retry(format!(
            "TLS handshake with {address} failed"
        )));
    }
    increment(&context.counters.tls_handshakes);
    let authenticated = authenticate_client_connection(&connection, &context.certificates)
        .map_err(|error| {
            classify_authentication(context, &error);
            DialError::ConfigurationBlocked {
                generation,
                message: format!("peer authentication failed for {address}: {error}"),
            }
        })?;
    if authenticated.peer_id() != &context.peer {
        increment(&context.counters.identity_mismatches);
        endpoint_failed(context);
        return Err(DialError::ConfigurationBlocked {
            generation,
            message: format!(
                "endpoint {address} authenticated as {}, expected {}",
                authenticated.peer_id(),
                context.peer
            ),
        });
    }
    Ok(TlsChannel {
        address,
        connection,
        socket,
        deadline,
        authenticated,
    })
}

fn negotiate_rafter<G>(
    context: &SenderContext<G>,
    channel: TlsChannel,
    generation: EndpointGeneration,
) -> Result<(StreamOwned<ClientConnection, TcpStream>, usize), DialError> {
    let TlsChannel {
        address,
        connection,
        socket,
        deadline,
        authenticated,
    } = channel;
    let hello = context
        .handshake
        .begin_client_hello(&context.peer, &context.sessions)
        .map_err(|error| {
            increment(&context.counters.session_store_failures);
            DialError::Terminal(error.to_string())
        })?;
    let mut stream = StreamOwned::new(connection, socket);
    let mut scratch = Vec::new();
    let server_hello = {
        let mut handshake_stream = deadline.stream(&mut stream);
        write_client_hello(&mut handshake_stream, &hello, &mut scratch).map_err(|_| {
            tls_failed(context);
            DialError::Retry(format!("writing the client hello to {address} failed"))
        })?;
        read_server_hello(&mut handshake_stream, &mut scratch).map_err(|_| {
            increment(&context.counters.malformed_frames);
            endpoint_failed(context);
            DialError::Retry(format!("reading the server hello from {address} failed"))
        })?
    };
    let negotiated = context
        .handshake
        .validate_server_hello(&context.peer, &authenticated, &server_hello)
        .map_err(|error| {
            classify_client_handshake(context, &error, server_hello.status());
            let message = format!("Rafter handshake with {address} failed: {error}");
            classify_dial_error(&error, server_hello.status(), generation, message)
        })?;
    configure_established_socket(&stream.sock, context).map_err(|_| {
        tls_failed(context);
        DialError::Retry(format!(
            "configuring the established stream to {address} failed"
        ))
    })?;
    let frame_bytes = usize::try_from(negotiated.frame_bytes()).map_err(|_| {
        DialError::Terminal(format!(
            "negotiated frame bound {} does not fit local address space",
            negotiated.frame_bytes()
        ))
    })?;
    Ok((stream, frame_bytes))
}

fn configure_handshake_socket<G>(
    socket: &TcpStream,
    context: &SenderContext<G>,
) -> std::io::Result<HandshakeDeadline> {
    socket.set_nodelay(true)?;
    let deadline = HandshakeDeadline::new(context.timeouts.handshake())?;
    deadline.configure(socket)?;
    Ok(deadline)
}

fn configure_established_socket<G>(
    socket: &TcpStream,
    context: &SenderContext<G>,
) -> std::io::Result<()> {
    socket.set_read_timeout(Some(context.timeouts.read()))?;
    socket.set_write_timeout(Some(context.timeouts.write()))
}
