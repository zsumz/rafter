//! Dial-failure counters and retry classification.

use crate::diagnostics::increment;
use crate::{
    EndpointGeneration, ServerHelloStatus, ServerRefusal, TlsClientHandshakeError,
    TlsPeerAuthenticationError,
};

use super::super::sender::SenderContext;
use super::DialError;

pub(super) fn classify_dial_error(
    error: &TlsClientHandshakeError,
    status: ServerHelloStatus,
    generation: EndpointGeneration,
    message: String,
) -> DialError {
    if matches!(
        error,
        TlsClientHandshakeError::Refused {
            reason: ServerRefusal::ServerBusy,
        }
    ) {
        return DialError::Retry(message);
    }
    if matches!(
        error,
        TlsClientHandshakeError::Refused {
            reason: ServerRefusal::StaleSession,
        } | TlsClientHandshakeError::NonCanonicalAccepted
    ) || matches!(status, ServerHelloStatus::Accepted)
        && matches!(&error, TlsClientHandshakeError::FrameLimitInvalid { .. })
    {
        return DialError::Terminal(message);
    }
    DialError::ConfigurationBlocked {
        generation,
        message,
    }
}

pub(super) fn endpoint_failed<G>(context: &SenderContext<G>) {
    increment(&context.counters.endpoint_failures);
    context.peer_counters.endpoint_failed();
    context.control.mark_degraded(&context.peer);
}

pub(super) fn tls_failed<G>(context: &SenderContext<G>) {
    increment(&context.counters.tls_failures);
    endpoint_failed(context);
}

pub(super) fn classify_authentication<G>(
    context: &SenderContext<G>,
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

pub(super) fn classify_client_handshake<G>(
    context: &SenderContext<G>,
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
        TlsClientHandshakeError::Refused { reason } => classify_refusal(context, *reason),
    }
    if matches!(status, ServerHelloStatus::Accepted) {
        increment(&context.counters.malformed_frames);
    }
    endpoint_failed(context);
}

fn classify_refusal<G>(context: &SenderContext<G>, reason: ServerRefusal) {
    let counter = match reason {
        ServerRefusal::UnknownCertificate => &context.counters.unknown_certificates,
        ServerRefusal::IdentityMismatch => &context.counters.identity_mismatches,
        ServerRefusal::ClusterMismatch => &context.counters.cluster_mismatches,
        ServerRefusal::TransportVersionMismatch | ServerRefusal::PeerCodecVersionMismatch => {
            &context.counters.version_mismatches
        }
        ServerRefusal::FrameLimitRejected => &context.counters.frame_too_large,
        ServerRefusal::StaleSession => &context.counters.stale_sessions,
        ServerRefusal::ServerBusy => &context.counters.connection_full,
    };
    increment(counter);
}
