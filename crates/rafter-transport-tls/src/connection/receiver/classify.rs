//! Diagnostic classification for rejected TLS, handshake, and frame inputs.

use crate::diagnostics::{increment, Counters};
use crate::{DecodePeerFrameError, TlsPeerAuthenticationError};

use super::super::io::PeerFrameIoError;

pub(super) fn classify_authentication(counters: &Counters, error: &TlsPeerAuthenticationError) {
    match error {
        TlsPeerAuthenticationError::UnknownCertificate { .. } => {
            increment(&counters.unknown_certificates);
        }
        TlsPeerAuthenticationError::HandshakeIncomplete
        | TlsPeerAuthenticationError::MissingAlpn
        | TlsPeerAuthenticationError::UnexpectedAlpn { .. }
        | TlsPeerAuthenticationError::MissingPeerCertificate => {
            increment(&counters.tls_failures);
        }
    }
}

pub(super) fn classify_frame_io(counters: &Counters, error: &PeerFrameIoError) {
    match error {
        PeerFrameIoError::TooLarge { .. } => increment(&counters.frame_too_large),
        PeerFrameIoError::LengthUnsupported(_) => increment(&counters.malformed_frames),
        PeerFrameIoError::ReceiveMemoryFull { .. } => {
            increment(&counters.inbound_full);
            increment(&counters.inbound_memory_full);
        }
        PeerFrameIoError::Io(_) => increment(&counters.tls_failures),
    }
    increment(&counters.frames_dropped);
}

pub(super) fn classify_decode_error<E>(counters: &Counters, error: &DecodePeerFrameError<E>) {
    match error {
        DecodePeerFrameError::FrameTooLarge { .. } => {
            increment(&counters.frame_too_large);
        }
        DecodePeerFrameError::SenderMismatch { .. } => {
            increment(&counters.identity_mismatches);
        }
        DecodePeerFrameError::ZeroSequence => {
            increment(&counters.sequence_violations);
        }
        DecodePeerFrameError::TruncatedLengthPrefix
        | DecodePeerFrameError::FrameLengthUnsupported(_)
        | DecodePeerFrameError::TruncatedFrame { .. }
        | DecodePeerFrameError::TrailingBytes { .. }
        | DecodePeerFrameError::TruncatedBody
        | DecodePeerFrameError::UnknownFrameKind(_)
        | DecodePeerFrameError::EmptyGroupId
        | DecodePeerFrameError::GroupIdTooLarge { .. }
        | DecodePeerFrameError::MessageLengthUnsupported(_)
        | DecodePeerFrameError::MessageLengthMismatch { .. }
        | DecodePeerFrameError::GroupDecode(_)
        | DecodePeerFrameError::GroupReencode(_)
        | DecodePeerFrameError::NonCanonicalGroupId
        | DecodePeerFrameError::MessageDecode(_) => {
            increment(&counters.malformed_frames);
        }
    }
    increment(&counters.frames_dropped);
}
