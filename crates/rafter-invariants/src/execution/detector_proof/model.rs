//! Typed detector-challenge values and terminal exchange classifications.

use std::{error::Error, fmt};

use super::wire;

/// A verifier-owned challenge released only after a valid proof request.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct DetectorChallenge([u8; wire::CHALLENGE_BYTES]);

impl DetectorChallenge {
    pub(super) fn new(bytes: [u8; wire::CHALLENGE_BYTES]) -> Self {
        Self(bytes)
    }

    pub(crate) fn encoded(&self) -> String {
        wire::encode_challenge(&self.0)
    }

    pub(super) fn as_bytes(&self) -> &[u8; wire::CHALLENGE_BYTES] {
        &self.0
    }
}

impl fmt::Debug for DetectorChallenge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DetectorChallenge([REDACTED])")
    }
}

/// Public shape of the private byte-level protocol for adapter validation.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ChallengeProtocol {
    pub(crate) socket_environment: &'static str,
    pub(crate) socket_directory: &'static str,
    pub(crate) challenge_bytes: usize,
    pub(crate) socket_nonce_bytes: usize,
    pub(crate) proof_request: u8,
    pub(crate) zero_challenge_encoding: String,
    pub(crate) zero_socket_nonce_encoding: String,
}

/// Terminal state of one detector challenge exchange.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ChallengeExchange {
    Completed,
    Disconnected,
    MalformedRequest,
    TransportError(TransportError),
}

/// A failure in socket setup, communication, joining, or cleanup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransportError {
    message: String,
}

impl TransportError {
    pub(super) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub(super) fn context(context: &str, error: impl fmt::Display) -> Self {
        Self::new(format!("{context}: {error}"))
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for TransportError {}
