//! Detector challenge transport independent of process and evidence policy.
//!
//! A [`ChallengeGate`] owns one random challenge and one managed Unix socket.
//! Callers arrange bounded process execution around the gate, then consume it
//! to obtain the typed exchange outcome. This module never launches processes.

#[cfg(unix)]
mod channel;
mod model;
#[cfg(unix)]
mod responder;
mod wire;

#[cfg(unix)]
pub(crate) use channel::ChallengeGate;
pub(crate) use model::{ChallengeExchange, ChallengeProtocol, DetectorChallenge, TransportError};

#[cfg(all(test, unix))]
mod tests;
