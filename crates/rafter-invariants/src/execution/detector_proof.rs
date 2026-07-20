//! Detector challenge transport independent of process and evidence policy.
//!
//! A [`ChallengeGate`] owns one random challenge and one managed Unix socket.
//! Callers arrange bounded process execution around the gate, then consume it
//! to obtain the typed exchange outcome. This module never launches processes.

mod model;
#[cfg(unix)]
mod responder;
#[cfg(unix)]
mod socket;
mod wire;

pub(crate) use model::{ChallengeExchange, ChallengeProtocol, DetectorChallenge, TransportError};
#[cfg(unix)]
pub(crate) use socket::ChallengeGate;

#[cfg(all(test, unix))]
mod tests;
