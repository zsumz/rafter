//! Endpoint selection, TLS authentication, and Rafter client negotiation.

mod classify;
mod endpoint;

use std::{
    collections::BTreeSet,
    net::TcpStream,
    sync::{atomic::Ordering, Arc},
};

use rustls::{ClientConnection, StreamOwned};

use crate::diagnostics::increment;
use crate::{EndpointGeneration, PeerEndpoint};

use self::classify::endpoint_failed;
use self::endpoint::dial_endpoint;

use super::sender::SenderContext;

#[derive(Debug)]
pub(crate) enum DialError {
    Retry(String),
    ConfigurationBlocked {
        generation: EndpointGeneration,
        message: String,
    },
    Terminal(String),
}

pub(crate) struct OutboundConnection {
    pub(crate) stream: StreamOwned<ClientConnection, TcpStream>,
    pub(crate) sequence: crate::OutboundSequence,
    pub(crate) frame_bytes: usize,
    pub(crate) endpoint_generation: EndpointGeneration,
    _presence: OutboundPresence,
}

#[derive(Debug, Default)]
pub(crate) struct DialAttemptState {
    generation: Option<EndpointGeneration>,
    blocked: BTreeSet<PeerEndpoint>,
    last_blocked_error: Option<String>,
}

impl DialAttemptState {
    fn observe(&mut self, generation: EndpointGeneration) {
        if self.generation != Some(generation) {
            self.generation = Some(generation);
            self.blocked.clear();
            self.last_blocked_error = None;
        }
    }

    fn block(&mut self, endpoint: &PeerEndpoint, message: String) {
        let _ = self.blocked.insert(endpoint.clone());
        self.last_blocked_error = Some(message);
    }
}

impl std::fmt::Debug for OutboundConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OutboundConnection")
            .field("frame_bytes", &self.frame_bytes)
            .field("endpoint_generation", &self.endpoint_generation)
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

pub(crate) fn dial<G>(
    context: &SenderContext<G>,
    reconnect: bool,
    attempts: &mut DialAttemptState,
) -> Result<OutboundConnection, DialError> {
    let snapshot = context.endpoints.snapshot(&context.peer).map_err(|error| {
        DialError::Terminal(format!(
            "endpoint book failed for {}: {error}",
            context.peer
        ))
    })?;
    let Some(snapshot) = snapshot else {
        endpoint_failed(context);
        return Err(DialError::Retry(format!(
            "no endpoints are installed for {}",
            context.peer
        )));
    };
    attempts.observe(snapshot.generation());

    let mut blocked = None;
    let mut transient = None;
    for endpoint in snapshot.endpoints() {
        if attempts.blocked.contains(endpoint) {
            continue;
        }
        if context.control.terminal() || context.control.shutdown_grace_expired() {
            return Err(DialError::Retry("transport is stopping".to_owned()));
        }
        match dial_endpoint(context, endpoint, snapshot.generation(), reconnect) {
            Ok(connection) => return Ok(connection),
            Err(DialError::Retry(message)) => transient = Some(message),
            Err(DialError::ConfigurationBlocked { message, .. }) => {
                increment(&context.counters.configuration_blocks);
                attempts.block(endpoint, message.clone());
                blocked = Some(message);
            }
            Err(error @ DialError::Terminal(_)) => return Err(error),
        }
    }
    if let Some(message) = transient {
        let message = match &attempts.last_blocked_error {
            Some(blocked) => {
                format!("{message}; another endpoint is configuration-blocked: {blocked}")
            }
            None => message,
        };
        Err(DialError::Retry(message))
    } else {
        Err(DialError::ConfigurationBlocked {
            generation: snapshot.generation(),
            message: blocked
                .or_else(|| attempts.last_blocked_error.clone())
                .unwrap_or_else(|| "all configured endpoints were refused".to_owned()),
        })
    }
}
