//! Stable dependency-free runtime diagnostics.

mod counters;
mod types;

pub use types::{PeerDiagnostics, QueueDepths, TransportDiagnostics, TransportHealth};

pub(crate) use counters::{add, increment, Counters, PeerCounterMap, PeerCounters};
