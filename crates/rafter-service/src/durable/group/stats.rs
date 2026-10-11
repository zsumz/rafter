//! Observe retained client credits without borrowing or driving the backend.
use super::DurableCommandSender;
use crate::durable::{DurableReadStats, DurableWriteStats};

/// Retained admission credits for one grouped durable endpoint.
///
/// Includes queued and submitted work, canceled work still owned by the driver,
/// and completed results awaiting collection. Each counter is observational;
/// concurrent admission or release can change usage between reads. This is not
/// an atomic snapshot across counters or a reservation of available capacity.
/// Submission remains authoritative and can report backpressure after this view.
///
/// Payload charges exclude application results, transient dispatch allocations,
/// kernel logs, transport queues, and other embedding-owned memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DurableCommandStats {
    /// Retained writes and their encoded payload capacity charges.
    pub writes: DurableWriteStats,
    /// Retained queries, or `None` when no query adapter was configured.
    pub reads: Option<DurableReadStats>,
    /// Retained membership/leadership-transfer credits, sharing one slot.
    pub controls: usize,
    /// Normalized target and promotion-barrier capacity charged to that slot.
    /// A scalar membership request or leadership transfer charges zero bytes.
    pub control_payload_bytes: usize,
}

impl<G, O, R> DurableCommandSender<G, O, R> {
    /// Observes retained count and payload charges without polling the driver.
    ///
    /// Reads no backend state, invokes no callbacks, and remains available after
    /// backend release. Uncollected results can still hold credits at that point.
    /// The counters may change independently during concurrent admission/release;
    /// they neither prove readiness nor reserve capacity for a later request.
    #[must_use]
    pub fn retained(&self) -> DurableCommandStats {
        let (controls, control_payload_bytes) = self.shared.budget.stats();
        DurableCommandStats {
            writes: self.shared.writes.shared.stats(),
            reads: self.shared.reads.as_ref().map(|r| r.shared.stats()),
            controls,
            control_payload_bytes,
        }
    }
}
