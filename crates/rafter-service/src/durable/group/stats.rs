//! Observe retained client credits without borrowing or driving the backend.
use super::DurableCommandSender;
use crate::durable::{DurableReadSender, DurableReadStats, DurableWriteStats};

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

/// Cumulative admission refusals caused by retained count or payload budgets.
///
/// Each counter saturates at `usize::MAX`. These observations do not drive work,
/// reserve capacity, or form an atomic snapshot across queues. Only admission
/// errors reported as `Backpressure` count. Foreign groups, closed admission,
/// unsupported operations, `PayloadTooLarge` validation errors and kernel
/// refusals are excluded. Counts are not persisted across recovery.
/// Refusals count at admission even if the returned error future is never polled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DurableBackpressureStats {
    /// Write budget refusals since construction of the underlying client owner.
    pub writes: usize,
    /// Query budget refusals since owner construction, or `None` without queries.
    pub reads: Option<usize>,
    /// Membership/transfer budget refusals since this driver's attachment.
    pub controls: usize,
}

impl<G, O, R> DurableCommandSender<G, O, R> {
    /// Observes cumulative budget refusals, before or after backend release.
    ///
    /// Write/query counters share the original owner's raw sender budgets and
    /// can predate driver attachment. The control counter belongs to this driver.
    /// Observing them invokes no callbacks. Counts remain available after release
    /// and saturate rather than wrap; independent counters can race with admission.
    #[must_use]
    pub fn backpressure_refusals(&self) -> DurableBackpressureStats {
        DurableBackpressureStats {
            writes: self.shared.writes.backpressure_refusals(),
            reads: self
                .shared
                .reads
                .as_ref()
                .map(DurableReadSender::backpressure_refusals),
            controls: self.shared.budget.refusals(),
        }
    }
}
