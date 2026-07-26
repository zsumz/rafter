//! Aggregated many-group metrics.

use rafter_app::metrics::RaftGroupMetrics;

use crate::error::MultiRaftError;

/// Metrics for every group currently open in a host.
///
/// This aggregates nothing: it concatenates each group's own
/// [`RaftGroupMetrics`] in key order. No sum, rate, or cross-group derivation
/// is computed here, and none should be inferred from the name.
///
/// A snapshot is never all-or-nothing. One driver that misreports its own
/// identity used to make this whole surface an `Err`, which blinded an
/// operator to every healthy group in the process at the moment they most
/// needed to see them.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct MultiRaftMetrics<G> {
    /// Every group whose driver reported the key it is open under, in key
    /// order.
    pub groups: Vec<RaftGroupMetrics<G>>,
    /// The groups excluded from `groups`, and why.
    ///
    /// A driver that reports another group's identity is excluded rather than
    /// published under a key it disowns, because publishing it would put a
    /// fabricated `group_id` into a metrics stream. It is listed here rather
    /// than dropped, so an operator sees a gap and its reason instead of a
    /// silently shorter list.
    pub failures: Vec<MultiRaftError<G>>,
}

impl<G> MultiRaftMetrics<G> {
    /// Whether every open group is present in `groups`.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }
}
