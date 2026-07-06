use super::{MetricsWatch, RaftGroupMetrics};

/// Helper for constructing static metrics watches in tests or simple drivers.
#[must_use]
pub fn metrics_watch_from_current<G>(metrics: RaftGroupMetrics<G>) -> MetricsWatch<G> {
    MetricsWatch::new(metrics)
}
