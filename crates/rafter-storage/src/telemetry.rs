//! Opt-in per-thread diagnostics for steady-state persistence and kernel work.
//!
//! Disabled by default. Enable on the thread that owns the runtime, and read
//! snapshots there. Counters cover attempted instrumented operations, including
//! failures; they exclude recovery, snapshot publication, and log rewrites.
//! Timing runs should disable diagnostics and use a separate diagnostic run.

use std::{cell::RefCell, time::Instant};

/// An instrumented steady-state operation, not an inferred durability promise.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
#[repr(usize)]
pub enum Stage {
    /// Deterministic kernel execution, excluding persistence.
    Kernel,
    /// Log frame encoding and validation for an append batch.
    LogEncode,
    /// Log file write syscall loop.
    LogWrite,
    /// Log file data sync.
    LogSync,
    /// Hard-state encoding.
    HardStateEncode,
    /// Hard-state file write syscall loop.
    HardStateWrite,
    /// Hard-state file data sync.
    HardStateSync,
    /// Replacement hard-state parent-directory sync.
    HardStateDirectorySync,
}

const NAMES: [&str; 8] = [
    "kernel",
    "log_encode",
    "log_write",
    "log_sync",
    "hard_state_encode",
    "hard_state_write",
    "hard_state_sync",
    "hard_state_directory_sync",
];

/// Cumulative timings on the owning thread; no percentile subtraction is valid.
#[derive(Clone, Debug, Default)]
pub struct Metric {
    /// Attempted operations, including failed operations.
    pub calls: u64,
    /// Sum of elapsed nanoseconds.
    pub total_ns: u64,
    /// Maximum elapsed nanoseconds.
    pub max_ns: u64,
    /// Base-two nanosecond bucket counts; bucket 0 contains zero and one.
    pub buckets: Vec<u64>,
}

#[derive(Default)]
struct State {
    enabled: bool,
    metrics: [Metric; 8],
}
thread_local! {
    static STATE: RefCell<State> = RefCell::new(State::default());
}

/// Enables or disables diagnostics on the calling thread without resetting counts.
pub fn set_enabled(enabled: bool) {
    STATE.with_borrow_mut(|state| state.enabled = enabled);
}

/// Returns named cumulative counters for the calling thread.
#[must_use]
pub fn snapshot() -> Vec<(&'static str, Metric)> {
    STATE.with_borrow(|state| {
        NAMES
            .into_iter()
            .zip(state.metrics.iter().cloned())
            .collect()
    })
}

/// Times one operation; dropping the guard also records a failed early return.
#[derive(Debug)]
pub struct Timer {
    stage: Stage,
    started: Option<Instant>,
}
impl Timer {
    /// Starts a timer only when diagnostics are enabled on this thread.
    #[must_use]
    pub fn start(stage: Stage) -> Self {
        Self {
            stage,
            started: STATE.with_borrow(|state| state.enabled.then(Instant::now)),
        }
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            let ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            STATE.with_borrow_mut(|state| {
                let metric = &mut state.metrics[self.stage as usize];
                metric.calls = metric.calls.saturating_add(1);
                metric.total_ns = metric.total_ns.saturating_add(ns);
                metric.max_ns = metric.max_ns.max(ns);
                metric.buckets.resize(64, 0);
                let bucket = 63 - ns.max(1).leading_zeros() as usize;
                metric.buckets[bucket] = metric.buckets[bucket].saturating_add(1);
            });
        }
    }
}

/// Runs and records one operation when diagnostics are enabled.
pub fn measure<T>(stage: Stage, operation: impl FnOnce() -> T) -> T {
    let _timer = Timer::start(stage);
    operation()
}

#[cfg(test)]
#[path = "telemetry_test.rs"]
mod tests;
