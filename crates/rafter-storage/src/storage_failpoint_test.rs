//! Thread-scoped, one-shot filesystem failpoints for deterministic crash tests.
//!
//! Production builds do not compile this module. Storage implementations call
//! it only from `#[cfg(test)]` blocks at named publication boundaries, keeping
//! normal code free of fault-injection state and synchronization.

use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    thread::ThreadId,
};

/// Named storage boundaries where a test may inject one synthetic I/O failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DurabilityPoint {
    HardStateAfterTempSync,
    HardStateAfterRename,
    HardStateAfterDirectorySync,
    LogAppendAfterSync,
    LogRewriteAfterTempSync,
    LogRewriteBeforeRename,
    LogRewriteAfterRename,
    LogRewriteAfterDirectorySync,
    LogMarkerAfterTempSync,
    LogMarkerAfterRename,
    LogMarkerAfterDirectorySync,
    SnapshotAfterTempSync,
    SnapshotAfterFileRename,
    SnapshotAfterFileDirectorySync,
    SnapshotAfterManifestTempSync,
    SnapshotAfterManifestRename,
    SnapshotAfterManifestDirectorySync,
    PendingClearAfterManifestRemoval,
    PendingClearAfterBodyRemoval,
    PendingClearAfterDirectorySync,
}

#[derive(Debug)]
struct Armed {
    thread: ThreadId,
    point: DurabilityPoint,
    triggered: Arc<AtomicBool>,
}

static ACTIVE_FAILPOINTS: Mutex<Vec<Armed>> = Mutex::new(Vec::new());

/// Clears an armed failpoint when a scenario exits before reaching it.
#[derive(Debug)]
pub(crate) struct Guard {
    thread: ThreadId,
    point: DurabilityPoint,
    triggered: Arc<AtomicBool>,
}

/// Arms one failpoint on the current test thread.
///
/// Storage operations are synchronous, so thread-local state prevents unrelated
/// parallel tests from observing the injection.
#[must_use]
pub(crate) fn arm(point: DurabilityPoint) -> Guard {
    let thread = std::thread::current().id();
    let triggered = Arc::new(AtomicBool::new(false));
    let mut active = active_failpoints();
    assert!(
        active.iter().all(|armed| armed.thread != thread),
        "a storage failpoint is already armed on this test thread"
    );
    active.push(Armed {
        thread,
        point,
        triggered: Arc::clone(&triggered),
    });
    Guard {
        thread,
        point,
        triggered,
    }
}

/// Returns one synthetic I/O failure when `point` is the armed boundary.
pub(crate) fn check(point: DurabilityPoint) -> io::Result<()> {
    let thread = std::thread::current().id();
    let mut active = active_failpoints();
    let Some(index) = active
        .iter()
        .position(|armed| armed.thread == thread && armed.point == point)
    else {
        return Ok(());
    };

    let armed = active.swap_remove(index);
    armed.triggered.store(true, Ordering::Relaxed);
    Err(io::Error::other(format!(
        "injected storage failpoint: {point:?}"
    )))
}

impl Guard {
    /// Proves that the scenario reached and consumed the armed boundary.
    #[track_caller]
    pub(crate) fn assert_triggered(&self) {
        assert!(
            self.triggered.load(Ordering::Relaxed),
            "storage failpoint {:?} was not reached",
            self.point
        );
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let mut active = active_failpoints();
        if let Some(index) = active.iter().position(|armed| {
            armed.thread == self.thread
                && armed.point == self.point
                && Arc::ptr_eq(&armed.triggered, &self.triggered)
        }) {
            active.swap_remove(index);
        }
    }
}

fn active_failpoints() -> MutexGuard<'static, Vec<Armed>> {
    ACTIVE_FAILPOINTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

mod hard_state_test;
mod log_test;
mod snapshot_test;
mod support_test;
