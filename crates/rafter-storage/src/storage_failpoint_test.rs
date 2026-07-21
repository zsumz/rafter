//! Thread-local, one-shot filesystem failpoints for deterministic crash tests.
//!
//! Production builds do not compile this module. Storage implementations call
//! it only from `#[cfg(test)]` blocks at named publication boundaries, keeping
//! normal code free of fault-injection state and synchronization.

use std::{
    cell::{Cell, RefCell},
    io,
    rc::Rc,
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
    point: DurabilityPoint,
    triggered: Rc<Cell<bool>>,
}

thread_local! {
    static ACTIVE_FAILPOINT: RefCell<Option<Armed>> = const {
        RefCell::new(None)
    };
}

/// Clears an armed failpoint when a scenario exits before reaching it.
#[derive(Debug)]
pub(crate) struct Guard {
    point: DurabilityPoint,
    triggered: Rc<Cell<bool>>,
}

/// Arms one failpoint on the current test thread.
///
/// Storage operations are synchronous, so thread-local state prevents unrelated
/// parallel tests from observing the injection.
#[must_use]
pub(crate) fn arm(point: DurabilityPoint) -> Guard {
    let triggered = Rc::new(Cell::new(false));
    ACTIVE_FAILPOINT.with(|slot| {
        let mut active = slot.borrow_mut();
        assert!(
            active.is_none(),
            "a storage failpoint is already armed on this test thread"
        );
        *active = Some(Armed {
            point,
            triggered: Rc::clone(&triggered),
        });
    });
    Guard { point, triggered }
}

/// Returns one synthetic I/O failure when `point` is the armed boundary.
pub(crate) fn check(point: DurabilityPoint) -> io::Result<()> {
    ACTIVE_FAILPOINT.with(|slot| {
        let mut active = slot.borrow_mut();
        let matches = active.as_ref().is_some_and(|armed| armed.point == point);
        if !matches {
            return Ok(());
        }

        let armed = active
            .take()
            .expect("matching failpoint must still be armed");
        armed.triggered.set(true);
        Err(io::Error::other(format!(
            "injected storage failpoint: {point:?}"
        )))
    })
}

impl Guard {
    /// Proves that the scenario reached and consumed the armed boundary.
    #[track_caller]
    pub(crate) fn assert_triggered(&self) {
        assert!(
            self.triggered.get(),
            "storage failpoint {:?} was not reached",
            self.point
        );
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        ACTIVE_FAILPOINT.with(|slot| {
            let should_clear = slot
                .borrow()
                .as_ref()
                .is_some_and(|armed| armed.point == self.point);
            if should_clear {
                *slot.borrow_mut() = None;
            }
        });
    }
}

mod hard_state_test;
mod log_test;
mod snapshot_test;
mod support_test;
