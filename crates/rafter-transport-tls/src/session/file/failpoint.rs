//! Thread-scoped one-shot durability failpoints for session-store unit tests.

use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    thread::ThreadId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DurabilityPoint {
    CreateAfterTempSync,
    CreateAfterRename,
    CreateAfterDirectorySync,
    ReplaceAfterTempSync,
    ReplaceAfterRename,
    ReplaceAfterDirectorySync,
}

#[derive(Debug)]
struct Armed {
    thread: ThreadId,
    point: DurabilityPoint,
    triggered: Arc<AtomicBool>,
}

static ACTIVE: Mutex<Vec<Armed>> = Mutex::new(Vec::new());

#[derive(Debug)]
pub(super) struct Guard {
    thread: ThreadId,
    point: DurabilityPoint,
    triggered: Arc<AtomicBool>,
}

#[must_use]
pub(super) fn arm(point: DurabilityPoint) -> Guard {
    let thread = std::thread::current().id();
    let triggered = Arc::new(AtomicBool::new(false));
    let mut active = active();
    assert!(
        active.iter().all(|armed| armed.thread != thread),
        "a session-store failpoint is already armed on this test thread"
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

pub(super) fn check(point: DurabilityPoint) -> io::Result<()> {
    let thread = std::thread::current().id();
    let mut active = active();
    let Some(index) = active
        .iter()
        .position(|armed| armed.thread == thread && armed.point == point)
    else {
        return Ok(());
    };
    let armed = active.swap_remove(index);
    armed.triggered.store(true, Ordering::Relaxed);
    Err(io::Error::other(format!(
        "injected session-store failpoint: {point:?}"
    )))
}

impl Guard {
    #[track_caller]
    pub(super) fn assert_triggered(&self) {
        assert!(
            self.triggered.load(Ordering::Relaxed),
            "session-store failpoint {:?} was not reached",
            self.point
        );
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let mut active = active();
        if let Some(index) = active.iter().position(|armed| {
            armed.thread == self.thread
                && armed.point == self.point
                && Arc::ptr_eq(&armed.triggered, &self.triggered)
        }) {
            active.swap_remove(index);
        }
    }
}

fn active() -> MutexGuard<'static, Vec<Armed>> {
    ACTIVE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
