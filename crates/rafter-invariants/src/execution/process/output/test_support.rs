//! Deterministic collection-loop readiness and poll-ordering controls.

use std::{
    cell::{Cell, RefCell},
    error::Error,
    time::{Duration, Instant},
};

use super::super::{ProcessArtifacts, PROCESS_POLL_INTERVAL};

thread_local! {
    static NEXT_TARGET_STDOUT_READINESS: RefCell<Option<TargetStdoutReadiness>> =
        const { RefCell::new(None) };
    static HOLD_NEXT_POLL: Cell<bool> = const { Cell::new(false) };
}

struct TargetStdoutReadiness {
    prefix: Vec<u8>,
    timeout: Duration,
}

pub(crate) struct TargetStdoutReadinessGuard;

pub(crate) fn await_next_target_stdout_prefix(
    prefix: &[u8],
    timeout: Duration,
) -> TargetStdoutReadinessGuard {
    assert!(
        !prefix.is_empty(),
        "target readiness prefix must not be empty"
    );
    assert!(
        !timeout.is_zero(),
        "target readiness timeout must be positive"
    );
    NEXT_TARGET_STDOUT_READINESS.with(|next| {
        assert!(
            next.borrow_mut()
                .replace(TargetStdoutReadiness {
                    prefix: prefix.to_vec(),
                    timeout,
                })
                .is_none(),
            "target stdout readiness hook was already armed"
        );
    });
    TargetStdoutReadinessGuard
}

impl Drop for TargetStdoutReadinessGuard {
    fn drop(&mut self) {
        NEXT_TARGET_STDOUT_READINESS.with(|next| {
            next.borrow_mut().take();
        });
    }
}

pub(super) fn await_target_stdout_readiness(
    artifacts: &ProcessArtifacts,
    execution_window: Instant,
) -> Result<(), Box<dyn Error>> {
    const MAX_READINESS_STDOUT_BYTES: u64 = 64 * 1024 * 1024;

    let readiness = NEXT_TARGET_STDOUT_READINESS.with(|next| next.borrow_mut().take());
    let Some(readiness) = readiness else {
        return Ok(());
    };
    let deadline = Instant::now()
        .checked_add(readiness.timeout)
        .ok_or("target stdout readiness deadline overflow")?
        .min(execution_window);
    let operation_deadline =
        crate::execution::filesystem::OperationDeadline::at(deadline, "target stdout readiness");
    loop {
        let stdout = artifacts.read_stdout(operation_deadline, MAX_READINESS_STDOUT_BYTES)?;
        if stdout.starts_with(&readiness.prefix) {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(format!(
                "target did not publish its stdout readiness prefix within {} ms",
                readiness.timeout.as_millis()
            )
            .into());
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL.min(deadline.duration_since(now)));
    }
}

/// Hold the next collection poll until the execution window has closed.
///
/// The window edge lands inside the poll far more often than anywhere else --
/// the poll is a hundred milliseconds and the observation it separates costs
/// single-digit ones -- but "far more often" is not something a fixture can
/// assert. Waiting for the real window states that ordering exactly, the way
/// `await_next_internal_completion_after_deadline` states its own, instead of
/// guessing with a fixed sleep which side of the edge a run landed on.
pub(crate) fn hold_next_poll_until_the_execution_window_closes() {
    HOLD_NEXT_POLL.with(|hold| hold.set(true));
}

pub(super) fn hold_poll_across_the_execution_window_if_requested(execution_window: Instant) {
    if HOLD_NEXT_POLL.with(|hold| hold.replace(false)) {
        std::thread::sleep(execution_window.saturating_duration_since(Instant::now()));
    }
}
