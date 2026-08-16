//! Aggregate topology for measured target execution and its two direct children.

mod cleanup;
mod target;

use std::{
    process::{Child, ExitStatus},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use super::{
    DirectChild, NoSignalReaper, ProcessGroupAnchor, ProcessObserver, TargetLifetimeLease,
};
#[cfg(test)]
pub(crate) use cleanup::force_next_cleanup_target_alive;
pub(crate) use target::TargetObservation;
use target::TargetPlacement;
#[cfg(test)]
pub(crate) use target::{
    before_next_wrapper_exit_observation, classify_target_quiescence_for_test,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct CleanupFailures {
    failures: Arc<Mutex<Vec<String>>>,
}

impl CleanupFailures {
    pub(super) fn record(&self, error: String) {
        self.lock().push(error);
    }

    pub(crate) fn take(&self) -> Vec<String> {
        let mut failures = std::mem::take(&mut *self.lock());
        failures.sort();
        failures
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<String>> {
        self.failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CleanupState {
    Armed,
    Cleaning,
    Complete,
    Failed,
}

/// Owns the resource wrapper and the separate direct child anchoring its target group.
#[derive(Debug)]
pub(crate) struct ManagedProcess {
    wrapper: DirectChild,
    target: ProcessGroupAnchor,
    placement: TargetPlacement,
    cleanup_deadline: Instant,
    cleanup_confirmation_timeout: Duration,
    cleanup_state: CleanupState,
    cleanup_failures: CleanupFailures,
    observer: Option<ProcessObserver>,
    target_lifetime: Option<TargetLifetimeLease>,
}

impl ManagedProcess {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        child: Child,
        anchor: ProcessGroupAnchor,
        cleanup_deadline: Instant,
        cleanup_confirmation_timeout: Duration,
        cleanup_failures: CleanupFailures,
        observer: Option<ProcessObserver>,
        reaper: NoSignalReaper,
        target_lifetime: TargetLifetimeLease,
    ) -> Self {
        Self {
            wrapper: DirectChild::new(child, reaper),
            target: anchor,
            placement: TargetPlacement::UnpublishedInWrapperGroup,
            cleanup_deadline,
            cleanup_confirmation_timeout,
            cleanup_state: CleanupState::Armed,
            cleanup_failures,
            observer,
            target_lifetime: Some(target_lifetime),
        }
    }

    pub(crate) fn id(&self) -> u32 {
        self.wrapper.id()
    }

    pub(crate) fn target_group_id(&self) -> u32 {
        self.target.id()
    }

    pub(crate) fn wrapper_exit_observed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        self.wrapper.exit_observed()
    }

    pub(crate) fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.wrapper.try_wait()
    }

    pub(crate) fn wait_until(&mut self, deadline: Instant) -> std::io::Result<Option<ExitStatus>> {
        self.wrapper.wait_until(deadline)
    }

    pub(crate) fn disarm(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.target.is_owned() || !self.wrapper.is_reaped() {
            return Err(
                "cannot disarm measured process before both direct children are reaped".into(),
            );
        }
        self.cleanup_state = CleanupState::Complete;
        Ok(())
    }
}
