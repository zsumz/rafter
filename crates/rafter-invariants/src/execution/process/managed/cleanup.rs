//! Absolute-deadline emergency cleanup and no-signal quarantine transitions.

use std::time::Instant;

use super::super::{ProcessSignal, PROCESS_POLL_INTERVAL};
use super::{CleanupState, ManagedProcess, TargetPlacement};

#[cfg(test)]
thread_local! {
    static FORCE_NEXT_CLEANUP_TARGET_ALIVE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

impl ManagedProcess {
    pub(crate) fn cleanup_until(
        &mut self,
        cleanup_start: Instant,
        deadline: Instant,
    ) -> Result<(), String> {
        match self.cleanup_state {
            CleanupState::Complete => return Ok(()),
            CleanupState::Failed => {
                return Err("subprocess cleanup already failed and will not be retried".to_owned())
            }
            CleanupState::Cleaning => return Err("subprocess cleanup was re-entered".to_owned()),
            CleanupState::Armed => self.cleanup_state = CleanupState::Cleaning,
        }
        if cleanup_start > deadline {
            self.cleanup_state = CleanupState::Failed;
            return Err("subprocess cleanup boundary exceeds its deadline".to_owned());
        }
        let confirmation_deadline = Instant::now()
            .checked_add(self.cleanup_confirmation_timeout)
            .unwrap_or(deadline)
            .min(deadline);
        let result = self.cleanup_owned_processes(confirmation_deadline);
        self.cleanup_state = if result.is_ok() {
            CleanupState::Complete
        } else {
            CleanupState::Failed
        };
        result
    }

    fn cleanup_owned_processes(&mut self, deadline: Instant) -> Result<(), String> {
        let mut errors = Vec::new();
        #[cfg(test)]
        let force_target_alive = FORCE_NEXT_CLEANUP_TARGET_ALIVE.with(|force| force.replace(false));
        #[cfg(not(test))]
        let force_target_alive = false;

        if Instant::now() < deadline {
            self.signal_emergency_groups(&mut errors);
            self.await_emergency_cleanup(deadline, force_target_alive, &mut errors);
        } else {
            errors
                .push("subprocess cleanup deadline expired before emergency signaling".to_owned());
        }
        if self.target.is_owned() || self.wrapper.is_owned() {
            self.record_expired_ownership(&mut errors);
            self.quarantine_owned_children(&mut errors);
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    fn signal_emergency_groups(&mut self, errors: &mut Vec<String>) {
        match self.placement {
            TargetPlacement::UnpublishedInWrapperGroup
            | TargetPlacement::PublishedInWrapperGroup { .. } => {
                if let Err(error) = self.wrapper.signal_group(ProcessSignal::Kill) {
                    errors.push(error.to_string());
                }
            }
            TargetPlacement::JoiningAnchorGroup { .. } => {
                if let Err(error) = self.signal_target_group(ProcessSignal::Kill) {
                    errors.push(error.to_string());
                }
                if let Err(error) = self.wrapper.signal_group(ProcessSignal::Kill) {
                    errors.push(error.to_string());
                }
            }
            TargetPlacement::InAnchorGroup { .. } => {
                if let Err(error) = self.signal_target_group(ProcessSignal::Kill) {
                    errors.push(error.to_string());
                }
            }
            TargetPlacement::Finished => {}
        }
    }

    fn await_emergency_cleanup(
        &mut self,
        deadline: Instant,
        force_target_alive: bool,
        errors: &mut Vec<String>,
    ) {
        let mut observation_failed = false;
        let mut quiescence = None;
        loop {
            if self.target.is_owned()
                && matches!(
                    self.placement,
                    TargetPlacement::UnpublishedInWrapperGroup
                        | TargetPlacement::PublishedInWrapperGroup { .. }
                )
                && self.wrapper.is_reaped()
            {
                if let Err(error) = self.release_unjoined_anchor(deadline) {
                    errors.push(error.to_string());
                }
            } else if self.target.is_owned()
                && matches!(
                    self.placement,
                    TargetPlacement::JoiningAnchorGroup { .. }
                        | TargetPlacement::InAnchorGroup { .. }
                )
            {
                if !force_target_alive && !observation_failed && quiescence.is_none() {
                    match self.observe_target_members(deadline, deadline) {
                        Ok(observation) => quiescence = observation.into_quiescence(),
                        Err(error) => {
                            errors.push(error.to_string());
                            observation_failed = true;
                        }
                    }
                }
                let anchor_exited = self.target.exit_observed().unwrap_or(false);
                if anchor_exited && self.target.signal_was_sent(ProcessSignal::Kill) {
                    if let Some(proof) = quiescence {
                        if let Err(error) = self.reap_target_anchor_after_kill(proof, deadline) {
                            errors.push(error.to_string());
                        }
                    } else if observation_failed {
                        // Preserve the unreaped identity for no-signal quarantine below.
                    }
                }
            }
            if self.wrapper.is_owned() {
                if let Err(error) = self.wrapper.try_wait() {
                    errors.push(format!(
                        "reap resource wrapper after cleanup signal: {error}"
                    ));
                }
            }
            if !self.target.is_owned() && !self.wrapper.is_owned() {
                return;
            }
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            std::thread::sleep(PROCESS_POLL_INTERVAL.min(deadline.duration_since(now)));
        }
    }

    fn record_expired_ownership(&self, errors: &mut Vec<String>) {
        if self.wrapper.is_owned() {
            errors.push("resource wrapper remained unreaped after emergency cleanup".to_owned());
        }
        if self.target.is_owned() {
            errors.push(format!(
                "process group {} remained owned after emergency cleanup",
                self.target.id()
            ));
        }
    }

    fn quarantine_owned_children(&mut self, errors: &mut Vec<String>) {
        if self.target.is_owned() {
            match self.target_lifetime.take() {
                Some(lifetime) => match self.target.quarantine_until_target_exit(lifetime) {
                    Ok(true) => errors.push(format!(
                        "process-group anchor {} and target lifetime transferred to no-signal reaper",
                        self.target.id()
                    )),
                    Ok(false) => {}
                    Err((lifetime, error)) => {
                        self.target_lifetime = Some(lifetime);
                        errors.push(error.to_string());
                    }
                },
                None => errors.push(
                    "owned process-group anchor lost its target lifetime lease before quarantine"
                        .to_owned(),
                ),
            }
        }
        match self.wrapper.quarantine("resource wrapper") {
            Ok(true) => errors.push(format!(
                "resource wrapper {} transferred to no-signal reaper",
                self.wrapper.id()
            )),
            Ok(false) => {}
            Err(error) => errors.push(error.to_string()),
        }
    }
}

#[cfg(test)]
pub(crate) fn force_next_cleanup_target_alive() {
    FORCE_NEXT_CLEANUP_TARGET_ALIVE.with(|force| force.set(true));
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        if self.cleanup_state == CleanupState::Armed {
            if let Err(error) = self.cleanup_until(Instant::now(), self.cleanup_deadline) {
                eprintln!("rafter-invariants: fallback subprocess cleanup failed: {error}");
                self.cleanup_failures.record(error);
            }
        }
        if self.target.is_owned() || self.wrapper.is_owned() {
            let mut failures = Vec::new();
            self.quarantine_owned_children(&mut failures);
            if !failures.is_empty() {
                self.cleanup_failures.record(failures.join("; "));
            }
        }
    }
}
