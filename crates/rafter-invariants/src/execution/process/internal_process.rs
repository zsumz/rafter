//! Direct-child ownership for trusted, bounded observer commands.

use std::{
    process::{Child, ChildStderr, ChildStdout, ExitStatus},
    time::Instant,
};

use super::{
    CleanupFailures, DirectChild, NoSignalReaper, ProcessLeaseState, ProcessLifetimeLease,
    ProcessSignal, SignalDelivery, PROCESS_POLL_INTERVAL,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CleanupState {
    Armed,
    Complete,
}

#[derive(Debug)]
pub(crate) struct ManagedInternalProcess {
    child: DirectChild,
    cleanup_deadline: Instant,
    cleanup_state: CleanupState,
    cleanup_failures: CleanupFailures,
    lifetime: Option<ProcessLifetimeLease>,
}

impl ManagedInternalProcess {
    pub(crate) fn new(
        child: Child,
        cleanup_deadline: Instant,
        cleanup_failures: CleanupFailures,
        reaper: NoSignalReaper,
        lifetime: ProcessLifetimeLease,
    ) -> Self {
        Self {
            child: DirectChild::new(child, reaper),
            cleanup_deadline,
            cleanup_state: CleanupState::Armed,
            cleanup_failures,
            lifetime: Some(lifetime),
        }
    }

    pub(crate) fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.take_stdout()
    }

    pub(crate) fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.take_stderr()
    }

    pub(crate) fn exit_observed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        self.child.exit_observed()
    }

    pub(crate) fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    pub(crate) fn lifetime_state(&self) -> Result<ProcessLeaseState, Box<dyn std::error::Error>> {
        self.lifetime
            .as_ref()
            .ok_or_else(|| "internal command lifetime lease was already released".into())
            .and_then(ProcessLifetimeLease::observe)
    }

    pub(crate) fn signal_kill(&mut self) -> Result<SignalDelivery, Box<dyn std::error::Error>> {
        self.child.signal_group(ProcessSignal::Kill)
    }

    pub(crate) fn disarm(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.child.is_reaped() {
            return Err("cannot disarm internal command before its direct child is reaped".into());
        }
        if self.lifetime_state()? != ProcessLeaseState::Released {
            return Err("cannot disarm internal command while its process lineage is live".into());
        }
        self.lifetime.take();
        self.cleanup_state = CleanupState::Complete;
        Ok(())
    }

    fn cleanup(&mut self) -> Result<(), String> {
        if !self.child.is_owned() {
            return Ok(());
        }
        let mut errors = Vec::new();
        if Instant::now() < self.cleanup_deadline {
            if let Err(error) = self.child.signal_group(ProcessSignal::Kill) {
                errors.push(error.to_string());
            }
            self.reap_released_lineage(&mut errors);
        } else {
            errors.push("internal-command cleanup deadline expired before signaling".to_owned());
        }
        self.quarantine_owned_child(&mut errors);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    fn reap_released_lineage(&mut self, errors: &mut Vec<String>) {
        loop {
            let now = Instant::now();
            if now >= self.cleanup_deadline {
                errors.push(format!(
                    "internal command {} remained unreaped through its cleanup deadline",
                    self.child.id()
                ));
                return;
            }
            let exited = match self.child.exit_observed() {
                Ok(exited) => exited,
                Err(error) => {
                    errors.push(format!(
                        "observe internal command {} after SIGKILL: {error}",
                        self.child.id()
                    ));
                    return;
                }
            };
            let lineage_released = match self.lifetime_state() {
                Ok(ProcessLeaseState::Released) => true,
                Ok(ProcessLeaseState::Held) => false,
                Err(error) => {
                    errors.push(error.to_string());
                    return;
                }
            };
            if exited && lineage_released {
                if Instant::now() >= self.cleanup_deadline {
                    errors.push(format!(
                        "internal command {} exited too late to reap before its cleanup deadline",
                        self.child.id()
                    ));
                    return;
                }
                match self.child.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) => {}
                    Err(error) => {
                        errors.push(format!(
                            "reap internal command {} after SIGKILL: {error}",
                            self.child.id()
                        ));
                        return;
                    }
                }
            }
            std::thread::sleep(
                PROCESS_POLL_INTERVAL.min(self.cleanup_deadline.saturating_duration_since(now)),
            );
        }
    }

    fn quarantine_owned_child(&mut self, errors: &mut Vec<String>) {
        if !self.child.is_owned() {
            return;
        }
        let Some(lifetime) = self.lifetime.take() else {
            errors.push("internal command lost its lifetime lease before quarantine".to_owned());
            return;
        };
        match self.child.quarantine_leased(lifetime) {
            Ok(true) => errors.push(format!(
                "internal command {} and its lifetime lease transferred to no-signal reaper",
                self.child.id()
            )),
            Ok(false) => {}
            Err((lifetime, error)) => {
                self.lifetime = Some(lifetime);
                errors.push(error.to_string());
            }
        }
    }
}

impl Drop for ManagedInternalProcess {
    fn drop(&mut self) {
        if self.cleanup_state == CleanupState::Armed {
            if let Err(error) = self.cleanup() {
                eprintln!("rafter-invariants: fallback internal cleanup failed: {error}");
                self.cleanup_failures.record(error);
            }
        }
        if self.child.is_owned() {
            let mut failures = Vec::new();
            self.quarantine_owned_child(&mut failures);
            if !failures.is_empty() {
                self.cleanup_failures.record(failures.join("; "));
            }
        }
    }
}
