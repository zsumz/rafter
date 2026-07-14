use super::{
    confirm_process_group_absent, fmt, signal_process_group, Child, Duration, Error, ExitStatus,
    MutexGuard, PathBuf, FALLBACK_CLEANUP_FAILURES,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProcessGroupState {
    Alive,
    Absent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProcessGroupObservation {
    pub(super) state: ProcessGroupState,
    pub(super) rss_kib: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SignalDelivery {
    Sent,
    GroupAbsent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProcessSignal {
    Term,
    Kill,
}

#[derive(Debug)]
pub(super) struct ProcessCleanupError {
    pub(super) detail: String,
    pub(super) stdout_path: PathBuf,
    pub(super) stderr_path: PathBuf,
    pub(super) telemetry_path: Option<PathBuf>,
}

impl fmt::Display for ProcessCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; retained subprocess stdout at {} and stderr at {}",
            self.detail,
            self.stdout_path.display(),
            self.stderr_path.display()
        )?;
        if let Some(path) = &self.telemetry_path {
            write!(formatter, " and resource telemetry at {}", path.display())?;
        }
        Ok(())
    }
}

impl Error for ProcessCleanupError {}

#[derive(Debug)]
pub(super) struct TimeoutTermination {
    pub(super) status: ExitStatus,
    pub(super) timed_out: bool,
    pub(super) term_signal_sent: bool,
    pub(super) kill_signal_sent: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CollectedProcessStatus {
    pub(super) status: ExitStatus,
    pub(super) timed_out: bool,
    pub(super) term_signal_sent: bool,
    pub(super) kill_signal_sent: bool,
}

#[derive(Debug)]
pub(super) struct ManagedProcess {
    child: Child,
    pub(super) target_group: Option<u32>,
    wrapper_status: Option<ExitStatus>,
    kill_confirmation_timeout: Duration,
    armed: bool,
}

impl ManagedProcess {
    pub(super) fn new(child: Child, kill_confirmation_timeout: Duration) -> Self {
        Self {
            child,
            target_group: None,
            wrapper_status: None,
            kill_confirmation_timeout,
            armed: true,
        }
    }

    pub(super) fn id(&self) -> u32 {
        self.child.id()
    }

    pub(super) fn set_target_group(&mut self, process_group: u32) {
        self.target_group = Some(process_group);
    }

    pub(super) fn mark_target_absent(&mut self) {
        self.target_group = None;
    }

    pub(super) fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        if self.wrapper_status.is_none() {
            self.wrapper_status = self.child.try_wait()?;
        }
        Ok(self.wrapper_status)
    }

    pub(super) fn wait(&mut self) -> std::io::Result<ExitStatus> {
        if let Some(status) = self.wrapper_status {
            return Ok(status);
        }
        let status = self.child.wait()?;
        self.wrapper_status = Some(status);
        Ok(status)
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }

    pub(super) fn cleanup(&mut self) -> Result<(), String> {
        if !self.armed {
            return Ok(());
        }
        let mut errors = Vec::new();
        if let Some(process_group) = self.target_group {
            if let Err(error) = signal_process_group(process_group, ProcessSignal::Kill) {
                errors.push(error.to_string());
            }
        }
        if self.wrapper_status.is_none() {
            match self.child.try_wait() {
                Ok(Some(status)) => self.wrapper_status = Some(status),
                Ok(None) => {}
                Err(error) => errors.push(format!("probe resource wrapper: {error}")),
            }
        }
        if self.wrapper_status.is_none() {
            if let Err(error) = signal_process_group(self.child.id(), ProcessSignal::Kill) {
                errors.push(error.to_string());
            }
            match self.child.wait() {
                Ok(status) => self.wrapper_status = Some(status),
                Err(error) => errors.push(format!("reap resource wrapper: {error}")),
            }
        }
        if let Some(process_group) = self.target_group {
            if let Err(error) =
                confirm_process_group_absent(process_group, self.kill_confirmation_timeout)
            {
                errors.push(error.to_string());
            } else {
                self.target_group = None;
            }
        }
        if errors.is_empty() {
            self.disarm();
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            record_fallback_cleanup_failure(error);
        }
    }
}

fn record_fallback_cleanup_failure(error: String) {
    eprintln!("rafter-invariants: fallback subprocess cleanup failed: {error}");
    fallback_cleanup_failures().push(error);
}

pub(super) fn take_fallback_cleanup_failures() -> Vec<String> {
    let mut failures = std::mem::take(&mut *fallback_cleanup_failures());
    failures.sort();
    failures
}

fn fallback_cleanup_failures() -> MutexGuard<'static, Vec<String>> {
    FALLBACK_CLEANUP_FAILURES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
