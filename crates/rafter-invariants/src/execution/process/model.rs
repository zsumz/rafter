//! Domain-neutral process requests, observations, completions, and policies.

use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::Path,
    process::ExitStatus,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::fd::BorrowedFd;

#[derive(Clone, Copy)]
pub(crate) struct RuntimeExecutable<'a> {
    pub(crate) path: &'a Path,
    #[cfg(unix)]
    pub(crate) descriptor: BorrowedFd<'a>,
}

#[derive(Clone, Copy)]
pub(crate) struct ProcessRuntime<'a> {
    pub(crate) perl: RuntimeExecutable<'a>,
    pub(crate) time: RuntimeExecutable<'a>,
    pub(crate) observer: RuntimeExecutable<'a>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProcessDeadlines {
    pub(crate) target_timeout: Duration,
    pub(crate) execution_window: Instant,
    pub(crate) cleanup_start: Instant,
    pub(crate) finalization_start: Instant,
    pub(crate) lifecycle: Instant,
}

impl ProcessDeadlines {
    pub(crate) fn new(
        target_timeout: Duration,
        execution_window: Instant,
        cleanup_start: Instant,
        finalization_start: Instant,
        lifecycle: Instant,
    ) -> Result<Self, &'static str> {
        if execution_window > cleanup_start {
            return Err("process execution deadline exceeds its cleanup boundary");
        }
        if cleanup_start > finalization_start {
            return Err("process cleanup boundary exceeds its finalization boundary");
        }
        if finalization_start > lifecycle {
            return Err("process finalization boundary exceeds its lifecycle deadline");
        }
        Ok(Self {
            target_timeout,
            execution_window,
            cleanup_start,
            finalization_start,
            lifecycle,
        })
    }
}

#[cfg(test)]
pub(super) const DEFAULT_KILL_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(100);

const MAX_PROCESS_STDOUT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PROCESS_STDERR_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PROCESS_TELEMETRY_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct TerminationPolicy {
    pub(crate) grace: Duration,
    pub(crate) publication_timeout: Duration,
    pub(crate) kill_confirmation_timeout: Duration,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FinalizationPolicy {
    pub(super) timeout: Duration,
    pub(super) stdout_max_bytes: u64,
    pub(super) stderr_max_bytes: u64,
    pub(super) telemetry_max_bytes: u64,
}

impl FinalizationPolicy {
    pub(crate) const fn bounded(timeout: Duration) -> Self {
        Self {
            timeout,
            stdout_max_bytes: MAX_PROCESS_STDOUT_BYTES,
            stderr_max_bytes: MAX_PROCESS_STDERR_BYTES,
            telemetry_max_bytes: MAX_PROCESS_TELEMETRY_BYTES,
        }
    }

    #[cfg(test)]
    pub(super) const fn with_output_limits(
        mut self,
        stdout_max_bytes: u64,
        stderr_max_bytes: u64,
    ) -> Self {
        self.stdout_max_bytes = stdout_max_bytes;
        self.stderr_max_bytes = stderr_max_bytes;
        self
    }
}

pub(crate) struct ProcessRequest<'a> {
    pub(crate) program: &'a str,
    pub(crate) executable_path: &'a Path,
    pub(crate) arguments: &'a [OsString],
    pub(crate) environment: &'a BTreeMap<String, String>,
    pub(crate) deadlines: ProcessDeadlines,
    pub(crate) termination: TerminationPolicy,
    pub(crate) finalization: FinalizationPolicy,
    pub(crate) runtime: ProcessRuntime<'a>,
    #[cfg(unix)]
    pub(crate) executable_descriptor: BorrowedFd<'a>,
    #[cfg(unix)]
    pub(crate) target_descriptor: BorrowedFd<'a>,
    #[cfg(unix)]
    pub(crate) working_directory_descriptor: BorrowedFd<'a>,
    #[cfg(unix)]
    pub(crate) inherited_descriptors: &'a [BorrowedFd<'a>],
}

#[derive(Debug)]
pub(crate) struct ProcessOutput {
    pub(crate) status: ExitStatus,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
    pub(crate) duration: Duration,
    pub(crate) peak_rss_kib: u64,
    pub(crate) timed_out: bool,
    pub(crate) termination: ProcessTermination,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProcessTermination {
    pub(crate) process_group: bool,
    pub(crate) term_signal_sent: bool,
    pub(crate) grace: Duration,
    pub(crate) kill_signal_sent: bool,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessGroupState {
    Alive,
    Absent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TargetMemberState {
    Live,
    Quiescent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProcessGroupObservation {
    pub(crate) target_members: TargetMemberState,
    pub(crate) rss_kib: u64,
    pub(crate) anchor: Option<ProcessAnchorState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessAnchorState {
    Alive,
    Zombie,
    Missing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SignalDelivery {
    Sent,
    AlreadySent,
    GroupAbsent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProcessSignal {
    Term,
    Kill,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProcessCompletion {
    pub(crate) status: ExitStatus,
    pub(crate) timed_out: bool,
    pub(crate) term_signal_sent: bool,
    pub(crate) kill_signal_sent: bool,
}

pub(crate) fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
