//! Detector-replay process capability and retained observation vocabulary.

use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
    path::{Path, PathBuf},
    process::ExitStatus,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::fd::BorrowedFd;

use crate::execution::process::{base_environment, run_bounded, BoundCommand};

pub(super) struct ReplayCommand {
    command: BoundCommand,
}

impl ReplayCommand {
    pub(super) fn bind(
        program: &str,
        arguments: &[OsString],
        environment: &BTreeMap<String, String>,
        current_dir: &Path,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            command: BoundCommand::bind(program, arguments, environment, current_dir)?,
        })
    }

    pub(super) fn program_sha256(&self) -> String {
        self.command.target_identity().sha256
    }
}

pub(super) struct ReplayProcessOutput {
    pub(super) status: ExitStatus,
    pub(super) stdout: Vec<u8>,
    pub(super) stderr: Vec<u8>,
    pub(super) duration: Duration,
    pub(super) peak_rss_kib: u64,
    pub(super) timed_out: bool,
    pub(super) termination: ReplayTermination,
}

#[derive(Clone, Copy)]
pub(super) struct ReplayProcessBudget {
    target_timeout: Duration,
    lifecycle_deadline: Instant,
}

impl ReplayProcessBudget {
    pub(super) const fn new(target_timeout: Duration, lifecycle_deadline: Instant) -> Self {
        Self {
            target_timeout,
            lifecycle_deadline,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct RetainedProcessDiagnostics {
    pub(super) stdout: PathBuf,
    pub(super) stderr: PathBuf,
    pub(super) telemetry: Option<PathBuf>,
}

pub(super) struct ReplayTermination {
    pub(super) process_group: bool,
    pub(super) term_signal_sent: bool,
    pub(super) grace: Duration,
    pub(super) kill_signal_sent: bool,
}

pub(super) fn environment() -> BTreeMap<String, String> {
    base_environment()
}

pub(super) fn retained_diagnostics(
    error: &(dyn Error + 'static),
) -> Option<RetainedProcessDiagnostics> {
    crate::execution::process::retained_diagnostics(error).map(|diagnostics| {
        RetainedProcessDiagnostics {
            stdout: diagnostics.stdout,
            stderr: diagnostics.stderr,
            telemetry: diagnostics.telemetry,
        }
    })
}

#[cfg(unix)]
pub(super) fn run(
    command: &ReplayCommand,
    environment: &BTreeMap<String, String>,
    budget: ReplayProcessBudget,
    inherited_descriptors: &[BorrowedFd<'_>],
) -> Result<ReplayProcessOutput, Box<dyn Error>> {
    let output = run_bounded(
        &command.command,
        environment,
        budget.target_timeout,
        budget.lifecycle_deadline,
        inherited_descriptors,
    )?;
    Ok(ReplayProcessOutput {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
        duration: output.duration,
        peak_rss_kib: output.peak_rss_kib,
        timed_out: output.timed_out,
        termination: ReplayTermination {
            process_group: output.termination.process_group,
            term_signal_sent: output.termination.term_signal_sent,
            grace: output.termination.grace,
            kill_signal_sent: output.termination.kill_signal_sent,
        },
    })
}
