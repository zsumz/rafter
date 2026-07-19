//! Target process-group publication and ownership transfer.

use std::{
    error::Error,
    io::Write,
    os::unix::net::UnixStream,
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::execution::filesystem::OperationDeadline;

use rustix::process::{getpgid, Pid};

use super::{diagnostics::retained_error, duration_ms, ManagedProcess, ProcessArtifacts};

const PROCESS_GROUP_RECEIPT_MAX_BYTES: u64 = 64;

#[cfg(test)]
thread_local! {
    static NEXT_RECEIPT_DELAY: std::cell::Cell<Option<Duration>> = const {
        std::cell::Cell::new(None)
    };
    static LAST_DELAYED_PROCESS_GROUP: std::cell::Cell<Option<u32>> = const {
        std::cell::Cell::new(None)
    };
    static NEXT_TARGET_RELEASE_DELAY: std::cell::Cell<Option<Duration>> = const {
        std::cell::Cell::new(None)
    };
    static LAST_UNRELEASED_PROCESS_GROUP: std::cell::Cell<Option<u32>> = const {
        std::cell::Cell::new(None)
    };
}

#[cfg(test)]
pub(crate) fn delay_next_process_group_receipt(delay: Duration) {
    NEXT_RECEIPT_DELAY.with(|next| next.set(Some(delay)));
    LAST_DELAYED_PROCESS_GROUP.with(|last| last.set(None));
}

#[cfg(test)]
pub(crate) fn take_last_delayed_process_group() -> Option<u32> {
    LAST_DELAYED_PROCESS_GROUP.with(std::cell::Cell::take)
}

#[cfg(test)]
pub(crate) fn delay_next_target_release(delay: Duration) {
    NEXT_TARGET_RELEASE_DELAY.with(|next| next.set(Some(delay)));
    LAST_UNRELEASED_PROCESS_GROUP.with(|last| last.set(None));
}

#[cfg(test)]
pub(crate) fn take_last_unreleased_process_group() -> Option<u32> {
    LAST_UNRELEASED_PROCESS_GROUP.with(std::cell::Cell::take)
}

struct ProcessDiagnosticPaths {
    stdout: PathBuf,
    stderr: PathBuf,
    resource: PathBuf,
}

impl ProcessDiagnosticPaths {
    fn retained(&self, detail: impl std::fmt::Display) -> Box<dyn Error> {
        retained_error(detail, &self.stdout, &self.stderr, Some(&self.resource))
    }
}

struct TargetGroupPublication<'a> {
    process: &'a mut ManagedProcess,
    acknowledgement: &'a mut UnixStream,
    process_group: Option<u32>,
    acknowledgement_sent: bool,
    deadline: Instant,
    paths: ProcessDiagnosticPaths,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TargetGroupFrame {
    Pending,
    Planned(u32),
    Ready(u32),
}

pub(crate) fn parse_target_group_frame(source: &str) -> Result<TargetGroupFrame, &'static str> {
    let Some((published, remainder)) = source.split_once('\n') else {
        return Ok(TargetGroupFrame::Pending);
    };
    let published = published
        .parse::<u32>()
        .map_err(|_| "target launcher published a malformed process group")?;
    if published == 0 {
        return Err("target launcher published an invalid process group");
    }
    match remainder {
        "" => Ok(TargetGroupFrame::Planned(published)),
        "ready\n" => Ok(TargetGroupFrame::Ready(published)),
        partial if "ready\n".starts_with(partial) => Ok(TargetGroupFrame::Planned(published)),
        _ => Err("target launcher published malformed process-group readiness"),
    }
}

pub(crate) fn validate_target_group_candidate_with(
    published: u32,
    wrapper_group: u32,
    process_group_of: impl FnOnce(u32) -> Result<u32, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    if published == wrapper_group {
        return Err("target launcher published the wrapper process group".into());
    }
    let observed_group = process_group_of(published)?;
    if observed_group != wrapper_group {
        return Err(format!(
            "target launcher process {published} belongs to process group {observed_group}, expected wrapper group {wrapper_group}"
        )
        .into());
    }
    Ok(())
}

pub(crate) fn validate_ready_target_group_with(
    published: u32,
    expected_group: u32,
    process_group_of: impl FnOnce(u32) -> Result<u32, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let observed_group = process_group_of(published)?;
    if observed_group != expected_group {
        return Err(format!(
            "ready target process {published} belongs to process group {observed_group}, expected anchored group {expected_group}"
        )
        .into());
    }
    Ok(())
}

fn process_group_of(pid: u32) -> Result<u32, Box<dyn Error>> {
    let raw = i32::try_from(pid).map_err(|_| format!("process ID exceeds i32: {pid}"))?;
    let pid = Pid::from_raw(raw).ok_or_else(|| format!("process ID must be positive: {pid}"))?;
    let process_group =
        getpgid(Some(pid)).map_err(|error| format!("read process group for {pid}: {error}"))?;
    Ok(u32::try_from(process_group.as_raw_pid())?)
}

impl TargetGroupPublication<'_> {
    fn consume(&mut self, source: &str) -> Result<Option<u32>, Box<dyn Error>> {
        let frame = parse_target_group_frame(source).map_err(|error| self.paths.retained(error))?;
        let published = match frame {
            TargetGroupFrame::Pending => return Ok(None),
            TargetGroupFrame::Planned(published) | TargetGroupFrame::Ready(published) => published,
        };
        if self.process_group.is_none() {
            validate_target_group_candidate_with(published, self.process.id(), process_group_of)
                .map_err(|error| self.paths.retained(error))?;
            self.process
                .record_published_target(published)
                .map_err(|error| self.paths.retained(error))?;
            self.process_group = Some(published);
        }
        if self.process_group != Some(published) {
            return Err(self
                .paths
                .retained("target launcher changed its published process group"));
        }
        if self.process_group.is_some() && !self.acknowledgement_sent {
            self.process
                .begin_target_group_transition(published)
                .map_err(|error| self.paths.retained(error))?;
            self.acknowledgement.write_all(b"G").map_err(|error| {
                self.paths
                    .retained(format!("acknowledge target process group: {error}"))
            })?;
            self.acknowledgement_sent = true;
        }
        match frame {
            TargetGroupFrame::Ready(published) => {
                let anchored_group = self.process.target_group_id();
                validate_ready_target_group_with(published, anchored_group, process_group_of)
                    .map_err(|error| self.paths.retained(error))?;
                #[cfg(test)]
                NEXT_TARGET_RELEASE_DELAY.with(|delay| {
                    if let Some(delay) = delay.take() {
                        std::thread::sleep(
                            delay.min(self.deadline.saturating_duration_since(Instant::now())),
                        );
                    }
                });
                if Instant::now() >= self.deadline {
                    #[cfg(test)]
                    LAST_UNRELEASED_PROCESS_GROUP.with(|last| last.set(Some(anchored_group)));
                    return Err(self
                        .paths
                        .retained("target execution release deadline expired"));
                }
                self.process.promote_target_group(anchored_group)?;
                self.acknowledgement.write_all(b"R").map_err(|error| {
                    self.paths
                        .retained(format!("release target execution: {error}"))
                })?;
                Ok(Some(anchored_group))
            }
            TargetGroupFrame::Pending | TargetGroupFrame::Planned(_) => Ok(None),
        }
    }
}

pub(crate) fn await_target_process_group(
    process: &mut ManagedProcess,
    artifacts: &ProcessArtifacts,
    target_group_ack: &mut UnixStream,
    deadline: Instant,
) -> Result<u32, Box<dyn Error>> {
    let started = Instant::now();
    #[cfg(test)]
    let receipt_eligible_at = NEXT_RECEIPT_DELAY.with(|delay| {
        delay
            .take()
            .and_then(|delay| Instant::now().checked_add(delay))
    });
    let receipt_deadline = OperationDeadline::at(deadline, "process-group receipt publication");
    let mut publication = TargetGroupPublication {
        process,
        acknowledgement: target_group_ack,
        process_group: None,
        acknowledgement_sent: false,
        deadline,
        paths: ProcessDiagnosticPaths {
            stdout: artifacts.stdout_path(),
            stderr: artifacts.stderr_path(),
            resource: artifacts.resource_path(),
        },
    };
    loop {
        let receipt_is_eligible = {
            #[cfg(test)]
            {
                receipt_eligible_at.is_none_or(|eligible| Instant::now() >= eligible)
            }
            #[cfg(not(test))]
            {
                true
            }
        };
        if receipt_is_eligible {
            if let Ok(source) =
                artifacts.read_process_group(receipt_deadline, PROCESS_GROUP_RECEIPT_MAX_BYTES)
            {
                if let Some(process_group) = publication.consume(&source)? {
                    return Ok(process_group);
                }
            }
        }
        if publication
            .process
            .wrapper_exit_observed()
            .map_err(|error| {
                publication
                    .paths
                    .retained(format!("observe resource wrapper exit: {error}"))
            })?
        {
            return Err(publication
                .paths
                .retained("resource wrapper exited before publishing the target process group"));
        }
        if Instant::now() >= deadline {
            #[cfg(test)]
            if !receipt_is_eligible {
                let process_group = artifacts
                    .read_process_group(
                        OperationDeadline::none("test process-group receipt inspection"),
                        PROCESS_GROUP_RECEIPT_MAX_BYTES,
                    )
                    .ok()
                    .and_then(|source| source.lines().next()?.parse::<u32>().ok());
                LAST_DELAYED_PROCESS_GROUP.with(|last| last.set(process_group));
            }
            return Err(publication.paths.retained(format!(
                "target launcher did not publish its process group within {} ms",
                duration_ms(started.elapsed())
            )));
        }
        let now = Instant::now();
        if now < deadline {
            std::thread::sleep(Duration::from_millis(1).min(deadline.duration_since(now)));
        }
    }
}
