//! Collision-safe process telemetry paths and bounded resource observation.

#[cfg(test)]
mod test_support;

use std::{
    error::Error,
    fmt,
    path::PathBuf,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::fd::{AsFd, OwnedFd};

use super::{
    internal_command::bounded_internal_output_with_runtime, NoSignalReaper, ProcessAnchorState,
    ProcessGroupObservation, RuntimeExecutable, TargetMemberState,
};

#[cfg(test)]
pub(crate) use test_support::{
    delay_next_process_group_observation, fail_next_process_group_observation_command,
    omit_anchor_from_next_process_group_observation,
    omit_target_rows_from_process_group_observations, process_observer_path,
};
#[cfg(test)]
use test_support::{
    delay_observation_if_requested, injected_observer_arguments, omit_rows_if_requested,
};

pub(super) const PS_TELEMETRY_TIMEOUT: Duration = Duration::from_secs(2);

const OBSERVER_ARGUMENTS: &[&str] = &["-e", "-o", "pid=,pgid=,rss=,stat="];

/// A failure of the observer *command* rather than of what it observed.
///
/// Eligibility for the execution-window retry is exactly this distinction, so
/// it is carried by the type instead of by a message prefix. A prefix is a
/// promise each construction site has to remember to keep, and one of the two
/// sites here did not: a `ps` that hung was retried while a `ps` that failed
/// outright -- a fork that lost to memory pressure, a transient `EAGAIN`, the
/// very things a starved runner does instead of hanging -- was fatal. Naming
/// the type is a promise the compiler holds the site to.
#[derive(Debug)]
pub(super) struct ObserverCommandFailure {
    detail: String,
}

impl ObserverCommandFailure {
    fn boxed(detail: impl fmt::Display) -> Box<dyn Error> {
        Box::new(Self {
            detail: detail.to_string(),
        })
    }
}

impl fmt::Display for ObserverCommandFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "run process-group observer: {}", self.detail)
    }
}

impl Error for ObserverCommandFailure {}

#[derive(Debug)]
pub(crate) struct ProcessObserver {
    path: PathBuf,
    #[cfg(unix)]
    descriptor: OwnedFd,
    reaper: NoSignalReaper,
}

impl ProcessObserver {
    pub(crate) fn capture(
        runtime: RuntimeExecutable<'_>,
        reaper: NoSignalReaper,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            path: runtime.path.to_owned(),
            #[cfg(unix)]
            descriptor: runtime.descriptor.try_clone_to_owned()?,
            reaper,
        })
    }

    pub(crate) fn runtime(&self) -> RuntimeExecutable<'_> {
        RuntimeExecutable {
            path: &self.path,
            #[cfg(unix)]
            descriptor: self.descriptor.as_fd(),
        }
    }

    pub(crate) fn reaper(&self) -> NoSignalReaper {
        self.reaper.clone()
    }
}

pub(crate) fn parse_peak_rss(stderr: &[u8]) -> Option<u64> {
    let stderr = String::from_utf8_lossy(stderr);
    if cfg!(target_os = "macos") {
        stderr.lines().find_map(|line| {
            line.trim()
                .strip_suffix("  maximum resident set size")
                .and_then(|bytes| bytes.trim().parse::<u64>().ok())
                .map(|bytes| bytes.div_ceil(1024))
        })
    } else {
        stderr.lines().find_map(|line| {
            line.trim()
                .strip_prefix("Maximum resident set size (kbytes):")
                .and_then(|kib| kib.trim().parse::<u64>().ok())
        })
    }
}

/// One completed observation, or the window that would have contained it
/// closing before it could start.
///
/// `WindowClosed` is a lifecycle fact, not an evidence failure: nothing was
/// observed because there was no time left in which to observe. An observation
/// that does start and then fails or times out stays an error, exactly as
/// before.
#[derive(Debug)]
pub(crate) enum GroupObservation {
    Observed(ProcessGroupObservation),
    WindowClosed,
}

pub(crate) fn process_group_observation(
    process_group: u32,
    anchor: Option<u32>,
    observer: &ProcessObserver,
    observation_deadline: Instant,
    lifecycle_deadline: Instant,
) -> Result<GroupObservation, Box<dyn Error>> {
    #[cfg(test)]
    delay_observation_if_requested(observation_deadline);
    // A window already over before the observer was even entered means
    // something consumed it inside this call -- a stalled observer, not a
    // window that ran out underneath a running one. That stays fail-closed.
    if Instant::now() >= observation_deadline {
        return Err("process-group observer exhausted its absolute deadline".into());
    }
    // `ps` is given PS_TELEMETRY_TIMEOUT unless the observation window is
    // shorter than that, in which case the window truncates it. Remembering
    // which of the two bounded this run is what separates "the observer is
    // broken" from "the window ended underneath it" below.
    let full_budget = Instant::now()
        .checked_add(PS_TELEMETRY_TIMEOUT)
        .ok_or("process observation deadline overflow")?;
    let truncated_by_window = observation_deadline < full_budget;
    let phase_deadline = full_budget.min(observation_deadline);
    let arguments = OBSERVER_ARGUMENTS;
    #[cfg(test)]
    let arguments = injected_observer_arguments().unwrap_or(arguments);
    let output = match bounded_internal_output_with_runtime(
        observer.runtime(),
        arguments,
        phase_deadline,
        lifecycle_deadline,
        observer.reaper(),
    ) {
        Ok(output) => output,
        // The window closed while a truncated observation was still running.
        // That is the window ending, not evidence failing: the caller's own
        // deadline check would have said the same thing microseconds later, and
        // every caller already has a path for it. Weekly run 31585942873 lost
        // an 84-minute simulator run here -- `ps` was handed 711 ms of a window
        // that was about to close on a loaded runner, and its timeout was
        // reported as a harness failure 711 ms before the same outcome would
        // have been reached legitimately.
        //
        // Deliberately narrow: an observation that received its full
        // PS_TELEMETRY_TIMEOUT and then failed or timed out is as fatal as it
        // has always been, because the window did not bound it.
        Err(_) if truncated_by_window && Instant::now() >= observation_deadline => {
            return Ok(GroupObservation::WindowClosed);
        }
        Err(error) => return Err(ObserverCommandFailure::boxed(error)),
    };
    // A `ps` that ran and reported its own failure is the observer command
    // failing just as much as one that never answered, and a host too starved
    // to fork is likelier to produce this one than the hang.
    if !output.status.success() {
        return Err(ObserverCommandFailure::boxed(format!(
            "sample process-group RSS with ps exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let source = String::from_utf8_lossy(&output.stdout);
    #[cfg(test)]
    let source = omit_rows_if_requested(source.into_owned(), process_group, anchor)?;
    parse_process_group_observation(&source, process_group, anchor).map(GroupObservation::Observed)
}

pub(crate) fn parse_process_group_observation(
    source: &str,
    process_group: u32,
    anchor: Option<u32>,
) -> Result<ProcessGroupObservation, Box<dyn Error>> {
    let mut observation = ProcessGroupObservation {
        target_members: TargetMemberState::Quiescent,
        rss_kib: 0,
        anchor: anchor.map(|_| ProcessAnchorState::Missing),
    };
    for line in source.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let process_id = fields
            .next()
            .ok_or("ps RSS row omitted process ID")?
            .parse::<u32>()?;
        let process_group_id = fields
            .next()
            .ok_or("ps RSS row omitted process-group ID")?
            .parse::<u32>()?;
        let rss = fields
            .next()
            .ok_or("ps RSS row omitted resident-set size")?
            .parse::<u64>()?;
        let state = fields.next().ok_or("ps RSS row omitted process state")?;
        if fields.next().is_some() {
            return Err("ps RSS row contained unexpected fields".into());
        }
        if process_group_id != process_group {
            continue;
        }
        if anchor == Some(process_id) {
            observation.anchor = Some(if state.starts_with('Z') {
                ProcessAnchorState::Zombie
            } else {
                ProcessAnchorState::Alive
            });
            continue;
        }
        // Zombies cannot execute, fork, hold descriptors, or survive a signal.
        if !state.starts_with('Z') {
            observation.target_members = TargetMemberState::Live;
            observation.rss_kib = observation
                .rss_kib
                .checked_add(rss)
                .ok_or("process-group RSS sum overflowed u64")?;
        }
    }
    Ok(observation)
}
