//! Collision-safe process telemetry paths and bounded resource observation.

use std::{
    error::Error,
    path::PathBuf,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::fd::{AsFd, OwnedFd};

#[cfg(test)]
use std::path::Path;

use super::{
    internal_command::bounded_internal_output_with_runtime, NoSignalReaper, ProcessAnchorState,
    ProcessGroupObservation, RuntimeExecutable, TargetMemberState,
};

#[cfg(test)]
thread_local! {
    static NEXT_OBSERVATION_DELAY: std::cell::Cell<Option<Duration>> = const {
        std::cell::Cell::new(None)
    };
    static OMIT_NEXT_ANCHOR_ROW: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static OMIT_TARGET_ROWS: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

pub(super) const PS_TELEMETRY_TIMEOUT: Duration = Duration::from_secs(2);

/// Marks a failure of the observer *command* rather than of what it observed;
/// written here and matched by the execution-window retry.
pub(super) const OBSERVER_COMMAND_FAILURE: &str = "run process-group observer";

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
#[cfg(all(test, target_os = "linux"))]
const PS_PATH: &str = "/usr/bin/ps";
#[cfg(all(test, target_os = "macos"))]
const PS_PATH: &str = "/bin/ps";
#[cfg(all(test, not(any(target_os = "linux", target_os = "macos"))))]
const PS_PATH: &str = "/usr/bin/ps";

#[cfg(test)]
pub(crate) fn process_observer_path() -> &'static Path {
    Path::new(PS_PATH)
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
    NEXT_OBSERVATION_DELAY.with(|delay| {
        if let Some(delay) = delay.take() {
            let remaining = observation_deadline.saturating_duration_since(Instant::now());
            std::thread::sleep(delay.min(remaining));
        }
    });
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
    let output = match bounded_internal_output_with_runtime(
        observer.runtime(),
        &["-e", "-o", "pid=,pgid=,rss=,stat="],
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
        Err(error) => return Err(format!("{OBSERVER_COMMAND_FAILURE}: {error}").into()),
    };
    if !output.status.success() {
        return Err(format!(
            "sample process-group RSS with ps exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let source = String::from_utf8_lossy(&output.stdout);
    #[cfg(test)]
    let mut source = source.into_owned();
    #[cfg(test)]
    if OMIT_NEXT_ANCHOR_ROW.with(|omit| omit.replace(false)) {
        let anchor = anchor
            .ok_or("anchor-row omission requires an expected anchor")?
            .to_string();
        source = source
            .lines()
            .filter(|line| line.split_whitespace().next() != Some(anchor.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
    }
    #[cfg(test)]
    if OMIT_TARGET_ROWS.with(std::cell::Cell::get) {
        source = source
            .lines()
            .filter(|line| {
                let mut fields = line.split_whitespace();
                let process_id = fields.next().and_then(|field| field.parse::<u32>().ok());
                let group_id = fields.next().and_then(|field| field.parse::<u32>().ok());
                group_id != Some(process_group) || process_id.is_none() || process_id == anchor
            })
            .collect::<Vec<_>>()
            .join("\n");
    }
    parse_process_group_observation(&source, process_group, anchor).map(GroupObservation::Observed)
}

#[cfg(test)]
pub(crate) fn delay_next_process_group_observation(delay: Duration) {
    NEXT_OBSERVATION_DELAY.with(|next| next.set(Some(delay)));
}

#[cfg(test)]
pub(crate) fn omit_anchor_from_next_process_group_observation() {
    OMIT_NEXT_ANCHOR_ROW.with(|omit| omit.set(true));
}

/// Scopes a target-row omission to the run that armed it.
#[cfg(test)]
pub(crate) struct TargetRowOmission;

#[cfg(test)]
impl Drop for TargetRowOmission {
    fn drop(&mut self) {
        OMIT_TARGET_ROWS.with(|omit| omit.set(false));
    }
}

/// Omit live target-member rows from every process-group observation until the
/// returned guard is dropped.
///
/// A one-shot omission lands on whichever observation happens to run first. On
/// a loaded machine that is an observation taken before the resource wrapper
/// has exited, and such an observation cannot reach the harness error the
/// omission exists to expose — the omission is spent and the run then succeeds.
/// Staying armed lets the omission land on the first observation that is able
/// to decide, whenever that turns out to be.
#[cfg(test)]
#[must_use]
pub(crate) fn omit_target_rows_from_process_group_observations() -> TargetRowOmission {
    OMIT_TARGET_ROWS.with(|omit| omit.set(true));
    TargetRowOmission
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
