//! Deterministic process-observer stall, failure, and inventory controls.

use std::{
    cell::Cell,
    error::Error,
    path::Path,
    time::{Duration, Instant},
};

thread_local! {
    static NEXT_OBSERVATION_DELAY: Cell<Option<Duration>> = const { Cell::new(None) };
    static OMIT_NEXT_ANCHOR_ROW: Cell<bool> = const { Cell::new(false) };
    static OMIT_TARGET_ROWS: Cell<bool> = const { Cell::new(false) };
    static FAIL_NEXT_OBSERVER_COMMAND: Cell<bool> = const { Cell::new(false) };
}

#[cfg(target_os = "linux")]
const PS_PATH: &str = "/usr/bin/ps";
#[cfg(target_os = "macos")]
const PS_PATH: &str = "/bin/ps";
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
const PS_PATH: &str = "/usr/bin/ps";

pub(crate) fn process_observer_path() -> &'static Path {
    Path::new(PS_PATH)
}

pub(crate) fn delay_next_process_group_observation(delay: Duration) {
    NEXT_OBSERVATION_DELAY.with(|next| next.set(Some(delay)));
}

/// Stall the next observation inside the observer, never past its own window:
/// the stall is what the scenario injects, and the window is what it is
/// injected against.
pub(super) fn delay_observation_if_requested(observation_deadline: Instant) {
    if let Some(delay) = NEXT_OBSERVATION_DELAY.with(Cell::take) {
        let remaining = observation_deadline.saturating_duration_since(Instant::now());
        std::thread::sleep(delay.min(remaining));
    }
}

/// Fail the next observation the way a `ps` that ran and exited non-zero does.
///
/// The scenario this serves has to reach the *command* failing, not the
/// observation disagreeing with itself, so the injection stops at the observer
/// and never touches how a completed observation is read.
pub(crate) fn fail_next_process_group_observation_command() {
    FAIL_NEXT_OBSERVER_COMMAND.with(|fail| fail.set(true));
}

/// The real observer, asked for something it rejects.
///
/// Substituting the selector keeps the failure the observer's own -- `ps` runs,
/// refuses, and exits non-zero -- rather than fabricating an exit status the
/// platform never produced.
pub(super) fn injected_observer_arguments() -> Option<&'static [&'static str]> {
    const REJECTED_ARGUMENTS: &[&str] = &["--rafter-injected-observer-failure"];

    FAIL_NEXT_OBSERVER_COMMAND
        .with(|fail| fail.replace(false))
        .then_some(REJECTED_ARGUMENTS)
}

pub(crate) fn omit_anchor_from_next_process_group_observation() {
    OMIT_NEXT_ANCHOR_ROW.with(|omit| omit.set(true));
}

/// Scopes a target-row omission to the run that armed it.
pub(crate) struct TargetRowOmission;

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
#[must_use]
pub(crate) fn omit_target_rows_from_process_group_observations() -> TargetRowOmission {
    OMIT_TARGET_ROWS.with(|omit| omit.set(true));
    TargetRowOmission
}

/// Drop the rows an armed omission is hiding, leaving the inventory otherwise
/// exactly as `ps` reported it.
pub(super) fn omit_rows_if_requested(
    source: String,
    process_group: u32,
    anchor: Option<u32>,
) -> Result<String, Box<dyn Error>> {
    let mut source = source;
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
    if OMIT_TARGET_ROWS.with(Cell::get) {
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
    Ok(source)
}
