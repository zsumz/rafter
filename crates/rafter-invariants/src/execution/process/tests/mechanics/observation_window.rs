//! Observation-window scenarios: which bound ended a `ps` run, and what that
//! means for the caller that asked for it.

use std::time::{Duration, Instant};

use super::super::super::{process_group_observation, ps_telemetry_timeout, GroupObservation};
use super::super::support::process_observer;

/// A window that was already over before the observer was entered means
/// something consumed it inside this call. That is a stalled observer, not a
/// window closing underneath a running one, and it stays fail-closed.
#[cfg(unix)]
#[test]
fn an_already_closed_window_is_still_fail_closed() {
    let observer = process_observer();
    let error = process_group_observation(
        std::process::id(),
        None,
        &observer,
        Instant::now(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect_err("an observer entered after its window is a stall, not an ending");
    assert!(
        error
            .to_string()
            .contains("observer exhausted its absolute deadline"),
        "{error}"
    );
}

/// The reclassification is bounded by the observation budget itself, so the
/// rule and the timeout cannot drift apart: a window at least this long is
/// never truncated, and an observation inside it is never excused.
#[cfg(unix)]
#[test]
fn an_untruncated_observation_is_never_excused_as_a_closed_window() {
    let observer = process_observer();
    // Strictly more than the budget, so the budget bounds the run and the
    // window does not.
    let window = Instant::now() + ps_telemetry_timeout() + Duration::from_secs(5);
    let outcome = process_group_observation(std::process::id(), None, &observer, window, window);
    assert!(
        !matches!(outcome, Ok(GroupObservation::WindowClosed)),
        "an observation offered its whole budget must observe or fail, never be excused"
    );
}

/// A window shorter than the observation budget still observes. This is the
/// short-grace configuration -- a two-second termination grace can never offer
/// a full budget, and refusing to look inside it would escalate every such
/// window straight to SIGKILL without ever asking whether the group was already
/// quiescent.
#[cfg(unix)]
#[test]
fn a_window_shorter_than_the_budget_still_observes() {
    let observer = process_observer();
    let window = Instant::now() + ps_telemetry_timeout() / 2;
    let outcome = process_group_observation(
        std::process::id(),
        None,
        &observer,
        window,
        Instant::now() + Duration::from_secs(30),
    )
    .expect("a truncated observation that completes is not a failure");
    if matches!(outcome, GroupObservation::WindowClosed) {
        assert!(
            Instant::now() >= window,
            "a closed window must actually have closed"
        );
    }
}
