//! State-count gates and resource caps for the scheduled exhaustive profiles.
//!
//! A scheduled profile passes only when it reaches its target count of both
//! unique protocol states and unique verifier states, and the wall-clock and
//! unique-state caps must be applied so a scheduled job terminates.

use super::*;

#[test]
fn exhaustive_target_gate_requires_protocol_and_verifier_state_counts() {
    assert_eq!(
        target_values(Profile::RaftNightly),
        (13_000_000, 13_000_000)
    );
    assert_eq!(
        target_values(Profile::RaftWeekly),
        (250_000_000, 250_000_000)
    );
    assert_eq!(Profile::Fast.exhaustive_targets(), None);
    let target = 13_000_000;

    assert!(assert_exhaustive_targets(Profile::RaftNightly, target, target).is_ok());
    let protocol_error = assert_exhaustive_targets(Profile::RaftNightly, target - 1, target)
        .expect_err("below-target protocol states should fail");
    let verifier_error = assert_exhaustive_targets(Profile::RaftNightly, target, target - 1)
        .expect_err("below-target verifier states should fail");

    assert!(protocol_error.to_string().contains("protocol states"));
    assert!(verifier_error.to_string().contains("verifier states"));
}

#[test]
fn scheduled_profiles_apply_wall_clock_and_unique_state_caps() {
    let nightly = scheduled_exhaustive_bounds(Profile::RaftNightly, Bounds::new(8));
    assert_eq!(
        nightly.max_unique_states(),
        Some(NIGHTLY_EXHAUSTIVE_MAX_UNIQUE_STATES)
    );
    assert_eq!(
        nightly.max_wall_clock(),
        Some(NIGHTLY_EXHAUSTIVE_MAX_WALL_CLOCK)
    );

    let weekly = scheduled_exhaustive_bounds(Profile::RaftWeekly, Bounds::new(9));
    assert_eq!(
        weekly.max_unique_states(),
        Some(WEEKLY_EXHAUSTIVE_MAX_UNIQUE_STATES)
    );
    assert_eq!(
        weekly.max_wall_clock(),
        Some(WEEKLY_EXHAUSTIVE_MAX_WALL_CLOCK)
    );

    let fast = scheduled_exhaustive_bounds(Profile::Fast, Bounds::new(7));
    assert_eq!(fast.max_unique_states(), None);
    assert_eq!(fast.max_wall_clock(), None);
}
