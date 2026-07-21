//! Tests for standalone bounded process execution policy.

use std::time::{Duration, Instant};

use super::policies_at;

#[test]
fn process_policy_clamps_target_time_to_the_absolute_lifecycle_deadline() {
    let now = Instant::now();
    let outer_deadline = now + Duration::from_secs(60);
    let (deadlines, _, _) = policies_at(now, Duration::from_secs(120), outer_deadline)
        .expect("outer deadline leaves a target window");

    assert_eq!(deadlines.target_timeout, Duration::from_secs(10));
    assert_eq!(deadlines.lifecycle, outer_deadline);
}

#[test]
fn process_policy_rejects_an_outer_deadline_without_lifecycle_reserve() {
    let now = Instant::now();
    let error = policies_at(now, Duration::from_secs(1), now + Duration::from_secs(50))
        .expect_err("the complete lifecycle reserve is mandatory");

    assert!(error.contains("no target execution budget"));
}
