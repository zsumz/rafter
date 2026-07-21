//! Tests for verifier-owned replay deadlines.

use std::time::Duration;

use super::{ReplayDeadlines, PUBLICATION_RESERVE};

#[test]
fn work_stops_before_the_absolute_publication_deadline() {
    let deadlines =
        ReplayDeadlines::from_timeout(Duration::from_secs(900)).expect("derive replay deadlines");

    assert_eq!(
        deadlines.publication().duration_since(deadlines.work()),
        PUBLICATION_RESERVE
    );
}

#[test]
fn total_timeout_must_leave_the_publication_reserve() {
    let error = ReplayDeadlines::from_timeout(PUBLICATION_RESERVE)
        .expect_err("zero work budget must fail closed");
    assert!(error.contains("publication reserve"), "{error}");
    assert_eq!(PUBLICATION_RESERVE, Duration::from_secs(20));
}
