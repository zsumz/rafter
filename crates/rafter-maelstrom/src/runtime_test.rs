//! The tick interval the process loop drives evidence on.
//!
//! The interval must be explicit and non-zero: a zero value would spin the loop
//! and a defaulted one would stall evidence, and the kernel's determinism rests
//! on this loop choosing the interval rather than wall time.

use std::time::Duration;

use super::tick_interval;

#[test]
fn evidence_tick_interval_is_explicit_and_nonzero() {
    assert_eq!(tick_interval(None), Ok(Duration::from_millis(50)));
    assert_eq!(tick_interval(Some("25")), Ok(Duration::from_millis(25)));
    assert!(tick_interval(Some("0")).is_err());
    assert!(tick_interval(Some("bad")).is_err());
}
