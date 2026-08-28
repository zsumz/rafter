//! Environment-supplied lease and evidence settings read before the node opens.
//!
//! Lease reads must stay off unless an explicit evidence flag turns them on,
//! and every evidence timing value must resolve to a positive tick count, so a
//! misread environment cannot quietly weaken read safety.

use super::{lease_reads_enabled, timing_value};

#[test]
fn lease_reads_are_enabled_only_by_an_explicit_evidence_flag() {
    assert!(lease_reads_enabled(Some("1")));
    assert!(lease_reads_enabled(Some("true")));
    assert!(!lease_reads_enabled(None));
    assert!(!lease_reads_enabled(Some("0")));
    assert!(!lease_reads_enabled(Some("TRUE")));
}

#[test]
fn evidence_timing_values_are_explicit_positive_ticks() {
    assert_eq!(timing_value("ticks", None, 5), Ok(5));
    assert_eq!(timing_value("ticks", Some("20"), 5), Ok(20));
    assert!(timing_value("ticks", Some("0"), 5).is_err());
    assert!(timing_value("ticks", Some("nope"), 5).is_err());
}
