//! Scenarios for Maelstrom receipt classification and Java identity decoding.

use crate::EvidenceStatus;

use super::{java_major, valid_counterexample_attribution};

#[test]
fn receipt_accepts_combined_rd05_rd06_counterexample_only() {
    assert!(valid_counterexample_attribution(&[
        ("RD-05", EvidenceStatus::Fail),
        ("RD-06", EvidenceStatus::Fail),
        ("LG-04", EvidenceStatus::Incomplete),
    ]));
    assert!(!valid_counterexample_attribution(&[
        ("RD-05", EvidenceStatus::Fail),
        ("LG-04", EvidenceStatus::Fail),
    ]));

    assert_eq!(java_major("java 21.0.5 2024-10-15 LTS"), Some(21));
    assert_eq!(java_major("java version \"1.8.0_402\""), Some(8));
    assert_eq!(java_major("not-java"), None);
}
