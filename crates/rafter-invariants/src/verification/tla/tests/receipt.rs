//! TLA+ runner receipt artifact-inventory scenarios.

use super::required_proof_artifact_kinds;
use crate::evidence::format::tla::{detector_config_kind, detector_log_kind, DETECTOR_PROBES};

#[test]
fn passing_receipt_requires_two_artifacts_per_detector_probe() {
    let kinds = required_proof_artifact_kinds(false, false);
    assert_eq!(kinds.len(), 13 + 2 * DETECTOR_PROBES.len());
    assert!(!kinds.contains("tla-detector-log"));
    for probe in DETECTOR_PROBES {
        assert!(kinds.contains(&detector_log_kind(probe).expect("registered probe")));
        assert!(kinds.contains(&detector_config_kind(probe).expect("registered probe")));
    }
}

#[test]
fn checkpointed_pass_requires_recovery_and_final_inventory_artifacts() {
    let fresh = required_proof_artifact_kinds(true, false);
    assert!(fresh.contains("tla-checkpoint-recovery-report"));
    assert!(fresh.contains("tla-checkpoint-contract"));
    assert!(fresh.contains("tla-checkpoint-inventory"));
    assert!(!fresh.contains("tla-checkpoint-recovered-contract"));

    let recovered = required_proof_artifact_kinds(true, true);
    assert!(recovered.contains("tla-checkpoint-recovered-contract"));
    assert!(recovered.contains("tla-checkpoint-recovered-inventory"));
}
