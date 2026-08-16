//! The exact artifact inventory a passing TLA+ receipt must carry.
//!
//! Split out of receipt validation because it answers a different question.
//! Receipt validation asks whether the receipt's claims agree with each other
//! and with the contract; this asks only what set of artifact kinds the run
//! should have produced, which is a function of the pinned configuration and
//! nothing about the run's outcome. The caller compares the two sets and
//! rejects any receipt that is missing a kind, carries an unexpected one, or
//! lists the same kind twice.

use std::collections::BTreeSet;

use crate::contract::profile::RunnerContract;
use crate::evidence::format::tla::checkpoint::{
    CONTRACT_KIND, INVENTORY_KIND, RECOVERED_CONTRACT_KIND, RECOVERED_INVENTORY_KIND,
    RECOVERY_REPORT_KIND,
};
use crate::evidence::format::tla::{
    detector_config_kind, detector_log_kind, DETECTOR_PROBES, MUTATION_SUITE_ARTIFACT_KIND,
};

pub(super) fn required_proof_artifact_kinds(
    checkpoint_enabled: bool,
    checkpoint_candidate_present: bool,
    contract: &RunnerContract,
) -> BTreeSet<String> {
    let mut kinds = [
        "tla-log",
        "tla-trace-log",
        "tla-tool",
        "tla-spec",
        "tla-trace-spec",
        "tla-detector-spec",
        "tla-runner",
        "tla-tool-asset-id",
        "tla-tool-checksums",
        "tla-config",
        "tla-trace-config",
        "tla-detector-config",
        MUTATION_SUITE_ARTIFACT_KIND,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    // An unregistered probe yields a kind no artifact can match rather than
    // being skipped, so a registry that stops naming a probe fails the
    // inventory comparison instead of quietly shrinking what is required.
    for probe in DETECTOR_PROBES {
        kinds.insert(detector_log_kind(probe).unwrap_or_else(|| {
            format!(
                "invalid-tla-detector-log:{}:{}",
                probe.predicate, probe.mode
            )
        }));
        kinds.insert(detector_config_kind(probe).unwrap_or_else(|| {
            format!(
                "invalid-tla-detector-config:{}:{}",
                probe.predicate, probe.mode
            )
        }));
    }
    kinds.extend(super::obligation::artifact_kinds(contract));
    if checkpoint_enabled {
        kinds.extend([
            CONTRACT_KIND.to_owned(),
            INVENTORY_KIND.to_owned(),
            RECOVERY_REPORT_KIND.to_owned(),
        ]);
        if checkpoint_candidate_present {
            kinds.extend([
                RECOVERED_CONTRACT_KIND.to_owned(),
                RECOVERED_INVENTORY_KIND.to_owned(),
            ]);
        }
    }
    kinds
}
