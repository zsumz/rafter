//! Dispatch to independent runner-family receipt policies.

use std::collections::BTreeMap;

use crate::{
    contract::{
        catalog::EvidenceDescriptor,
        profile::{EvidenceLayer, RunnerContract},
    },
    evidence::ResultBundle,
};

pub(super) fn validate(
    layer: EvidenceLayer,
    bundle: &ResultBundle,
    runner_contract: &RunnerContract,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
) -> Result<(), &'static str> {
    match layer {
        EvidenceLayer::Tests => {
            crate::verification::test_runner::validate_receipt(bundle, expected)
        }
        EvidenceLayer::Simulator => {
            crate::verification::simulator::validate_receipt(bundle, expected, runner_contract)
        }
        EvidenceLayer::Tla => {
            crate::verification::tla::validate_receipt(bundle, expected, runner_contract)
        }
        EvidenceLayer::Maelstrom => {
            crate::verification::maelstrom::validate_receipt(bundle, expected, runner_contract)
        }
    }
}
