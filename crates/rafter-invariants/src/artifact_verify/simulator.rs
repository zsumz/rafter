//! Compatibility facade for the verification-owned simulator domain.

#[cfg(test)]
pub(super) use crate::verification::simulator::event_semantics_test_support::{
    derive_check_contract_issue, derive_simulator_observation_counts, index_simulator_event,
    inspect_machine_events, raw_event_issue, verified_passing_simulator_event_contract,
    verify_composite_observation, verify_negative_detector_evidence_with,
    verify_negative_fixture_binding, verify_nonpassing_event_classification,
    verify_simulator_observations, RawEventIssue,
};

#[cfg(test)]
#[path = "../verification/simulator/tests/event_semantics.rs"]
mod event_semantics_tests;
