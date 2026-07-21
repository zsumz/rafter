//! Thread-local lifecycle for exactly one detector-test invocation.

use std::{
    cell::RefCell,
    sync::atomic::{AtomicBool, Ordering},
};

use super::{
    outcome::DetectorTestOutcome, proof::DetectorProofChannel, wire::TOKEN_ENV,
    witness::DetectorWitness,
};

static OBSERVED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Default)]
enum DetectorTestState {
    #[default]
    Idle,
    Active {
        gate: DetectorGate,
        witnesses: Vec<DetectorWitness>,
    },
}

#[derive(Debug, Default)]
pub(super) enum DetectorGate {
    #[default]
    Standalone,
    ProofBound {
        token: String,
        challenge: String,
    },
    SetupFailed,
}

std::thread_local! {
    static DETECTOR_TEST_STATE: RefCell<DetectorTestState> = RefCell::default();
}

#[doc(hidden)]
pub fn begin_detector_test() {
    DETECTOR_TEST_STATE.with_borrow_mut(|state| {
        assert!(
            matches!(state, DetectorTestState::Idle),
            "detector test session was started twice"
        );
        *state = DetectorTestState::Active {
            gate: detector_gate(),
            witnesses: Vec::new(),
        };
    });
    OBSERVED.store(false, Ordering::Relaxed);
}

#[doc(hidden)]
#[must_use]
pub fn detector_test_outcome() -> DetectorTestOutcome {
    DETECTOR_TEST_STATE.with_borrow_mut(|state| match std::mem::take(state) {
        DetectorTestState::Idle => DetectorTestOutcome::not_started(),
        DetectorTestState::Active { gate, witnesses } => {
            DetectorTestOutcome::completed(gate, witnesses)
        }
    })
}

pub(crate) fn record_expected_rejection(identity: &'static str) {
    record_witness(DetectorWitness::expected_rejection(identity));
}

pub(crate) fn record_recorder_invocation(identity: &'static str) {
    record_witness(DetectorWitness::recorder_invocation(identity));
}

pub(crate) fn mark_first_observation() -> bool {
    !OBSERVED.swap(true, Ordering::Relaxed)
}

fn detector_gate() -> DetectorGate {
    let token = std::env::var(TOKEN_ENV);
    let channel = DetectorProofChannel::connect();
    match (token, channel) {
        (Ok(token), Ok(mut channel)) => channel
            .challenge()
            .map_or(DetectorGate::SetupFailed, |challenge| {
                DetectorGate::ProofBound { token, challenge }
            }),
        (Err(std::env::VarError::NotPresent), Err(())) => DetectorGate::Standalone,
        (Ok(_), Err(()))
        | (Err(std::env::VarError::NotPresent), Ok(_))
        | (Err(std::env::VarError::NotUnicode(_)), _) => DetectorGate::SetupFailed,
    }
}

fn record_witness(witness: DetectorWitness) {
    DETECTOR_TEST_STATE.with_borrow_mut(|state| {
        if let DetectorTestState::Active { witnesses, .. } = state {
            witnesses.push(witness);
        }
    });
}
