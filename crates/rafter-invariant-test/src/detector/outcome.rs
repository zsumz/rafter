//! Opaque detector-test result and its libtest termination contract.

use std::process::{ExitCode, Termination};

use super::{
    session::DetectorGate,
    wire::{self, TOKEN_ENV},
    witness::DetectorWitness,
};

/// Opaque successful return value produced by [`crate::detector_test`].
#[derive(Debug)]
pub struct DetectorTestOutcome {
    completed: Option<CompletedDetectorTest>,
}

impl DetectorTestOutcome {
    pub(super) const fn not_started() -> Self {
        Self { completed: None }
    }

    pub(super) fn completed(gate: DetectorGate, witnesses: Vec<DetectorWitness>) -> Self {
        Self {
            completed: Some(CompletedDetectorTest { gate, witnesses }),
        }
    }
}

#[derive(Debug)]
struct CompletedDetectorTest {
    gate: DetectorGate,
    witnesses: Vec<DetectorWitness>,
}

impl Termination for DetectorTestOutcome {
    fn report(self) -> ExitCode {
        let Some(CompletedDetectorTest { gate, witnesses }) = self.completed else {
            eprintln!("detector test returned without an invocation-bound rejection");
            return ExitCode::FAILURE;
        };
        let has_rejection = witnesses
            .iter()
            .any(|witness| witness.is_expected_rejection());
        if !has_rejection {
            eprintln!("detector test returned without an invocation-bound rejection");
            return ExitCode::FAILURE;
        }
        let (token, mut channel) = match gate {
            DetectorGate::Standalone => return ExitCode::SUCCESS,
            DetectorGate::SetupFailed => {
                eprintln!("detector test started with an invalid gate token");
                return ExitCode::FAILURE;
            }
            DetectorGate::ProofBound { token, channel } => match std::env::var(TOKEN_ENV) {
                Ok(current) if current == token => (token, channel),
                Ok(_) => {
                    eprintln!("detector test returned with a different gate token");
                    return ExitCode::FAILURE;
                }
                Err(_) => {
                    eprintln!("detector test returned without its gate token");
                    return ExitCode::FAILURE;
                }
            },
        };
        let Some(challenge) = channel.challenge() else {
            eprintln!("detector test could not complete its post-invocation proof");
            return ExitCode::FAILURE;
        };
        for witness in witnesses {
            wire::emit_witness(&token, witness);
            wire::emit_proof(&token, witness, &challenge);
        }
        ExitCode::SUCCESS
    }
}
