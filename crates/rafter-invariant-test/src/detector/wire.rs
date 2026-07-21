//! Stable environment and stderr protocol shared with the invariant runner.

use std::fmt;

use super::witness::DetectorWitness;

pub(super) const TOKEN_ENV: &str = "RAFTER_INVARIANT_ORACLE_TOKEN";
const OBSERVED_PREFIX: &str = "RAFTER_INVARIANT_ORACLE_OBSERVED:";
const VIOLATION_PREFIX: &str = "RAFTER_INVARIANT_ORACLE_VIOLATION:";
const DETECTOR_WITNESS_PREFIX: &str = "RAFTER_INVARIANT_DETECTOR_WITNESS:";
const DETECTOR_PROOF_PREFIX: &str = "RAFTER_INVARIANT_DETECTOR_PROOF:";
pub(super) const DETECTOR_PROOF_FD_ENV: &str = "RAFTER_INVARIANT_DETECTOR_PROOF_FD";
#[cfg(unix)]
pub(super) const DETECTOR_CHALLENGE_BYTES: usize = 32;
#[cfg(unix)]
pub(super) const DETECTOR_PROOF_REQUEST: u8 = 0xa7;

pub(crate) fn emit_observed() {
    if let Ok(token) = std::env::var(TOKEN_ENV) {
        eprintln!("{OBSERVED_PREFIX}{token}");
    }
}

pub(crate) fn violation_message(message: fmt::Arguments<'_>) -> String {
    match std::env::var(TOKEN_ENV) {
        Ok(token) => format!("{VIOLATION_PREFIX}{token}: {message}"),
        Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => {
            message.to_string()
        }
    }
}

pub(super) fn emit_witness(token: &str, witness: DetectorWitness) {
    eprintln!(
        "{DETECTOR_WITNESS_PREFIX}{token}:{}:{}()",
        witness.kind(),
        witness.identity()
    );
}

pub(super) fn emit_proof(token: &str, witness: DetectorWitness, challenge: &str) {
    let contract = format!("{}:{}", witness.kind(), witness.identity());
    eprintln!("{DETECTOR_PROOF_PREFIX}{token}:{contract}():{challenge}");
}

#[cfg(test)]
pub(crate) fn fabricate_witness(kind: &str, detector: &str) {
    if let Ok(token) = std::env::var(TOKEN_ENV) {
        eprintln!("{DETECTOR_WITNESS_PREFIX}{token}:{kind}:{detector}()");
    }
}
