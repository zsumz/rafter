use std::{error::Error, fmt};

use crate::{
    check_guarded_history, check_linearizable, CheckError, CheckReport, GuardedCheckError,
    GuardedCheckReport, GuardedHistoryEvent, HistoryEvent, LockConfig,
};

/// Coverage from the independent lock and guarded-resource checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvidenceReport {
    /// Black-box lock-history coverage.
    pub lock: CheckReport,
    /// External guarded-resource coverage.
    pub guarded: GuardedCheckReport,
}

/// Which independent evidence check failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvidenceError {
    /// The black-box lock history was malformed, undecided, or impossible.
    Lock(CheckError),
    /// The external guarded-resource history was malformed or impossible.
    Guarded(GuardedCheckError),
}

/// Runs the independent lock and guarded-resource checks together.
///
/// The two histories remain separate and use separate specifications. This
/// function only gives test harnesses one place to require both reports.
///
/// # Errors
///
/// Returns the exact error from the first failing independent check.
pub fn check_evidence(
    config: LockConfig,
    lock_history: &[HistoryEvent],
    guarded_history: &[GuardedHistoryEvent],
) -> Result<EvidenceReport, EvidenceError> {
    let lock = check_linearizable(config, lock_history).map_err(EvidenceError::Lock)?;
    let guarded = check_guarded_history(guarded_history).map_err(EvidenceError::Guarded)?;
    Ok(EvidenceReport { lock, guarded })
}

impl fmt::Display for EvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lock(error) => write!(formatter, "lock-history check failed: {error}"),
            Self::Guarded(error) => {
                write!(formatter, "guarded-resource check failed: {error}")
            }
        }
    }
}

impl Error for EvidenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Lock(error) => Some(error),
            Self::Guarded(error) => Some(error),
        }
    }
}
