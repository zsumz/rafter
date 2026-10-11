//! Platform admission precedes every replay compilation or fixture launch.

use super::{execution::CompilationFailure, result::DetectorReplayAttempt};

pub(super) fn run(attempt: impl FnOnce() -> DetectorReplayAttempt) -> DetectorReplayAttempt {
    // Keep the shared pipeline type-checked on Unix hosts while admitting
    // execution only where descriptor-bound launch is supported. Removing its
    // caller with #[cfg] orphaned the pipeline on macOS under -D warnings.
    if cfg!(target_os = "linux") {
        attempt()
    } else {
        DetectorReplayAttempt::CompilationFailed(Box::new(
            CompilationFailure::unsupported_platform(),
        ))
    }
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
