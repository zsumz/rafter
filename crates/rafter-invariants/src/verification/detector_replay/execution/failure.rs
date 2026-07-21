//! Compilation failure state with retained bounded-process diagnostics.

use std::error::Error;

use super::super::process::{
    retained_diagnostics, ReplayProcessOutput, RetainedProcessDiagnostics,
};

pub(in crate::verification::detector_replay) struct CompilationFailure {
    pub(in crate::verification::detector_replay) message: String,
    pub(in crate::verification::detector_replay) metadata_output: Option<ReplayProcessOutput>,
    pub(in crate::verification::detector_replay) compiler_output: Option<ReplayProcessOutput>,
    pub(in crate::verification::detector_replay) retained_diagnostics:
        Option<RetainedProcessDiagnostics>,
}

impl CompilationFailure {
    #[cfg(not(target_os = "linux"))]
    pub(in crate::verification::detector_replay) fn unsupported_platform() -> Self {
        Self::setup("detector replay requires descriptor-bound executable launch on Linux")
    }

    pub(super) fn setup(error: impl std::fmt::Display) -> Self {
        Self {
            message: error.to_string(),
            metadata_output: None,
            compiler_output: None,
            retained_diagnostics: None,
        }
    }

    pub(super) fn setup_error(error: &(dyn Error + 'static)) -> Self {
        let retained_diagnostics = retained_diagnostics(error);
        Self {
            message: error.to_string(),
            metadata_output: None,
            compiler_output: None,
            retained_diagnostics,
        }
    }

    pub(super) fn after_metadata(
        error: impl std::fmt::Display,
        metadata_output: ReplayProcessOutput,
    ) -> Self {
        Self {
            message: error.to_string(),
            metadata_output: Some(metadata_output),
            compiler_output: None,
            retained_diagnostics: None,
        }
    }

    pub(super) fn after_metadata_error(
        error: &(dyn Error + 'static),
        metadata_output: ReplayProcessOutput,
    ) -> Self {
        let retained_diagnostics = retained_diagnostics(error);
        Self {
            message: error.to_string(),
            metadata_output: Some(metadata_output),
            compiler_output: None,
            retained_diagnostics,
        }
    }

    pub(super) fn after_compiler(
        error: impl std::fmt::Display,
        metadata_output: ReplayProcessOutput,
        compiler_output: ReplayProcessOutput,
    ) -> Self {
        Self {
            message: error.to_string(),
            metadata_output: Some(metadata_output),
            compiler_output: Some(compiler_output),
            retained_diagnostics: None,
        }
    }
}
