//! Fail-closed verifier errors for raw liveness evidence.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LivenessReportErrorKind {
    Missing,
    Malformed,
}

#[derive(Debug)]
pub(crate) struct LivenessReportError {
    #[cfg_attr(not(test), allow(dead_code))]
    pub kind: LivenessReportErrorKind,
    pub message: String,
}

pub(super) fn missing(message: impl Into<String>) -> LivenessReportError {
    LivenessReportError {
        kind: LivenessReportErrorKind::Missing,
        message: message.into(),
    }
}

pub(super) fn malformed(message: impl Into<String>) -> LivenessReportError {
    LivenessReportError {
        kind: LivenessReportErrorKind::Malformed,
        message: message.into(),
    }
}
