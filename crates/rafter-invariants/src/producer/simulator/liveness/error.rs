//! Producer classifications for unusable liveness output.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer::simulator) enum LivenessReportErrorKind {
    Missing,
    Malformed,
}

#[derive(Debug)]
pub(in crate::producer::simulator) struct LivenessReportError {
    pub(in crate::producer::simulator) kind: LivenessReportErrorKind,
    pub(in crate::producer::simulator) message: String,
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
