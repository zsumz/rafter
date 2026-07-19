//! Process wire-format failures separated from verifier acceptance policy.

use std::{error::Error, fmt, str::Utf8Error};

#[derive(Debug)]
pub(crate) enum ProcessFormatError {
    EmptyTranscript,
    InvalidDetectorChallenge,
    InvalidExitCode,
    InvalidLabel,
    InvalidMetric(&'static str),
    InvalidTimeout,
    InvalidUtf8(Utf8Error),
    InvalidUtf8Boundary(&'static str),
    Json(serde_json::Error),
    MalformedCombinedHeader(&'static str),
    MissingTermination,
    PayloadLengthOverflow,
    TruncatedPayload,
    UnexpectedTermination,
    UnsupportedCombinedSchema(u32),
    UnsupportedSchema { expected: u32, observed: u32 },
    ZeroPeakRss,
}

impl fmt::Display for ProcessFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyTranscript => write!(formatter, "combined process log is empty"),
            Self::InvalidDetectorChallenge => {
                write!(
                    formatter,
                    "detector challenge is not 32 lowercase hexadecimal bytes"
                )
            }
            Self::InvalidExitCode => write!(formatter, "combined process exit code is malformed"),
            Self::InvalidLabel => write!(formatter, "process label is empty or contains a newline"),
            Self::InvalidMetric(field) => {
                write!(
                    formatter,
                    "combined process metric {field} is missing or invalid"
                )
            }
            Self::InvalidTimeout => {
                write!(formatter, "combined process timeout status is malformed")
            }
            Self::InvalidUtf8(error) => write!(formatter, "process output is not UTF-8: {error}"),
            Self::InvalidUtf8Boundary(stream) => {
                write!(formatter, "combined process {stream} boundary is not UTF-8")
            }
            Self::Json(error) => write!(formatter, "process JSON is invalid: {error}"),
            Self::MalformedCombinedHeader(field) => {
                write!(
                    formatter,
                    "combined process log omitted or malformed {field}"
                )
            }
            Self::MissingTermination => {
                write!(formatter, "process schema requires a termination receipt")
            }
            Self::PayloadLengthOverflow => {
                write!(formatter, "combined process payload length overflowed")
            }
            Self::TruncatedPayload => write!(formatter, "combined process payload was truncated"),
            Self::UnexpectedTermination => {
                write!(formatter, "process schema forbids a termination receipt")
            }
            Self::UnsupportedCombinedSchema(observed) => write!(
                formatter,
                "combined process schema version {observed} is unsupported"
            ),
            Self::UnsupportedSchema { expected, observed } => write!(
                formatter,
                "process schema version {observed} does not match required version {expected}"
            ),
            Self::ZeroPeakRss => write!(formatter, "combined process log omitted peak RSS"),
        }
    }
}

impl Error for ProcessFormatError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUtf8(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<Utf8Error> for ProcessFormatError {
    fn from(error: Utf8Error) -> Self {
        Self::InvalidUtf8(error)
    }
}

impl From<serde_json::Error> for ProcessFormatError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub(super) fn validate_label(label: &str) -> Result<(), ProcessFormatError> {
    if label.is_empty() || label.contains('\n') || label.contains('\r') {
        return Err(ProcessFormatError::InvalidLabel);
    }
    Ok(())
}
