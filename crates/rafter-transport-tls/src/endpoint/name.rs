//! Canonical TLS server names.

use std::{error::Error, fmt, net::IpAddr, str::FromStr, sync::Arc};

/// Maximum textual TLS server-name length.
pub const MAX_TLS_SERVER_NAME_BYTES: usize = 253;

/// Canonical DNS or IP identity used for TLS server-name verification.
///
/// DNS names are lowercased and must use ASCII labels. IP addresses are stored
/// in [`IpAddr`]'s canonical text form. Brackets and trailing DNS dots are
/// refused so one logical target has one endpoint-book representation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TlsServerName(Arc<str>);

impl TlsServerName {
    /// Validates and canonicalizes a TLS server name.
    ///
    /// # Errors
    ///
    /// Returns [`TlsServerNameError`] when `value` is not an IP address or an
    /// ASCII DNS name with valid labels.
    pub fn new(value: &str) -> Result<Self, TlsServerNameError> {
        if value.is_empty() {
            return Err(TlsServerNameError::Empty);
        }
        if value.len() > MAX_TLS_SERVER_NAME_BYTES {
            return Err(TlsServerNameError::TooLong {
                len: value.len(),
                maximum: MAX_TLS_SERVER_NAME_BYTES,
            });
        }
        if let Ok(address) = value.parse::<IpAddr>() {
            return Ok(Self(Arc::from(address.to_string())));
        }

        validate_dns_name(value)?;
        Ok(Self(Arc::from(value.to_ascii_lowercase())))
    }

    /// Returns the canonical server name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TlsServerName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for TlsServerName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for TlsServerName {
    type Err = TlsServerNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

/// Invalid TLS server name.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsServerNameError {
    /// The name was empty.
    Empty,
    /// The name exceeded [`MAX_TLS_SERVER_NAME_BYTES`].
    TooLong {
        /// Actual byte length.
        len: usize,
        /// Maximum byte length.
        maximum: usize,
    },
    /// DNS names must contain ASCII only.
    NonAscii {
        /// Byte offset of the first non-ASCII code unit.
        byte_index: usize,
    },
    /// A trailing dot would create a second spelling of the same DNS name.
    TrailingDot,
    /// One DNS label was empty.
    EmptyLabel {
        /// Zero-based label index.
        label_index: usize,
    },
    /// One DNS label exceeded 63 bytes.
    LabelTooLong {
        /// Zero-based label index.
        label_index: usize,
        /// Actual label length.
        len: usize,
    },
    /// A label began or ended with a hyphen.
    HyphenBoundary {
        /// Zero-based label index.
        label_index: usize,
    },
    /// A label contained a byte other than ASCII letter, digit, or hyphen.
    InvalidLabelByte {
        /// Zero-based label index.
        label_index: usize,
        /// Byte offset within the label.
        byte_index: usize,
        /// Invalid byte.
        byte: u8,
    },
}

impl fmt::Display for TlsServerNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("TLS server name must not be empty"),
            Self::TooLong { len, maximum } => write!(
                formatter,
                "TLS server name is {len} bytes, exceeding maximum {maximum}"
            ),
            Self::NonAscii { byte_index } => write!(
                formatter,
                "TLS DNS server name contains non-ASCII data at byte {byte_index}"
            ),
            Self::TrailingDot => {
                formatter.write_str("TLS DNS server name must not have a trailing dot")
            }
            Self::EmptyLabel { label_index } => {
                write!(
                    formatter,
                    "TLS DNS server name label {label_index} is empty"
                )
            }
            Self::LabelTooLong { label_index, len } => write!(
                formatter,
                "TLS DNS server name label {label_index} is {len} bytes, exceeding 63"
            ),
            Self::HyphenBoundary { label_index } => write!(
                formatter,
                "TLS DNS server name label {label_index} begins or ends with a hyphen"
            ),
            Self::InvalidLabelByte {
                label_index,
                byte_index,
                byte,
            } => write!(
                formatter,
                "TLS DNS server name label {label_index} has invalid byte {byte:#04x} at \
                 offset {byte_index}"
            ),
        }
    }
}

impl Error for TlsServerNameError {}

fn validate_dns_name(value: &str) -> Result<(), TlsServerNameError> {
    if value.ends_with('.') {
        return Err(TlsServerNameError::TrailingDot);
    }
    if let Some(byte_index) = value.bytes().position(|byte| !byte.is_ascii()) {
        return Err(TlsServerNameError::NonAscii { byte_index });
    }
    for (label_index, label) in value.as_bytes().split(|byte| *byte == b'.').enumerate() {
        validate_dns_label(label_index, label)?;
    }
    Ok(())
}

fn validate_dns_label(label_index: usize, label: &[u8]) -> Result<(), TlsServerNameError> {
    if label.is_empty() {
        return Err(TlsServerNameError::EmptyLabel { label_index });
    }
    if label.len() > 63 {
        return Err(TlsServerNameError::LabelTooLong {
            label_index,
            len: label.len(),
        });
    }
    if label.first() == Some(&b'-') || label.last() == Some(&b'-') {
        return Err(TlsServerNameError::HyphenBoundary { label_index });
    }

    for (byte_index, byte) in label.iter().copied().enumerate() {
        if !byte.is_ascii_alphanumeric() && byte != b'-' {
            return Err(TlsServerNameError::InvalidLabelByte {
                label_index,
                byte_index,
                byte,
            });
        }
    }
    Ok(())
}
