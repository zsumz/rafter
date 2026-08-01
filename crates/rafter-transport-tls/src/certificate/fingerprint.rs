//! Canonical SHA-256 leaf-certificate fingerprints.

use std::{error::Error, fmt, str::FromStr};

use sha2::{Digest, Sha256};

const CERTIFICATE_FINGERPRINT_BYTES: usize = 32;

/// SHA-256 fingerprint of one DER-encoded leaf certificate.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CertificateFingerprint([u8; CERTIFICATE_FINGERPRINT_BYTES]);

impl CertificateFingerprint {
    /// Fingerprint length in bytes.
    pub const BYTE_LEN: usize = CERTIFICATE_FINGERPRINT_BYTES;
    /// Lowercase hexadecimal fingerprint length.
    pub const HEX_LEN: usize = Self::BYTE_LEN * 2;

    /// Computes the SHA-256 fingerprint of exact DER bytes.
    #[must_use]
    pub fn from_der(certificate_der: &[u8]) -> Self {
        let digest = Sha256::digest(certificate_der);
        let mut bytes = [0_u8; Self::BYTE_LEN];
        bytes.copy_from_slice(&digest);
        Self(bytes)
    }

    /// Creates a fingerprint from exact digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; Self::BYTE_LEN]) -> Self {
        Self(bytes)
    }

    /// Returns exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::BYTE_LEN] {
        &self.0
    }
}

impl fmt::Display for CertificateFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for CertificateFingerprint {
    type Err = CertificateFingerprintParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != Self::HEX_LEN {
            return Err(CertificateFingerprintParseError::InvalidLength {
                len: value.len(),
                expected: Self::HEX_LEN,
            });
        }

        let mut bytes = [0_u8; Self::BYTE_LEN];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high =
                decode_hex_nibble(pair[0]).ok_or(CertificateFingerprintParseError::InvalidHex {
                    byte_index: index * 2,
                    byte: pair[0],
                })?;
            let low =
                decode_hex_nibble(pair[1]).ok_or(CertificateFingerprintParseError::InvalidHex {
                    byte_index: index * 2 + 1,
                    byte: pair[1],
                })?;
            bytes[index] = high << 4 | low;
        }
        Ok(Self(bytes))
    }
}

/// Invalid hexadecimal certificate fingerprint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CertificateFingerprintParseError {
    /// The input was not exactly 64 hexadecimal bytes.
    InvalidLength {
        /// Actual text length.
        len: usize,
        /// Required text length.
        expected: usize,
    },
    /// One input byte was not hexadecimal ASCII.
    InvalidHex {
        /// Offset of the invalid byte.
        byte_index: usize,
        /// Invalid byte.
        byte: u8,
    },
}

impl fmt::Display for CertificateFingerprintParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { len, expected } => write!(
                formatter,
                "certificate fingerprint has length {len}, expected {expected}"
            ),
            Self::InvalidHex { byte_index, byte } => write!(
                formatter,
                "certificate fingerprint byte {byte_index} is not hexadecimal ASCII: \
                 {byte:#04x}"
            ),
        }
    }
}

impl Error for CertificateFingerprintParseError {}

const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
