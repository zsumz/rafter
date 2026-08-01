//! Finite immutable certificate-directory construction.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::Arc,
};

use crate::{CertificateDirectoryLimits, PeerId};

use super::{CertificateDirectory, CertificateDirectoryState, CertificateFingerprint};

/// Builder for one immutable [`CertificateDirectory`].
#[derive(Debug)]
pub struct CertificateDirectoryBuilder {
    limits: CertificateDirectoryLimits,
    by_fingerprint: BTreeMap<CertificateFingerprint, PeerId>,
    peers: BTreeSet<PeerId>,
}

impl CertificateDirectoryBuilder {
    /// Creates an empty builder with finite limits.
    #[must_use]
    pub fn new(limits: CertificateDirectoryLimits) -> Self {
        Self {
            limits,
            by_fingerprint: BTreeMap::new(),
            peers: BTreeSet::new(),
        }
    }

    /// Maps one DER leaf certificate to a stable principal.
    ///
    /// # Errors
    ///
    /// Returns [`CertificateDirectoryError`] for empty DER, conflicting
    /// mappings, or an exhausted finite bound.
    pub fn map_certificate(
        mut self,
        certificate_der: &[u8],
        peer_id: PeerId,
    ) -> Result<Self, CertificateDirectoryError> {
        if certificate_der.is_empty() {
            return Err(CertificateDirectoryError::EmptyCertificate);
        }
        let fingerprint = CertificateFingerprint::from_der(certificate_der);
        self.insert(fingerprint, peer_id)?;
        Ok(self)
    }

    /// Maps one already-computed fingerprint to a stable principal.
    ///
    /// # Errors
    ///
    /// Returns [`CertificateDirectoryError`] for conflicting mappings or an
    /// exhausted finite bound.
    pub fn map_fingerprint(
        mut self,
        fingerprint: CertificateFingerprint,
        peer_id: PeerId,
    ) -> Result<Self, CertificateDirectoryError> {
        self.insert(fingerprint, peer_id)?;
        Ok(self)
    }

    /// Finishes the immutable directory.
    ///
    /// An empty directory is valid and fails closed by authenticating no
    /// certificate.
    #[must_use]
    pub fn build(self) -> CertificateDirectory {
        CertificateDirectory {
            state: Arc::new(CertificateDirectoryState {
                by_fingerprint: self.by_fingerprint,
                peers: self.peers,
            }),
        }
    }

    fn insert(
        &mut self,
        fingerprint: CertificateFingerprint,
        peer_id: PeerId,
    ) -> Result<(), CertificateDirectoryError> {
        if let Some(existing) = self.by_fingerprint.get(&fingerprint) {
            if existing == &peer_id {
                return Ok(());
            }
            return Err(CertificateDirectoryError::FingerprintConflict {
                fingerprint,
                existing: existing.clone(),
                requested: peer_id,
            });
        }
        if self.by_fingerprint.len() >= self.limits.max_fingerprints() {
            return Err(CertificateDirectoryError::FingerprintLimit {
                maximum: self.limits.max_fingerprints(),
            });
        }

        let new_peer = !self.peers.contains(&peer_id);
        if new_peer && self.peers.len() >= self.limits.max_peers() {
            return Err(CertificateDirectoryError::PeerLimit {
                maximum: self.limits.max_peers(),
            });
        }

        self.by_fingerprint.insert(fingerprint, peer_id.clone());
        self.peers.insert(peer_id);
        Ok(())
    }
}

/// Refusal while constructing an explicit certificate directory.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CertificateDirectoryError {
    /// Empty bytes cannot be a DER leaf certificate.
    EmptyCertificate,
    /// One fingerprint was assigned to two principals.
    FingerprintConflict {
        /// Conflicting fingerprint.
        fingerprint: CertificateFingerprint,
        /// Principal already assigned to it.
        existing: PeerId,
        /// Principal requested by the new mapping.
        requested: PeerId,
    },
    /// The configured fingerprint bound was reached.
    FingerprintLimit {
        /// Maximum configured fingerprints.
        maximum: usize,
    },
    /// The configured distinct-principal bound was reached.
    PeerLimit {
        /// Maximum configured principals.
        maximum: usize,
    },
}

impl fmt::Display for CertificateDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCertificate => formatter.write_str("leaf certificate DER must not be empty"),
            Self::FingerprintConflict {
                fingerprint,
                existing,
                requested,
            } => write!(
                formatter,
                "certificate fingerprint {fingerprint} maps to {existing}, not requested \
                 principal {requested}"
            ),
            Self::FingerprintLimit { maximum } => write!(
                formatter,
                "certificate directory already holds its maximum {maximum} fingerprints"
            ),
            Self::PeerLimit { maximum } => write!(
                formatter,
                "certificate directory already names its maximum {maximum} peers"
            ),
        }
    }
}

impl Error for CertificateDirectoryError {}
