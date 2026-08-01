//! Explicit authenticated leaf-certificate fingerprint mappings.

mod builder;
mod fingerprint;
mod pem;

pub use builder::{CertificateDirectoryBuilder, CertificateDirectoryError};
pub use fingerprint::{CertificateFingerprint, CertificateFingerprintParseError};
pub use pem::{CertificatePemError, MAX_CERTIFICATE_PEM_BYTES};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::{CertificateDirectoryLimits, PeerId};

/// Immutable explicit certificate-fingerprint-to-principal directory.
///
/// TLS chain and server-name verification happen before this lookup. A valid
/// CA-signed certificate is still refused unless its leaf fingerprint appears
/// here. Multiple fingerprints may name one [`PeerId`] to permit credential
/// rotation without changing the transport principal.
#[derive(Clone, Debug)]
pub struct CertificateDirectory {
    pub(super) state: Arc<CertificateDirectoryState>,
}

#[derive(Debug)]
pub(super) struct CertificateDirectoryState {
    pub(super) by_fingerprint: BTreeMap<CertificateFingerprint, PeerId>,
    pub(super) peers: BTreeSet<PeerId>,
}

impl CertificateDirectory {
    /// Starts a builder with default finite limits.
    #[must_use]
    pub fn builder() -> CertificateDirectoryBuilder {
        CertificateDirectoryBuilder::new(CertificateDirectoryLimits::default())
    }

    /// Starts a builder with caller-selected finite limits.
    #[must_use]
    pub fn builder_with_limits(limits: CertificateDirectoryLimits) -> CertificateDirectoryBuilder {
        CertificateDirectoryBuilder::new(limits)
    }

    /// Returns the principal explicitly mapped to `fingerprint`.
    #[must_use]
    pub fn peer_for_fingerprint(&self, fingerprint: &CertificateFingerprint) -> Option<&PeerId> {
        self.state.by_fingerprint.get(fingerprint)
    }

    /// Computes and looks up the exact leaf-certificate fingerprint.
    #[must_use]
    pub fn peer_for_der(&self, certificate_der: &[u8]) -> Option<&PeerId> {
        let fingerprint = CertificateFingerprint::from_der(certificate_der);
        self.peer_for_fingerprint(&fingerprint)
    }

    /// Returns whether at least one configured certificate names `peer_id`.
    #[must_use]
    pub fn contains_peer(&self, peer_id: &PeerId) -> bool {
        self.state.peers.contains(peer_id)
    }

    /// Number of configured leaf-certificate fingerprints.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state.by_fingerprint.len()
    }

    /// Returns whether no certificate fingerprint is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.state.by_fingerprint.is_empty()
    }

    /// Returns configured fingerprints for one principal in canonical order.
    #[must_use]
    pub fn fingerprints_for_peer(&self, peer_id: &PeerId) -> Vec<CertificateFingerprint> {
        self.state
            .by_fingerprint
            .iter()
            .filter_map(|(fingerprint, mapped)| (mapped == peer_id).then_some(*fingerprint))
            .collect()
    }
}
