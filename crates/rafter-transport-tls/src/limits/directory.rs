//! Bounds for retained identity and endpoint directories.

use super::{require_nonzero, LimitError, LimitKind};

/// Bounds for the per-group authenticated peer directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectoryLimits {
    max_groups: usize,
    max_bindings_per_group: usize,
}

impl DirectoryLimits {
    /// Validates directory bounds.
    ///
    /// # Errors
    ///
    /// Returns [`LimitError`] when either bound is zero.
    pub fn new(max_groups: usize, max_bindings_per_group: usize) -> Result<Self, LimitError> {
        require_nonzero(LimitKind::Groups, max_groups)?;
        require_nonzero(LimitKind::BindingsPerGroup, max_bindings_per_group)?;
        Ok(Self {
            max_groups,
            max_bindings_per_group,
        })
    }

    /// Maximum number of known groups.
    #[must_use]
    pub const fn max_groups(self) -> usize {
        self.max_groups
    }

    /// Maximum principal/node bindings retained for one group.
    #[must_use]
    pub const fn max_bindings_per_group(self) -> usize {
        self.max_bindings_per_group
    }
}

impl Default for DirectoryLimits {
    fn default() -> Self {
        Self {
            max_groups: 4_096,
            max_bindings_per_group: 256,
        }
    }
}

/// Bounds for caller-managed resolved peer endpoints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointBookLimits {
    max_peers: usize,
    max_endpoints_per_peer: usize,
}

impl EndpointBookLimits {
    /// Validates endpoint-book bounds.
    ///
    /// # Errors
    ///
    /// Returns [`LimitError`] when either bound is zero.
    pub fn new(max_peers: usize, max_endpoints_per_peer: usize) -> Result<Self, LimitError> {
        require_nonzero(LimitKind::EndpointPeers, max_peers)?;
        require_nonzero(LimitKind::EndpointsPerPeer, max_endpoints_per_peer)?;
        Ok(Self {
            max_peers,
            max_endpoints_per_peer,
        })
    }

    /// Maximum number of peers with endpoint entries.
    #[must_use]
    pub const fn max_peers(self) -> usize {
        self.max_peers
    }

    /// Maximum resolved endpoints retained for one peer.
    #[must_use]
    pub const fn max_endpoints_per_peer(self) -> usize {
        self.max_endpoints_per_peer
    }
}

impl Default for EndpointBookLimits {
    fn default() -> Self {
        Self {
            max_peers: 128,
            max_endpoints_per_peer: 8,
        }
    }
}

/// Bounds for explicit leaf-certificate fingerprint mappings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CertificateDirectoryLimits {
    max_fingerprints: usize,
    max_peers: usize,
}

impl CertificateDirectoryLimits {
    /// Validates certificate-directory bounds.
    ///
    /// # Errors
    ///
    /// Returns [`LimitError`] when either bound is zero.
    pub fn new(max_fingerprints: usize, max_peers: usize) -> Result<Self, LimitError> {
        require_nonzero(LimitKind::CertificateFingerprints, max_fingerprints)?;
        require_nonzero(LimitKind::CertificatePeers, max_peers)?;
        Ok(Self {
            max_fingerprints,
            max_peers,
        })
    }

    /// Maximum number of configured leaf-certificate fingerprints.
    #[must_use]
    pub const fn max_fingerprints(self) -> usize {
        self.max_fingerprints
    }

    /// Maximum number of distinct peers named by fingerprints.
    #[must_use]
    pub const fn max_peers(self) -> usize {
        self.max_peers
    }
}

impl Default for CertificateDirectoryLimits {
    fn default() -> Self {
        Self {
            max_fingerprints: 512,
            max_peers: 128,
        }
    }
}
