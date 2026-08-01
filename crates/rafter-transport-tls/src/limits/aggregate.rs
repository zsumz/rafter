//! Aggregate transport limits.

use super::{
    CertificateDirectoryLimits, DirectoryLimits, EndpointBookLimits, RuntimeLimits,
    SessionStoreLimits, WireLimits,
};

/// Aggregate finite limits used by one transport instance.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransportLimits {
    directory: DirectoryLimits,
    endpoints: EndpointBookLimits,
    certificates: CertificateDirectoryLimits,
    sessions: SessionStoreLimits,
    runtime: RuntimeLimits,
    wire: WireLimits,
}

impl TransportLimits {
    /// Creates an aggregate from independently validated bounds.
    #[must_use]
    pub const fn new(
        directory: DirectoryLimits,
        endpoints: EndpointBookLimits,
        certificates: CertificateDirectoryLimits,
        wire: WireLimits,
    ) -> Self {
        Self {
            directory,
            endpoints,
            certificates,
            sessions: SessionStoreLimits::DEFAULT,
            runtime: RuntimeLimits::DEFAULT,
            wire,
        }
    }

    /// Per-group directory limits.
    #[must_use]
    pub const fn directory(self) -> DirectoryLimits {
        self.directory
    }

    /// Endpoint-book limits.
    #[must_use]
    pub const fn endpoints(self) -> EndpointBookLimits {
        self.endpoints
    }

    /// Certificate-directory limits.
    #[must_use]
    pub const fn certificates(self) -> CertificateDirectoryLimits {
        self.certificates
    }

    /// Replaces the durable session-store bound.
    #[must_use]
    pub const fn with_sessions(mut self, sessions: SessionStoreLimits) -> Self {
        self.sessions = sessions;
        self
    }

    /// Durable physical-peer session-store limits.
    #[must_use]
    pub const fn sessions(self) -> SessionStoreLimits {
        self.sessions
    }

    /// Replaces the blocking runtime queue and connection bounds.
    #[must_use]
    pub const fn with_runtime(mut self, runtime: RuntimeLimits) -> Self {
        self.runtime = runtime;
        self
    }

    /// Blocking runtime queue and connection bounds.
    #[must_use]
    pub const fn runtime(self) -> RuntimeLimits {
        self.runtime
    }

    /// Handshake and frame limits.
    #[must_use]
    pub const fn wire(self) -> WireLimits {
        self.wire
    }
}
