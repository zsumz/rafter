//! Explicit dependency builder for the owned blocking transport runtime.

mod state;
mod validated;

use std::fmt;

use crate::runtime::SessionStoreHandle;
use crate::snapshot::SnapshotResolverHandle;
use crate::{
    CertificateDirectory, EndpointBook, GroupIdCodec, SnapshotChunkResolver, TlsIdentity,
    TlsPeerDirectory, TlsPeerTransport, TlsTransportBuildError, TransportConfig,
    TransportSessionStore,
};

/// Builder requiring explicit TLS, certificate, directory, endpoint, and
/// durable-session dependencies before binding.
pub struct TlsPeerTransportBuilder<G, C> {
    pub(crate) config: TransportConfig,
    pub(crate) group_codec: C,
    pub(crate) identity: Option<TlsIdentity>,
    pub(crate) certificates: Option<CertificateDirectory>,
    pub(crate) directory: Option<TlsPeerDirectory<G>>,
    pub(crate) endpoints: Option<EndpointBook>,
    pub(crate) sessions: Option<SessionStoreHandle>,
    pub(crate) snapshot_resolver: Option<SnapshotResolverHandle<G>>,
}

impl<G, C> fmt::Debug for TlsPeerTransportBuilder<G, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsPeerTransportBuilder")
            .field("config", &self.config)
            .field("identity", &self.identity.is_some())
            .field("certificates", &self.certificates.is_some())
            .field("directory", &self.directory.is_some())
            .field("endpoints", &self.endpoints.is_some())
            .field("sessions", &self.sessions.is_some())
            .field("snapshot_resolver", &self.snapshot_resolver.is_some())
            .finish_non_exhaustive()
    }
}

impl<G, C> TlsPeerTransportBuilder<G, C> {
    /// Creates a builder with no implicit security, routing, discovery, or
    /// durable-state dependencies.
    #[must_use]
    pub fn new(config: TransportConfig, group_codec: C) -> Self {
        Self {
            config,
            group_codec,
            identity: None,
            certificates: None,
            directory: None,
            endpoints: None,
            sessions: None,
            snapshot_resolver: None,
        }
    }

    /// Installs the strict local mutual-TLS identity.
    #[must_use]
    pub fn identity(mut self, identity: TlsIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Installs the explicit validated-leaf-to-principal directory.
    #[must_use]
    pub fn certificates(mut self, certificates: CertificateDirectory) -> Self {
        self.certificates = Some(certificates);
        self
    }

    /// Installs group-specific principal/node bindings and authorization.
    #[must_use]
    pub fn directory(mut self, directory: TlsPeerDirectory<G>) -> Self {
        self.directory = Some(directory);
        self
    }

    /// Installs caller-managed resolved endpoint sets.
    #[must_use]
    pub fn endpoints(mut self, endpoints: EndpointBook) -> Self {
        self.endpoints = Some(endpoints);
        self
    }

    /// Installs the durable connection-session high-water store.
    #[must_use]
    pub fn session_store<S>(mut self, sessions: S) -> Self
    where
        S: TransportSessionStore,
    {
        self.sessions = Some(SessionStoreHandle::new(sessions));
        self
    }

    /// Installs the caller-owned snapshot payload resolver.
    ///
    /// Snapshot directives are admitted as bounded metadata only. A persistent
    /// sender worker calls this resolver later, outside the managed driver lock,
    /// before assigning the frame's live connection sequence.
    #[must_use]
    pub fn snapshot_resolver<R>(mut self, resolver: R) -> Self
    where
        R: SnapshotChunkResolver<G>,
    {
        self.snapshot_resolver = Some(SnapshotResolverHandle::new(resolver));
        self
    }
}

impl<G, C> TlsPeerTransportBuilder<G, C>
where
    G: Ord + Send + Sync + 'static,
    C: GroupIdCodec<G>,
{
    /// Validates every dependency, binds the listener, and starts bounded
    /// workers in a paused state.
    ///
    /// Paused workers own their finite resources but perform no dial, accept,
    /// TLS handshake, or durable session mutation until
    /// [`TlsPeerTransport::start`] succeeds. The sender and directory handles
    /// remain usable for bounded recovery-output admission and atomic policy
    /// publication while paused.
    ///
    /// # Errors
    ///
    /// Returns [`TlsTransportBuildError`] without returning a partially owned
    /// transport. Any workers started before a later spawn failure are shut down
    /// and joined before the error is returned.
    pub fn bind_paused(self) -> Result<TlsPeerTransport<G, C>, TlsTransportBuildError> {
        state::bind(self)
    }

    /// Validates, binds, activates, and returns the owned runtime.
    ///
    /// This is equivalent to [`TlsPeerTransportBuilder::bind_paused`] followed
    /// immediately by [`TlsPeerTransport::start`]. Callers that must establish
    /// application ownership or recovery readiness before network work begins
    /// use the two-step form.
    ///
    /// # Errors
    ///
    /// Returns [`TlsTransportBuildError`] for construction or activation
    /// failure. A runtime that cannot activate is shut down and joined before
    /// the error is returned.
    pub fn bind(self) -> Result<TlsPeerTransport<G, C>, TlsTransportBuildError> {
        let runtime = self.bind_paused()?;
        if let Err(source) = runtime.start() {
            let _ = runtime.join();
            return Err(TlsTransportBuildError::Start { source });
        }
        Ok(runtime)
    }
}
