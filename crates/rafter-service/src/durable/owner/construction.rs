//! Fresh recovery assigns exclusive ownership of all managed local IDs.
use super::{
    ApplicationEntry, Arc, CommittedCommand, DurableApplication, DurableClientOwner,
    DurableRaftNodeStorage, DurableService, DurableWriteLimits, NodeConfig, RaftHardStateStore,
    RaftLogSegment, RaftSnapshotStore, ReadShared, Reads, ServiceAdapterError, ServiceError,
    ServiceOptions, Shared, SnapshotChunkSource,
};
use crate::durable::DurableClientOptions;

impl<H, L, S, T, A> DurableClientOwner<H, L, S, T, A>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Recovers a fresh owner and installs the same notification for clients
    /// and workers. The callback must be bounded, nonblocking, non-panicking,
    /// and must not reenter this owner. Application adaptation retains the
    /// underlying service's range, semantics, and memory-estimate obligations.
    /// # Errors
    /// Consumes the stores and returns the underlying recovery/start error.
    pub fn recover(
        config: NodeConfig,
        storage: DurableRaftNodeStorage<H, L, S>,
        application: A,
        options: ServiceOptions,
        limits: DurableWriteLimits,
        adapter: impl FnMut(CommittedCommand) -> Result<T, ServiceAdapterError> + Send + 'static,
        wake: impl Fn() + Send + Sync + 'static,
    ) -> Result<Self, ServiceError> {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(wake);
        let shared = Shared::new(limits, options.max_proposal_bytes, Arc::clone(&wake));
        let service = DurableService::recover_with_wake(
            config,
            storage,
            application,
            options,
            adapter,
            move || wake(),
        )?;
        Ok(Self {
            service: Some(service),
            shared,
            options,
            failed: false,
            reads: None,
            prefer_reads: true,
        })
    }
}

impl<H, L, S, T, A, R: Send + 'static> DurableClientOwner<H, L, S, T, A, R>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Recovers an owner with independently bounded encoded writes and reads.
    ///
    /// The query adapter runs on the owner thread under an immutable verified
    /// application borrow. It must be bounded, read-only (including interior
    /// state), nonreentrant, and return an owned result. A panic fences this
    /// owner even when caught. A returned query error value is an ordinary
    /// application result. Encoding requests never borrows the worker.
    /// The wake callback must be bounded, nonblocking, and non-panicking.
    /// # Errors
    /// Consumes the stores and returns the underlying recovery/start error.
    pub fn recover_with_reads(
        config: NodeConfig,
        storage: DurableRaftNodeStorage<H, L, S>,
        application: A,
        options: DurableClientOptions,
        adapter: impl FnMut(CommittedCommand) -> Result<T, ServiceAdapterError> + Send + 'static,
        query: impl Fn(&A, &[u8]) -> R + Send + 'static,
        wake: impl Fn() + Send + Sync + 'static,
    ) -> Result<Self, ServiceError> {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(wake);
        let shared = Shared::new(
            options.writes,
            options.service.max_proposal_bytes,
            Arc::clone(&wake),
        );
        let reads = Reads {
            shared: ReadShared::new(options.reads, Arc::clone(&wake)),
            query: Box::new(query),
        };
        let service = DurableService::recover_with_wake(
            config,
            storage,
            application,
            options.service,
            adapter,
            move || wake(),
        )?;
        Ok(Self {
            service: Some(service),
            shared,
            options: options.service,
            failed: false,
            reads: Some(reads),
            prefer_reads: true,
        })
    }
}
