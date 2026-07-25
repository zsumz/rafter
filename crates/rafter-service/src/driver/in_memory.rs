#![allow(clippy::wildcard_imports)]

use super::*;

/// Concrete managed driver over real `rafter-app::RaftGroup`s.
///
/// This driver is intentionally in-memory: it is useful for tests, examples,
/// and embedding a fully managed service in a process that does not need an
/// external transport. Production transports can implement
/// [`DriverCommandSender`] directly or wrap the same group-driving logic around
/// an authenticated network boundary.
pub struct InMemoryRaftDriver<G, A, R> {
    pub(super) inner: Arc<Mutex<InMemoryRaftState<G, A, R>>>,
}

impl<G, A, R> Clone for InMemoryRaftDriver<G, A, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<G, A, R> Debug for InMemoryRaftDriver<G, A, R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InMemoryRaftDriver")
            .finish_non_exhaustive()
    }
}

impl<G, A, R> InMemoryRaftDriver<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    A::Error: Debug + Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    R::Error: Debug + Send + 'static,
{
    /// Builds a driver and drives the primary through election.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] if construction fails or the primary
    /// does not become leader within the configured drive bound.
    pub fn new_elected(
        primary_node_id: NodeId,
        groups: impl IntoIterator<Item = RaftGroup<G, A, R>>,
    ) -> Result<Self, ManagedDriverError> {
        let driver = Self::new(primary_node_id, groups)?;
        driver.elect_primary()?;
        Ok(driver)
    }

    /// Returns a cloneable handle connected to this in-memory driver.
    #[must_use]
    pub fn handle(
        &self,
    ) -> RaftHandle<G, A::Command, A::Query, A::CommandResult, A::QueryResult, Self> {
        let group_id = lock_state(&self.inner).group_id.clone();
        RaftHandle::new(group_id, self.clone())
    }

    /// Proposes a bounded batch of writes through the primary group.
    ///
    /// The driver reserves a contiguous local proposal ID range, submits the
    /// commands through one app-layer proposal batch, and then drives the
    /// in-memory network until every write has either applied or reached a
    /// managed error/unknown-outcome boundary. The returned vector preserves
    /// input order and contains one result per supplied entry.
    #[must_use]
    pub fn write_batch(
        &self,
        group_id: G,
        writes: Vec<WriteBatchEntry<A::Command>>,
    ) -> DriverFuture<Vec<Result<WriteReceipt<A::CommandResult>, WriteError>>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = lock_state(&inner);
            state.write_batch(&group_id, writes)
        })
    }

    /// Drives one tick on the primary node.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the driver has shut down, the group
    /// step fails, or routing does not drain within the configured bound.
    pub fn tick_primary(&self) -> Result<(), ManagedDriverError> {
        let mut state = lock_state(&self.inner);
        state.reject_if_shutting_down()?;
        state.tick_primary()
    }

    /// Shuts the driver down and returns every group it owns.
    ///
    /// The counterpart to [`InMemoryRaftDriver::new`], which takes its groups
    /// by value. This driver resolves every client future inside the call that
    /// created it, so there is never an outstanding waiter to release here;
    /// undelivered frames in the in-memory network are dropped, and the driver
    /// refuses every later operation — there is no re-adoption, because this
    /// driver's constructor builds a whole cluster rather than installing one
    /// node.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError::ShuttingDown`] when the driver has already
    /// shut down or released its groups.
    pub fn release_groups(
        &self,
    ) -> Result<BTreeMap<NodeId, RaftGroup<G, A, R>>, ManagedDriverError> {
        let mut state = lock_state(&self.inner);
        state.reject_if_shutting_down()?;
        state.shutting_down = true;
        state.network.clear();
        state.metrics.close();
        Ok(std::mem::take(&mut state.groups))
    }

    /// Drives ticks on the primary until it is leader.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the primary cannot be elected within
    /// the configured bound.
    pub fn elect_primary(&self) -> Result<(), ManagedDriverError> {
        let mut state = lock_state(&self.inner);
        state.reject_if_shutting_down()?;
        let mut drove = false;
        for _ in 0..state.max_drive_steps {
            if state.primary_role()? == Role::Leader {
                state.publish_primary_metrics();
                return Ok(());
            }
            state.tick_primary()?;
            drove = true;
        }
        if drove {
            state.publish_primary_metrics();
        }
        Err(ManagedDriverError::Stalled {
            max_steps: state.max_drive_steps,
        })
    }
}

impl<G, A, R> DriverCommandSender<G, A::Command, A::Query, A::CommandResult, A::QueryResult>
    for InMemoryRaftDriver<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    A::Error: Debug + Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    R::Error: Debug + Send + 'static,
{
    fn write(
        &self,
        group_id: G,
        command: A::Command,
        options: WriteOptions,
    ) -> DriverFuture<Result<WriteReceipt<A::CommandResult>, WriteError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = lock_state(&inner);
            state.write(&group_id, command, options).map_err(|error| {
                // Nothing reached a group, so the refusal is observed rather
                // than inferred.
                error.into_write_error(WriteFate::NotAppended)
            })
        })
    }

    fn read(
        &self,
        group_id: G,
        query: A::Query,
        consistency: ReadConsistency,
    ) -> DriverFuture<Result<QueryReceipt<G, A::QueryResult>, ReadError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = lock_state(&inner);
            state
                .read(&group_id, &query, consistency)
                .map_err(ManagedOperationError::into_read_error)
        })
    }

    fn transfer_leadership(
        &self,
        group_id: G,
        target: NodeId,
    ) -> DriverFuture<Result<(), TransferLeadershipError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = lock_state(&inner);
            state
                .transfer_leadership(&group_id, target)
                .map_err(ManagedOperationError::into_transfer_error)
        })
    }

    fn metrics(&self, group_id: G) -> Result<MetricsWatch<G>, MetricsError> {
        let state = lock_state(&self.inner);
        if group_id != state.group_id {
            return Err(MetricsError::WrongGroup);
        }
        Ok(state.metrics.watch())
    }

    fn shutdown(&self, group_id: G) -> DriverFuture<Result<(), ShutdownError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = lock_state(&inner);
            if group_id != state.group_id {
                return Err(ShutdownError::WrongGroup);
            }
            if state.shutting_down {
                return Err(ShutdownError::AlreadyShutDown);
            }
            state.shutting_down = true;
            state.metrics.close();
            Ok(())
        })
    }
}
