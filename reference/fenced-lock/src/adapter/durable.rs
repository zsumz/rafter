//! The fenced lock service behind `rafter-app`, over a durable transactional
//! backend.
//!
//! This is the same application as [`crate::LockStateMachine`] with one
//! difference: its state lives in a [`LockStore`] rather than in memory, and it
//! advances only when a transaction is durable. The pure model stays the
//! semantic authority — every lock, session, token, and expiry decision is
//! still [`LockService::apply`] — and every applied-index and snapshot rule
//! comes from [`crate::adapter::discipline`], which the in-memory machine uses
//! too. Nothing here re-derives either kind of decision, and nothing here
//! reads the independent oracle.
//!
//! The machine holds no mirror of the durable state. There is exactly one
//! [`LockService`] value, it lives inside the store, and the store replaces it
//! only after the transaction that carries it returns durable. A machine that
//! kept its own copy would have to explain what that copy meant between a
//! failed commit and a reopen; this one has nothing to explain. It also could
//! not answer the question that matters most here — which fencing high-water
//! marks are durable — without the reader knowing which copy to trust.
//!
//! # The transaction
//!
//! [`ReplicatedStateMachine::apply_batch`] applies a batch into a clone of the
//! durable service, then commits the resulting state and the batch's final
//! index as one transaction, then returns the results. Lock table mutations,
//! every fencing high-water mark, the session and deduplication mutation, the
//! cached command result, the replicated logical time, and the applied Raft
//! index are all in that one commit, which is what the contract requires.
//!
//! Returning after the commit is deliberate and is itself a crash window: a
//! caller's reply is released by this method returning, so a crash between the
//! commit point and the return leaves a command that is durably applied and
//! never acknowledged. For an acquisition that means a fencing token exists
//! that no client has heard. The contract's answer is the deduplication cache —
//! the client retries the same request identity and the cached result hands it
//! the same token — and `durable_crash.rs` proves that answer rather than
//! assuming it.
//!
//! A batch commits once rather than per entry. Every command in the batch moves
//! with the applied index either way, so the atomicity the contract names is
//! preserved, and a batch of one — which is what the deterministic driver
//! produces — is the per-entry case.

use std::{error::Error, fmt};

use rafter::LogIndex;
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyResult, ReadBarrier,
    ReplicatedStateMachine, SnapshotSupport,
};

use crate::{
    adapter::{codec, discipline},
    store::{LockStore, LockStoreError},
    ApplyOutcome, Command, LockAdapterError, LockConfig, LockQuery, LockQueryResult, LockService,
};

/// Failure of the durable lock state machine.
///
/// This enum is exhaustive, and its two variants are the two things that can go
/// wrong at this boundary: the application contract was violated, or the
/// durable backend could not complete a publication. Keeping them apart matters
/// because only the second one leaves the outcome of a transaction unknown.
#[derive(Debug)]
pub enum DurableLockError {
    /// The application contract was violated.
    ///
    /// This is the same taxonomy the in-memory adapter reports, because it is
    /// produced by the same rules.
    Adapter(LockAdapterError),
    /// The durable store could not complete an operation.
    Store(LockStoreError),
}

impl fmt::Display for DurableLockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => write!(formatter, "lock adapter failed: {error}"),
            Self::Store(error) => write!(formatter, "durable lock store failed: {error}"),
        }
    }
}

impl Error for DurableLockError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Adapter(error) => Some(error),
            Self::Store(error) => Some(error),
        }
    }
}

impl From<LockAdapterError> for DurableLockError {
    fn from(error: LockAdapterError) -> Self {
        Self::Adapter(error)
    }
}

impl From<LockStoreError> for DurableLockError {
    fn from(error: LockStoreError) -> Self {
        Self::Store(error)
    }
}

/// The fenced lock service over a durable transactional application backend.
#[derive(Debug)]
pub struct DurableLockStateMachine {
    store: LockStore,
}

impl DurableLockStateMachine {
    /// Wraps an opened store.
    ///
    /// The store has already recovered, so the machine starts at whatever
    /// applied index and whatever fencing high-water marks the last committed
    /// transaction left.
    #[must_use]
    pub const fn new(store: LockStore) -> Self {
        Self { store }
    }

    /// Returns the configured resource bounds.
    #[must_use]
    pub const fn config(&self) -> LockConfig {
        self.store.config()
    }

    /// Returns the durable lock service for inspection and independent
    /// comparison.
    #[must_use]
    pub const fn service(&self) -> &LockService {
        self.store.service()
    }

    /// Returns the durable store.
    #[must_use]
    pub const fn store(&self) -> &LockStore {
        &self.store
    }
}

impl ReplicatedStateMachine for DurableLockStateMachine {
    type Command = Command;
    type CommandResult = ApplyOutcome;
    type Query = LockQuery;
    type QueryResult = LockQueryResult;
    type Error = DurableLockError;

    /// Declared `Supported`: both methods below carry the whole
    /// contract-enumerated state — the lock table, every fencing high-water
    /// mark, sessions with their cached operation, fingerprint, and result, the
    /// replicated logical time, and the applied Raft index — through the
    /// adapter's own snapshot frame, and `durable_crash.rs` proves the round
    /// trip against the durable backend.
    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.store.applied_index())
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        Ok(codec::encode_command(command))
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        Ok(codec::decode_command(payload).map_err(LockAdapterError::from)?)
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        // The batch runs against a clone so that a failed commit leaves the
        // machine reporting exactly what is durable. The clone is adopted by
        // the store, and only by the store, and only after the commit point.
        let mut service = self.store.service().clone();
        let mut applied_index = self.store.applied_index();
        let mut results = Vec::with_capacity(batch.entries.len());

        for entry in batch.entries {
            discipline::admit_entry(entry.index, applied_index)?;
            let result = service.apply(entry.command);
            applied_index = entry.index;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result,
                local_proposal_id: entry.local_proposal_id,
            });
        }

        if results.is_empty() {
            return Ok(results);
        }
        // Every byte of the transaction is durable before the results leave
        // this method, so a caller's reply — and any fencing token inside it —
        // can only follow the commit point.
        self.store.commit(&service, applied_index)?;
        Ok(results)
    }

    fn read(
        &self,
        query: Self::Query,
        barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        discipline::admit_read(barrier, self.store.applied_index())?;
        Ok(match query {
            // Querying an unknown name must not track it, which is the pure
            // model's rule and is why this arm consults `status` rather than
            // anything that could insert.
            LockQuery::GetLock { resource } => {
                LockQueryResult::Lock(self.store.service().status(resource))
            }
        })
    }

    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
        discipline::admit_snapshot_request(at, self.store.applied_index())
            .map_err(DurableLockError::Adapter)?;
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: codec::encode_snapshot(at.0, &self.store.service().snapshot())
                .map_err(LockAdapterError::from)
                .map_err(DurableLockError::Adapter)?,
            raft_snapshot: None,
        })
    }

    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        let service_snapshot = discipline::admit_install(&snapshot, self.store.applied_index())
            .map_err(DurableLockError::Adapter)?;
        let service = LockService::from_snapshot(self.store.config(), service_snapshot)
            .map_err(LockAdapterError::Snapshot)
            .map_err(DurableLockError::Adapter)?;

        // An install replaces state rather than extending it, but it is the
        // same publication as an apply: one image into the stale slot, one
        // durability barrier, one adoption. The applied floor moves with the
        // data, and the store refuses any install that would lower a fencing
        // high-water mark before it writes a byte.
        self.store
            .install(&service, snapshot.applied_index)
            .map_err(DurableLockError::Store)?;
        Ok(())
    }
}
