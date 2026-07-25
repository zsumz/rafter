//! The ledger behind `rafter-app`, over a durable transactional backend.
//!
//! This is the same application as [`crate::LedgerStateMachine`] with one
//! difference: its state lives in a [`LedgerStore`] rather than in memory, and
//! it advances only when a transaction is durable. The pure model stays the
//! semantic authority — every ledger, session, and deduplication decision is
//! still [`Ledger::apply`] — and every applied-index and snapshot rule comes
//! from [`crate::adapter::discipline`], which the in-memory machine uses too.
//! Nothing here re-derives either kind of decision.
//!
//! The machine holds no mirror of the durable state. There is exactly one
//! ledger value, it lives inside the store, and the store replaces it only
//! after the transaction that carries it returns durable. A machine that kept
//! its own copy would have to explain what that copy meant between a failed
//! commit and a reopen; this one has nothing to explain.
//!
//! # The transaction
//!
//! [`ReplicatedStateMachine::apply_batch`] applies a batch into a clone of the
//! durable ledger, then commits the resulting state and the batch's final index
//! as one transaction, then returns the results. Account mutations, the session
//! and deduplication mutation, the cached command results, and the applied Raft
//! index are all in that one commit, which is what the contract requires.
//!
//! Returning after the commit is deliberate and is itself a crash window: a
//! caller's reply is released by this method returning, so a crash between the
//! commit point and the return leaves a command that is durably applied and
//! never acknowledged. The contract's answer is the deduplication cache — the
//! client retries the same request identity and the cached result answers it —
//! and `durable_crash.rs` proves that answer rather than assuming it.
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
    store::{LedgerStore, LedgerStoreError},
    ApplyOutcome, Command, Ledger, LedgerAdapterError, LedgerConfig, LedgerQuery,
    LedgerQueryResult,
};

/// Failure of the durable ledger state machine.
///
/// This enum is exhaustive, and its two variants are the two things that can go
/// wrong at this boundary: the application contract was violated, or the
/// durable backend could not complete a transaction. Keeping them apart matters
/// because only the second one leaves the outcome of a transaction unknown.
#[derive(Debug)]
pub enum DurableLedgerError {
    /// The application contract was violated.
    ///
    /// This is the same taxonomy the in-memory adapter reports, because it is
    /// produced by the same rules.
    Adapter(LedgerAdapterError),
    /// The durable store could not complete an operation.
    Store(LedgerStoreError),
}

impl fmt::Display for DurableLedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => write!(formatter, "ledger adapter failed: {error}"),
            Self::Store(error) => write!(formatter, "durable ledger store failed: {error}"),
        }
    }
}

impl Error for DurableLedgerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Adapter(error) => Some(error),
            Self::Store(error) => Some(error),
        }
    }
}

impl From<LedgerAdapterError> for DurableLedgerError {
    fn from(error: LedgerAdapterError) -> Self {
        Self::Adapter(error)
    }
}

impl From<LedgerStoreError> for DurableLedgerError {
    fn from(error: LedgerStoreError) -> Self {
        Self::Store(error)
    }
}

/// The ledger over a durable transactional application backend.
#[derive(Debug)]
pub struct DurableLedgerStateMachine {
    store: LedgerStore,
}

impl DurableLedgerStateMachine {
    /// Wraps an opened store.
    ///
    /// The store has already recovered, so the machine starts at whatever
    /// applied index the last committed transaction left.
    #[must_use]
    pub const fn new(store: LedgerStore) -> Self {
        Self { store }
    }

    /// Returns the configured resource bounds.
    #[must_use]
    pub const fn config(&self) -> LedgerConfig {
        self.store.config()
    }

    /// Returns the durable ledger for inspection and independent comparison.
    #[must_use]
    pub const fn ledger(&self) -> &Ledger {
        self.store.ledger()
    }

    /// Returns the durable store.
    #[must_use]
    pub const fn store(&self) -> &LedgerStore {
        &self.store
    }

    /// Rewrites the journal down to the current state in one transaction.
    ///
    /// This is the application's compaction point, and it pairs with the Raft
    /// log compaction a caller drives from [`Self::build_snapshot`]: both
    /// discard a prefix that the current state already accounts for. It is kept
    /// separate from `build_snapshot` so that building a snapshot stays a read
    /// of application state rather than a publication that can fail.
    ///
    /// # Errors
    ///
    /// Returns an error when the rewrite cannot be staged, published, or made
    /// durable.
    pub fn compact(&mut self) -> Result<(), DurableLedgerError> {
        self.store.compact().map_err(DurableLedgerError::Store)
    }
}

impl ReplicatedStateMachine for DurableLedgerStateMachine {
    type Command = Command;
    type CommandResult = ApplyOutcome;
    type Query = LedgerQuery;
    type QueryResult = LedgerQueryResult;
    type Error = DurableLedgerError;

    /// Declared `Supported`: both methods below carry the whole
    /// contract-enumerated state through the adapter's own frames, and
    /// `durable_crash.rs` proves the round trip against the durable backend.
    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.store.applied_index())
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        Ok(codec::encode_command(command))
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        Ok(codec::decode_command(payload).map_err(LedgerAdapterError::from)?)
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        // The batch runs against a clone so that a failed commit leaves the
        // machine reporting exactly what is durable. The clone is adopted by
        // the store, and only by the store, and only after the commit point.
        let mut ledger = self.store.ledger().clone();
        let mut applied_index = self.store.applied_index();
        let mut results = Vec::with_capacity(batch.entries.len());

        for entry in batch.entries {
            discipline::admit_entry(entry.index, applied_index)?;
            let result = ledger.apply(entry.command);
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
        // this method, so a caller's reply can only follow the commit point.
        self.store.commit(&ledger, applied_index)?;
        Ok(results)
    }

    fn read(
        &self,
        query: Self::Query,
        barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        discipline::admit_read(barrier, self.store.applied_index())?;
        Ok(self.store.ledger().query(query))
    }

    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
        discipline::admit_snapshot_request(at, self.store.applied_index())
            .map_err(DurableLedgerError::Adapter)?;
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: codec::encode_snapshot(at.0, &self.store.ledger().snapshot())
                .map_err(LedgerAdapterError::from)
                .map_err(DurableLedgerError::Adapter)?,
            raft_snapshot: None,
        })
    }

    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        let ledger_snapshot = discipline::admit_install(&snapshot, self.store.applied_index())
            .map_err(DurableLedgerError::Adapter)?;
        let ledger = Ledger::from_snapshot(self.store.config(), ledger_snapshot)
            .map_err(LedgerAdapterError::Snapshot)
            .map_err(DurableLedgerError::Adapter)?;

        // An install replaces state rather than extending it, so it is
        // published by rewriting the journal. The applied floor moves with the
        // data in that one publication, exactly as it does for an apply.
        self.store
            .replace(&ledger, snapshot.applied_index)
            .map_err(DurableLedgerError::Store)?;
        Ok(())
    }
}
