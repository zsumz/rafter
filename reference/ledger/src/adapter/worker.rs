//! External-consumer composition with the bounded application worker.

use std::mem;

use rafter::LogIndex;
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ApplyResult, ReplicatedStateMachine};
use rafter_runtime::application::{ApplicationEntry, DurableApplication};

use super::{DurableLedgerError, DurableLedgerStateMachine};
use crate::{adapter::codec, ApplyOutcome, Command};

/// One committed ledger command owned by the application worker.
///
/// The wrapper keeps Raft's entry metadata beside the command while the
/// worker transfers it between the service owner and the durable application
/// thread. Consuming a completion returns this exact value alongside the
/// corresponding [`ApplyResult`], so a service can release its own client
/// correlation only after the application transaction is durable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerApplicationEntry {
    entry: ApplyEntry<Command>,
}

impl LedgerApplicationEntry {
    /// Wraps one decoded committed application entry.
    #[must_use]
    pub const fn new(entry: ApplyEntry<Command>) -> Self {
        Self { entry }
    }

    /// Returns the wrapped application entry.
    #[must_use]
    pub const fn as_apply_entry(&self) -> &ApplyEntry<Command> {
        &self.entry
    }

    /// Returns the owned application entry.
    #[must_use]
    pub fn into_apply_entry(self) -> ApplyEntry<Command> {
        self.entry
    }
}

impl From<ApplyEntry<Command>> for LedgerApplicationEntry {
    fn from(entry: ApplyEntry<Command>) -> Self {
        Self::new(entry)
    }
}

impl ApplicationEntry for LedgerApplicationEntry {
    fn log_index(&self) -> LogIndex {
        self.entry.index
    }

    fn retained_bytes(&self) -> usize {
        // The current command and result vocabulary is allocation-free. The
        // encoded command length is still included as a conservative cushion
        // and keeps this estimate useful if the wire shape gains padding.
        mem::size_of::<Self>()
            .saturating_add(codec::encode_command(&self.entry.command).len())
            .saturating_add(mem::size_of::<ApplyResult<ApplyOutcome>>())
    }

    fn batch_bytes(&self) -> usize {
        mem::size_of::<ApplyEntry<Command>>()
            .saturating_add(codec::encode_command(&self.entry.command).len())
    }
}

impl DurableApplication<LedgerApplicationEntry> for DurableLedgerStateMachine {
    type Outcome = ApplyResult<ApplyOutcome>;
    type Error = DurableLedgerError;

    fn applied_through(&self) -> LogIndex {
        self.store().applied_index()
    }

    fn apply(
        &mut self,
        entries: &[LedgerApplicationEntry],
    ) -> Result<Vec<Self::Outcome>, Self::Error> {
        self.apply_batch(ApplyBatch {
            entries: entries.iter().map(|entry| entry.entry.clone()).collect(),
        })
    }
}
