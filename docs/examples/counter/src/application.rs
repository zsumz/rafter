//! Move committed counter commands through the bounded application worker.
use crate::{counter::Counter, disk};
use rafter::LogIndex;
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ApplyResult, ReplicatedStateMachine};
use rafter_runtime::{
    application::{ApplicationEntry, DurableApplication},
    service::{CommittedCommand, ServiceAdapterError},
};
use std::{io, mem::size_of};

#[derive(Debug)]
pub struct Entry {
    first: LogIndex,
    command: ApplyEntry<u64>,
}

impl TryFrom<CommittedCommand> for Entry {
    type Error = ServiceAdapterError;

    fn try_from(command: CommittedCommand) -> Result<Self, Self::Error> {
        // Decode to a scalar; do not retain an arbitrarily large backing frame.
        let amount = u64::from_be_bytes(
            command
                .payload
                .as_ref()
                .try_into()
                .map_err(|_| disk::invalid("an increment must be eight bytes"))?,
        );
        Ok(Self {
            first: command.first_index,
            command: ApplyEntry {
                index: command.index,
                term: command.term,
                command: amount,
                local_proposal_id: command.local_proposal_id,
            },
        })
    }
}

impl ApplicationEntry for Entry {
    fn first_log_index(&self) -> LogIndex {
        self.first
    }
    fn log_index(&self) -> LogIndex {
        self.command.index
    }
    fn retained_bytes(&self) -> usize {
        // Entry, temporary batch copy, and eventual result are all fixed size.
        size_of::<Self>() + size_of::<ApplyEntry<u64>>() + size_of::<ApplyResult<u64>>()
    }
}

impl DurableApplication<Entry> for Counter {
    type Outcome = ApplyResult<u64>;
    type Error = io::Error;

    fn applied_through(&self) -> LogIndex {
        // This implementation's floor is the atomically saved in-memory state.
        self.applied_index().expect("counter floor is infallible")
    }
    fn apply(&mut self, entries: &[Entry]) -> io::Result<Vec<Self::Outcome>> {
        self.apply_batch(ApplyBatch {
            entries: entries.iter().map(|entry| entry.command.clone()).collect(),
        })
    }
}
