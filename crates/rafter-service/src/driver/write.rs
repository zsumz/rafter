//! The managed single write, and where the batch behind it lives.
//!
//! One write is a one-entry batch, so this file is the adapter and
//! [`super::batch`] is the loop. [`super::batch_outcome`] reads a step's report
//! against the entries, and [`super::proposal_outcome`] says what one proposal's
//! outcome means to a client; both are re-exported or re-imported here so the
//! scenarios below read against the names they were written for.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use super::*;

pub(super) use super::proposal_outcome::{
    managed_unknown_reason_from_app, write_error_from_rejection,
};

#[cfg(test)]
use super::{
    batch::BatchWriteState, batch_outcome::observe_batch_report,
    proposal_outcome::observe_write_report,
};

impl<G, A, R> InMemoryRaftState<G, A, R>
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
    pub(super) fn write(
        &mut self,
        group_id: &G,
        command: A::Command,
        options: WriteOptions,
    ) -> ManagedWriteResult<A, R> {
        let mut outcomes = self.write_batch(
            group_id,
            vec![WriteBatchEntry::with_options(command, options)],
        );
        match outcomes.pop() {
            Some(Ok(receipt)) => Ok(receipt),
            Some(Err(error)) => Err(ManagedOperationError::Write(error)),
            None => Err(ManagedOperationError::Write(
                WriteError::ManagedInvariantViolation {
                    fate: WriteFate::Unresolved,
                    message: "managed single write produced no batch outcome".to_owned(),
                },
            )),
        }
    }
}

#[cfg(test)]
#[path = "write_test.rs"]
mod tests;
