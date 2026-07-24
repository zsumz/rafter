//! Public Rafter integration for the pure fenced lock service.
//!
//! The adapter owns encoding, the applied-index boundary, the query surface,
//! and the typed client surface over `rafter-service`. Every lock transition,
//! session decision, token issuance, and expiry rule stays in
//! [`crate::LockService`]; nothing here re-derives them, and nothing here reads
//! the independent oracle.

mod client;
mod codec;

use std::{error::Error, fmt};

use rafter::LogIndex;
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine,
};

use crate::{ApplyOutcome, Command, LockConfig, LockService, ResourceName, ResourceStatus};

pub use client::{unknown_outcome_reason, LockClient, LockHandle, QueryOutcome, SubmitOutcome};
pub use codec::{
    decode_command, decode_result, encode_command, encode_result, LockCodecError, NonZeroField,
};

/// Query accepted by the lock service read path.
///
/// The contract defines exactly one query. Every one of them is served behind
/// an ordinary linearizable read barrier: this application makes no lease-read
/// claim, and [`LockClient`] is the only path the tests use precisely so that
/// no caller can pick a weaker consistency by accident.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockQuery {
    /// Reports the holder, expiry, high-water mark, and logical time for a name.
    GetLock { resource: ResourceName },
}

/// Result of one lock service query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockQueryResult {
    /// Status of the queried resource name.
    Lock(ResourceStatus),
}

impl LockQueryResult {
    /// Returns the resource status this query answered.
    #[must_use]
    pub const fn status(self) -> ResourceStatus {
        match self {
            Self::Lock(status) => status,
        }
    }
}

/// Failure of an adapter operation, as distinct from a lock service result.
///
/// Contract-defined session, sequence, and lock rejections are ordinary
/// [`ApplyOutcome`] values, not errors. Every variant here means the adapter was
/// asked for something the application contract cannot express, and the group
/// layer is expected to treat it as fatal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockAdapterError {
    /// A replicated command frame was malformed.
    Codec(LockCodecError),
    /// A committed entry arrived at or below the durable applied floor.
    ///
    /// Re-applying it would execute an acknowledged command twice, which would
    /// reissue a fencing token a guarded resource has already accepted.
    AppliedIndexRegression {
        entry_index: LogIndex,
        applied_index: LogIndex,
    },
    /// A read barrier required freshness this replica has not applied.
    ReadBarrierUnsatisfied {
        required_applied_index: LogIndex,
        applied_index: LogIndex,
    },
    /// A durable application snapshot was requested before this slice defines
    /// its byte representation.
    ///
    /// `CONTRACT.md` states that the durable adapter "will later define a
    /// versioned byte representation" covering the lock table, every
    /// high-water mark, sessions, cached results, and replicated logical time.
    /// Until that slice lands, the pure model's snapshot stays opaque and this
    /// adapter refuses rather than shipping a format that a later slice would
    /// have to break. A driver that never compacts never reaches this path.
    DurableSnapshotUndefined {
        snapshot_index: LogIndex,
        applied_index: LogIndex,
    },
}

impl fmt::Display for LockAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(formatter, "malformed lock frame: {error}"),
            Self::AppliedIndexRegression {
                entry_index,
                applied_index,
            } => write!(
                formatter,
                "committed entry {entry_index} is at or below applied index {applied_index}"
            ),
            Self::ReadBarrierUnsatisfied {
                required_applied_index,
                applied_index,
            } => write!(
                formatter,
                "read barrier requires applied index {required_applied_index}, but this replica applied {applied_index}"
            ),
            Self::DurableSnapshotUndefined {
                snapshot_index,
                applied_index,
            } => write!(
                formatter,
                "durable lock snapshots are undefined until the durable slice; refused at index {snapshot_index} with applied index {applied_index}"
            ),
        }
    }
}

impl Error for LockAdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::AppliedIndexRegression { .. }
            | Self::ReadBarrierUnsatisfied { .. }
            | Self::DurableSnapshotUndefined { .. } => None,
        }
    }
}

impl From<LockCodecError> for LockAdapterError {
    fn from(error: LockCodecError) -> Self {
        Self::Codec(error)
    }
}

/// The fenced lock service behind Rafter's public replicated state-machine
/// contract.
///
/// The adapter holds the durable application boundary: the lock service's state
/// and the Raft index whose effects that state already reflects. Both move
/// together in [`ReplicatedStateMachine::apply_batch`], so no acknowledged
/// command can execute twice and no fencing token can be reissued.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockStateMachine {
    config: LockConfig,
    service: LockService,
    applied_index: LogIndex,
}

impl LockStateMachine {
    /// Creates an empty state machine with no applied entries.
    #[must_use]
    pub fn new(config: LockConfig) -> Self {
        Self {
            config,
            service: LockService::new(config),
            applied_index: LogIndex::ZERO,
        }
    }

    /// Returns the configured resource bounds.
    #[must_use]
    pub const fn config(&self) -> LockConfig {
        self.config
    }

    /// Returns the lock service for inspection and independent comparison.
    #[must_use]
    pub const fn service(&self) -> &LockService {
        &self.service
    }
}

impl ReplicatedStateMachine for LockStateMachine {
    type Command = Command;
    type CommandResult = ApplyOutcome;
    type Query = LockQuery;
    type QueryResult = LockQueryResult;
    type Error = LockAdapterError;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        Ok(codec::encode_command(command))
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        Ok(codec::decode_command(payload)?)
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        let mut results = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            if entry.index <= self.applied_index {
                return Err(LockAdapterError::AppliedIndexRegression {
                    entry_index: entry.index,
                    applied_index: self.applied_index,
                });
            }
            let result = self.service.apply(entry.command);
            self.applied_index = entry.index;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result,
                local_proposal_id: entry.local_proposal_id,
            });
        }
        Ok(results)
    }

    fn read(
        &self,
        query: Self::Query,
        barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        if self.applied_index < barrier.required_applied_index {
            return Err(LockAdapterError::ReadBarrierUnsatisfied {
                required_applied_index: barrier.required_applied_index,
                applied_index: self.applied_index,
            });
        }
        Ok(match query {
            // Querying an unknown name must not track it, which is the pure
            // model's rule and is why this arm consults `status` rather than
            // anything that could insert.
            LockQuery::GetLock { resource } => LockQueryResult::Lock(self.service.status(resource)),
        })
    }

    fn build_snapshot(&mut self, at: LogIndex) -> Result<ApplicationSnapshot, Self::Error> {
        Err(LockAdapterError::DurableSnapshotUndefined {
            snapshot_index: at,
            applied_index: self.applied_index,
        })
    }

    fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<(), Self::Error> {
        Err(LockAdapterError::DurableSnapshotUndefined {
            snapshot_index: snapshot.applied_index,
            applied_index: self.applied_index,
        })
    }
}
