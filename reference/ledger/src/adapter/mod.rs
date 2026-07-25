//! Public `rafter-app` integration for the pure ledger model.
//!
//! The adapter owns encoding, applied-index discipline, the read path, and the
//! versioned snapshot representation the contract promises. Every ledger
//! transition, session decision, and deduplication rule stays in
//! [`crate::Ledger`]; nothing here re-derives them. The query vocabulary itself
//! is application contract rather than integration, so it lives in
//! [`crate::LedgerQuery`] where the history checker can reach it without
//! reaching through this Rafter-facing module.

mod codec;

use std::{error::Error, fmt};

use rafter::LogIndex;
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyResult, ReadBarrier,
    ReplicatedStateMachine, SnapshotSupport,
};

use crate::{
    ApplyOutcome, Command, Ledger, LedgerConfig, LedgerQuery, LedgerQueryResult, SnapshotError,
};

pub use codec::{LedgerCodecError, NonZeroField};

/// Failure of an adapter operation, as distinct from a ledger result.
///
/// Contract-defined session, sequence, and business rejections are ordinary
/// [`ApplyOutcome`] values, not errors. Every variant here means the adapter
/// was asked for something the application contract cannot express, and the
/// group layer is expected to treat it as fatal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerAdapterError {
    /// A replicated command or snapshot frame was malformed.
    Codec(LedgerCodecError),
    /// A committed entry arrived at or below the durable applied floor.
    ///
    /// Re-applying it would execute an acknowledged command twice.
    AppliedIndexRegression {
        entry_index: LogIndex,
        applied_index: LogIndex,
    },
    /// A read barrier required freshness this replica has not applied.
    ReadBarrierUnsatisfied {
        required_applied_index: LogIndex,
        applied_index: LogIndex,
    },
    /// A snapshot was requested at an index this state machine cannot
    /// reproduce; it only holds the state of its current applied index.
    SnapshotIndexUnavailable {
        requested_index: LogIndex,
        applied_index: LogIndex,
    },
    /// The snapshot payload's own applied index disagrees with the index the
    /// installer declared.
    SnapshotIndexMismatch {
        payload_index: LogIndex,
        declared_index: LogIndex,
    },
    /// Installing the snapshot would move the applied floor backwards and make
    /// acknowledged commands executable again.
    SnapshotBehindAppliedIndex {
        snapshot_index: LogIndex,
        applied_index: LogIndex,
    },
    /// A snapshot install carried no inline payload.
    ///
    /// Resolving a promoted payload from a Raft snapshot descriptor requires a
    /// durable application snapshot store, which arrives with the durable
    /// composition slice.
    SnapshotPayloadUnavailable { applied_index: LogIndex },
    /// A decoded snapshot violated a model resource or supply invariant.
    Snapshot(SnapshotError),
}

impl fmt::Display for LedgerAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(formatter, "malformed ledger frame: {error}"),
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
            Self::SnapshotIndexUnavailable {
                requested_index,
                applied_index,
            } => write!(
                formatter,
                "cannot build a snapshot at index {requested_index} from state applied through {applied_index}"
            ),
            Self::SnapshotIndexMismatch {
                payload_index,
                declared_index,
            } => write!(
                formatter,
                "snapshot payload covers index {payload_index}, but the install declared {declared_index}"
            ),
            Self::SnapshotBehindAppliedIndex {
                snapshot_index,
                applied_index,
            } => write!(
                formatter,
                "snapshot at index {snapshot_index} is behind applied index {applied_index}"
            ),
            Self::SnapshotPayloadUnavailable { applied_index } => write!(
                formatter,
                "snapshot install at applied index {applied_index} carried no inline payload"
            ),
            Self::Snapshot(error) => write!(formatter, "invalid ledger snapshot: {error:?}"),
        }
    }
}

impl Error for LedgerAdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            _ => None,
        }
    }
}

impl From<LedgerCodecError> for LedgerAdapterError {
    fn from(error: LedgerCodecError) -> Self {
        Self::Codec(error)
    }
}

/// The ledger behind Rafter's public replicated state-machine contract.
///
/// The adapter holds the durable application boundary: the ledger's state and
/// the Raft index whose effects that state already reflects. Both move
/// together in [`ReplicatedStateMachine::apply_batch`] and in
/// [`ReplicatedStateMachine::install_snapshot`], so no acknowledged command
/// can execute twice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerStateMachine {
    config: LedgerConfig,
    ledger: Ledger,
    applied_index: LogIndex,
}

impl LedgerStateMachine {
    /// Creates an empty state machine with no applied entries.
    #[must_use]
    pub fn new(config: LedgerConfig) -> Self {
        Self {
            config,
            ledger: Ledger::new(config),
            applied_index: LogIndex::ZERO,
        }
    }

    /// Returns the configured resource bounds.
    #[must_use]
    pub const fn config(&self) -> LedgerConfig {
        self.config
    }

    /// Returns the ledger for inspection and independent comparison.
    #[must_use]
    pub const fn ledger(&self) -> &Ledger {
        &self.ledger
    }
}

impl ReplicatedStateMachine for LedgerStateMachine {
    type Command = Command;
    type CommandResult = ApplyOutcome;
    type Query = LedgerQuery;
    type QueryResult = LedgerQueryResult;
    type Error = LedgerAdapterError;

    /// Declared `Supported`: both methods below round-trip the whole ledger
    /// through the adapter's own frames, and `adapter_contract.rs` proves it.
    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

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
                return Err(LedgerAdapterError::AppliedIndexRegression {
                    entry_index: entry.index,
                    applied_index: self.applied_index,
                });
            }
            let result = self.ledger.apply(entry.command);
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
            return Err(LedgerAdapterError::ReadBarrierUnsatisfied {
                required_applied_index: barrier.required_applied_index,
                applied_index: self.applied_index,
            });
        }
        Ok(self.ledger.query(query))
    }

    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
        if at != self.applied_index {
            return Err(LedgerAdapterError::SnapshotIndexUnavailable {
                requested_index: at,
                applied_index: self.applied_index,
            }
            .into());
        }
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: codec::encode_snapshot(at.0, &self.ledger.snapshot())
                .map_err(LedgerAdapterError::from)?,
            raft_snapshot: None,
        })
    }

    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        if snapshot.applied_index < self.applied_index {
            return Err(LedgerAdapterError::SnapshotBehindAppliedIndex {
                snapshot_index: snapshot.applied_index,
                applied_index: self.applied_index,
            }
            .into());
        }
        if snapshot.payload.is_empty() {
            return Err(LedgerAdapterError::SnapshotPayloadUnavailable {
                applied_index: snapshot.applied_index,
            }
            .into());
        }

        let (payload_index, ledger_snapshot) =
            codec::decode_snapshot(&snapshot.payload).map_err(LedgerAdapterError::from)?;
        if payload_index != snapshot.applied_index.0 {
            return Err(LedgerAdapterError::SnapshotIndexMismatch {
                payload_index: LogIndex(payload_index),
                declared_index: snapshot.applied_index,
            }
            .into());
        }

        self.ledger = Ledger::from_snapshot(self.config, ledger_snapshot)
            .map_err(LedgerAdapterError::Snapshot)?;
        self.applied_index = snapshot.applied_index;
        Ok(())
    }
}
