use std::{collections::BTreeMap, error::Error, fmt};

use rafter::LogIndex;
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyResult, ReadBarrier,
    ReplicatedStateMachine, SnapshotSupport,
};

use crate::{
    ClientId, CounterCommand, CounterRejection, CounterResult, RequestFingerprint, RequestIdentity,
    Sequence, SessionEpoch,
};

use super::codec;

/// Replicated counter command owned by the consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicatedCounterCommand {
    /// Establishes or advances a replicated client session.
    OpenSession {
        /// Bounded client slot.
        client_id: ClientId,
        /// Requested session generation.
        epoch: SessionEpoch,
    },
    /// Applies or replays one counter request.
    Counter {
        /// Exact request identity.
        request: RequestIdentity,
        /// Counter operation.
        command: CounterCommand,
    },
    /// Models an irrecoverable consumer apply fault.
    ///
    /// The contract already exposes faulty work as a consumer-owned shape.
    /// This variant lets the real adapter prove the corresponding Raft-group
    /// poison and isolation path without adding a Rafter test hook.
    Faulty,
}

/// Replicated session outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionApplyResult {
    /// A vacant client slot opened.
    Opened,
    /// The exact epoch was already active.
    AlreadyOpen,
    /// A greater epoch replaced the old session and its dedup cache.
    Replaced,
}

/// Refusal reached after a command was already replicated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterApplyRejection {
    /// The request did not name an open client slot.
    SessionNotOpen,
    /// The request named an older session generation.
    StaleSession { current: SessionEpoch },
    /// The request named a generation the slot has not reached.
    FutureSession { current: SessionEpoch },
    /// The request fingerprint did not describe its command.
    FingerprintMismatch,
    /// The sequence is older than the cached completion.
    StaleSequence { highest: Sequence },
    /// The sequence skipped the next admissible value.
    SequenceGap { expected: Sequence },
    /// The identity was reused for different command content.
    ConflictingRetry,
}

/// Result emitted by the replicated counter state machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterApplyResult {
    /// Session establishment completed.
    Session(SessionApplyResult),
    /// A counter request completed or replayed.
    Counter(CounterResult),
    /// A replicated request was refused without changing counter state.
    Rejected(CounterApplyRejection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Completed {
    pub(super) sequence: Sequence,
    pub(super) command: CounterCommand,
    pub(super) result: CounterResult,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Session {
    pub(super) epoch: SessionEpoch,
    pub(super) completed: Option<Completed>,
}

/// Small replicated counter with bounded session state and snapshot support.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CounterStateMachine {
    applied_index: LogIndex,
    value: i64,
    max_sessions: usize,
    sessions: BTreeMap<ClientId, Session>,
}

impl CounterStateMachine {
    /// Creates an empty state machine with a fixed replicated-session bound.
    ///
    /// # Panics
    ///
    /// Panics when `max_sessions` is zero or cannot fit in the bounded snapshot
    /// representation.
    #[must_use]
    pub fn new(max_sessions: usize) -> Self {
        assert!(max_sessions != 0, "session bound must be nonzero");
        assert!(
            codec::supports_session_bound(max_sessions),
            "session bound exceeds the snapshot representation"
        );
        Self {
            applied_index: LogIndex::ZERO,
            value: 0,
            max_sessions,
            sessions: BTreeMap::new(),
        }
    }

    fn apply_open_session(
        &mut self,
        client_id: ClientId,
        epoch: SessionEpoch,
    ) -> Result<CounterApplyResult, CounterStateMachineError> {
        let Some(session) = self.sessions.get_mut(&client_id) else {
            if self.sessions.len() >= self.max_sessions {
                return Err(CounterStateMachineError::SessionCapacity);
            }
            self.sessions.insert(
                client_id,
                Session {
                    epoch,
                    completed: None,
                },
            );
            return Ok(CounterApplyResult::Session(SessionApplyResult::Opened));
        };
        if epoch < session.epoch {
            return Ok(CounterApplyResult::Rejected(
                CounterApplyRejection::StaleSession {
                    current: session.epoch,
                },
            ));
        }
        if epoch == session.epoch {
            return Ok(CounterApplyResult::Session(SessionApplyResult::AlreadyOpen));
        }
        session.epoch = epoch;
        session.completed = None;
        Ok(CounterApplyResult::Session(SessionApplyResult::Replaced))
    }

    fn apply_counter(
        &mut self,
        request: RequestIdentity,
        command: CounterCommand,
    ) -> CounterApplyResult {
        let Some(session) = self.sessions.get_mut(&request.client_id) else {
            return CounterApplyResult::Rejected(CounterApplyRejection::SessionNotOpen);
        };
        if request.session_epoch < session.epoch {
            return CounterApplyResult::Rejected(CounterApplyRejection::StaleSession {
                current: session.epoch,
            });
        }
        if request.session_epoch > session.epoch {
            return CounterApplyResult::Rejected(CounterApplyRejection::FutureSession {
                current: session.epoch,
            });
        }
        if request.fingerprint != RequestFingerprint::of(&command) {
            return CounterApplyResult::Rejected(CounterApplyRejection::FingerprintMismatch);
        }
        if let Some(completed) = session.completed {
            if request.sequence < completed.sequence {
                return CounterApplyResult::Rejected(CounterApplyRejection::StaleSequence {
                    highest: completed.sequence,
                });
            }
            if request.sequence == completed.sequence {
                return if command == completed.command {
                    CounterApplyResult::Counter(completed.result)
                } else {
                    CounterApplyResult::Rejected(CounterApplyRejection::ConflictingRetry)
                };
            }
            let Some(expected) = completed.sequence.successor() else {
                return CounterApplyResult::Rejected(CounterApplyRejection::StaleSequence {
                    highest: completed.sequence,
                });
            };
            if request.sequence != expected {
                return CounterApplyResult::Rejected(CounterApplyRejection::SequenceGap {
                    expected,
                });
            }
        } else if request.sequence != Sequence::first() {
            return CounterApplyResult::Rejected(CounterApplyRejection::SequenceGap {
                expected: Sequence::first(),
            });
        }

        let result = match command {
            CounterCommand::Add { delta } => match self.value.checked_add(delta.get()) {
                Some(value) => {
                    self.value = value;
                    CounterResult::Added { value }
                }
                None => CounterResult::Rejected(CounterRejection::CounterOverflow {
                    current: self.value,
                }),
            },
            CounterCommand::Read => CounterResult::Value { value: self.value },
        };
        session.completed = Some(Completed {
            sequence: request.sequence,
            command,
            result,
        });
        CounterApplyResult::Counter(result)
    }
}

impl ReplicatedStateMachine for CounterStateMachine {
    type Command = ReplicatedCounterCommand;
    type CommandResult = CounterApplyResult;
    type Query = ();
    type QueryResult = i64;
    type Error = CounterStateMachineError;

    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        codec::encode_command(command)
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        codec::decode_command(payload)
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        let mut results = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            let result = match entry.command {
                ReplicatedCounterCommand::OpenSession { client_id, epoch } => {
                    self.apply_open_session(client_id, epoch)?
                }
                ReplicatedCounterCommand::Counter { request, command } => {
                    self.apply_counter(request, command)
                }
                ReplicatedCounterCommand::Faulty => {
                    return Err(CounterStateMachineError::InjectedFault);
                }
            };
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

    fn read(&self, (): (), barrier: ReadBarrier) -> Result<Self::QueryResult, Self::Error> {
        if self.applied_index < barrier.required_applied_index {
            return Err(CounterStateMachineError::ReadBarrierNotReached);
        }
        Ok(self.value)
    }

    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
        if at != self.applied_index {
            return Err(CounterStateMachineError::SnapshotIndexMismatch.into());
        }
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: codec::encode_snapshot(self.applied_index, self.value, &self.sessions)?,
            raft_snapshot: None,
        })
    }

    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        if snapshot.payload.is_empty() && snapshot.raft_snapshot.is_some() {
            return Err(CounterStateMachineError::PromotedSnapshotUnsupported.into());
        }
        let (applied_index, value, sessions) =
            codec::decode_snapshot(&snapshot.payload, self.max_sessions)?;
        if applied_index != snapshot.applied_index {
            return Err(CounterStateMachineError::SnapshotIndexMismatch.into());
        }
        self.applied_index = applied_index;
        self.value = value;
        self.sessions = sessions;
        Ok(())
    }
}

/// Bounded codec or state-machine failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterStateMachineError {
    CommandTooLarge,
    MalformedCommand,
    MalformedSnapshot,
    UnsupportedVersion,
    SessionCapacity,
    SnapshotTooLarge,
    SnapshotIndexMismatch,
    PromotedSnapshotUnsupported,
    ReadBarrierNotReached,
    InjectedFault,
}

impl fmt::Display for CounterStateMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CommandTooLarge => "replicated command exceeds its bound",
            Self::MalformedCommand => "replicated command is malformed",
            Self::MalformedSnapshot => "counter snapshot is malformed",
            Self::UnsupportedVersion => "counter schema version is unsupported",
            Self::SessionCapacity => "replicated session capacity is exhausted",
            Self::SnapshotTooLarge => "counter snapshot exceeds its bound",
            Self::SnapshotIndexMismatch => "counter snapshot index does not match its payload",
            Self::PromotedSnapshotUnsupported => {
                "counter adapter cannot load a promoted snapshot payload"
            }
            Self::ReadBarrierNotReached => "counter read barrier has not been reached",
            Self::InjectedFault => "consumer fault poisoned the counter group",
        })
    }
}

impl Error for CounterStateMachineError {}
