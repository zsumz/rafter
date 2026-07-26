//! Managed service driver loop.
//!
//! The driver is split across several files, and each of them opens with
//! `use super::*` over the import list and type aliases declared here. That is
//! why every one of them allows `clippy::wildcard_imports`: the alternative is
//! the same twenty-line import block repeated in each file, kept in sync by
//! hand. The files are parts of one module rather than independent units, and
//! the wildcard is what says so.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt::{self, Debug},
    sync::{Arc, Mutex, MutexGuard},
};

use rafter::{
    LocalProposalId, LogIndex, NodeId, Output as RaftOutput, ProposalRejection, ReadId, Role,
};
use rafter_app::{
    error::GroupError,
    group::{
        GroupFatalState, GroupInput, GroupStepReport, LeadershipTransferEvent, RaftGroup,
        StepReportOptions,
    },
    membership::MembershipEvent,
    proposal::{ClientRequestId, Proposal, ProposalEvent, ProposalUnknownOutcomeReason},
    read::{ReadConsistency, ReadEvent, ReadOutcome, ReadRequest},
    snapshot::SnapshotEvent,
    state_machine::ReplicatedStateMachine,
    transport::PeerEnvelope,
};
use rafter_runtime_api::PersistedRaftRuntime;

use crate::{
    error::{
        ErrorCause, MetricsError, ReadAbandonReason, ReadError, ShutdownError,
        TransferLeadershipError, UnknownOutcomeReason, WriteError, WriteFate,
    },
    handle::RaftHandle,
    watch::{MetricsPublisher, MetricsWatch},
};

mod adoption;
#[path = "trait.rs"]
mod driver_trait;
mod in_memory;
mod mapping;
mod options;
mod read;
mod state;
mod transfer;
mod transport;
mod write;

pub use driver_trait::{DriverCommandSender, DriverFuture};
pub use in_memory::InMemoryRaftDriver;
pub use mapping::ManagedDriverError;
pub use options::{QueryReceipt, ReadOptions, WriteBatchEntry, WriteOptions, WriteReceipt};
pub use transport::{
    AddressedRead, AddressedWrite, InboundEnvelopeError, PendingWrite, TransportDriverOptions,
    TransportRaftDriver,
};

use mapping::{
    read_error_from_group, terminal_read_error, transfer_error_from_group, write_error_from_group,
    DriverRoutingError, ManagedOperationError,
};
use state::{lock_state, InMemoryRaftState};
use write::{managed_unknown_reason_from_app, write_error_from_rejection};

type DriverStepReport<G, A> = GroupStepReport<G, <A as ReplicatedStateMachine>::CommandResult>;
type ManagedError<A, R> =
    ManagedOperationError<<A as ReplicatedStateMachine>::Error, <R as PersistedRaftRuntime>::Error>;
type ManagedResult<A, R, T> = Result<T, ManagedError<A, R>>;
type ManagedWriteResult<A, R> =
    ManagedResult<A, R, WriteReceipt<<A as ReplicatedStateMachine>::CommandResult>>;
type ManagedQueryResult<G, A, R> =
    ManagedResult<A, R, QueryReceipt<G, <A as ReplicatedStateMachine>::QueryResult>>;
type ManagedReadResult<G, A, R> =
    Result<Option<QueryReceipt<G, <A as ReplicatedStateMachine>::QueryResult>>, ManagedError<A, R>>;
type ReadRequestResult<G, A, R> =
    ManagedResult<A, R, ReadRequest<G, <A as ReplicatedStateMachine>::Query>>;
type ManagedGroupResult<'a, G, A, R> = ManagedResult<A, R, &'a mut RaftGroup<G, A, R>>;
type DriverStepResult<G, A, R> = ManagedResult<A, R, Option<DriverStepReport<G, A>>>;
