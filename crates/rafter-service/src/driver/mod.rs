//! Managed service driver loop.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Debug,
    sync::{Arc, Mutex, MutexGuard},
};

use rafter::{LocalProposalId, LogIndex, NodeId, ProposalRejection, ReadId, Role};
use rafter_app::{
    error::{GroupError, StateMachineOperation},
    group::{
        GroupFatalState, GroupInput, GroupStepReport, LeadershipTransferEvent, RaftGroup,
        StepReportOptions,
    },
    metrics::RaftGroupMetrics,
    proposal::{Proposal, ProposalEvent, ProposalUnknownOutcomeReason},
    read::{ReadConsistency, ReadOutcome, ReadRequest},
    state_machine::ReplicatedStateMachine,
    transport::PeerEnvelope,
};
use rafter_runtime_api::PersistedRaftRuntime;

use crate::{
    error::{
        MetricsError, ReadError, ShutdownError, TransferLeadershipError, UnknownOutcomeReason,
        WriteError,
    },
    handle::RaftHandle,
    watch::{MetricsPublisher, MetricsWatch},
};

mod adoption;
#[path = "trait.rs"]
mod driver_trait;
mod in_memory;
mod mapping;
mod metrics;
mod options;
mod read;
mod state;
mod transfer;
mod write;

pub use driver_trait::{DriverCommandSender, DriverFuture};
pub use in_memory::InMemoryRaftDriver;
pub use mapping::ManagedDriverError;
pub use metrics::metrics_watch_from_current;
pub use options::{QueryReceipt, WriteBatchEntry, WriteOptions, WriteReceipt};

use mapping::{write_error_from_group, ManagedOperationError};
use state::{lock_state, InMemoryRaftState};

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
