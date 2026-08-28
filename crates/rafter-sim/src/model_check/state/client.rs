//! The client-visible history of the writes and reads a run performed.
//!
//! Each operation records when it started, what it was promised, and how it
//! terminated — completed, rejected, canceled, or explicitly unknown — because
//! linearizability is argued over that record. An event contradicting the
//! recorder is kept as an instrumentation error rather than smoothed away.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use rafter::{LogIndex, NodeId, SharedPayload, Term};

use super::super::ProposalId;

mod outcomes;
mod proposals;
mod reads;
mod register;

pub(super) use register::{initial_register_value, register_value_at};

#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct ClientHistory {
    pub(crate) initial_value: Option<SharedPayload>,
    pub(crate) next_event: u64,
    pub(crate) writes: BTreeMap<ProposalId, ClientWrite>,
    pub(crate) reads: BTreeMap<u64, ClientRead>,
    pub(crate) tracked_entries: BTreeMap<ProposalId, TrackedProposalEntry>,
    pub(crate) instrumentation_errors: BTreeSet<ClientInstrumentationError>,
    pub(crate) read_instrumentation_errors: BTreeSet<String>,
}

impl ClientHistory {
    pub(super) fn with_initial_value(initial_value: Option<SharedPayload>) -> Self {
        Self {
            initial_value,
            ..Self::default()
        }
    }

    pub(super) fn next_event(&mut self) -> u64 {
        let event = self.next_event;
        self.next_event += 1;
        event
    }
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct ClientWrite {
    pub(crate) proposal_id: ProposalId,
    pub(crate) node_id: NodeId,
    pub(crate) payload: SharedPayload,
    pub(crate) started_at: u64,
    pub(crate) status: ClientWriteStatus,
}

#[derive(Clone, Copy, Debug, Hash)]
pub(crate) enum ClientWriteStatus {
    Pending,
    Accepted {
        node_id: NodeId,
        index: LogIndex,
        term: Term,
    },
    Completed {
        node_id: NodeId,
        index: LogIndex,
        completed_at: u64,
    },
    Unknown {
        reason: ClientWriteUnknownReason,
    },
    Rejected,
}

#[derive(Clone, Copy, Debug, Hash)]
pub(crate) enum ClientWriteUnknownReason {
    StaleLeader,
    LocalTrackingDropped,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TrackedProposalEntry {
    pub(crate) node_id: NodeId,
    pub(crate) index: LogIndex,
    pub(crate) term: Term,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ClientInstrumentationError {
    pub(crate) proposal_id: ProposalId,
    pub(crate) event: &'static str,
    pub(crate) detail: String,
}

impl fmt::Display for ClientInstrumentationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tracked proposal {} {} event contradicted recorder state: {}",
            self.proposal_id.0, self.event, self.detail
        )
    }
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct ClientRead {
    pub(crate) operation_id: u64,
    pub(crate) node_id: NodeId,
    pub(crate) request_id: u64,
    pub(crate) committed_floor: LogIndex,
    pub(crate) started_at: u64,
    pub(crate) outcome: ClientReadOutcome,
}

#[derive(Clone, Debug, Hash)]
pub(crate) enum ClientReadOutcome {
    Pending,
    ProofGranted {
        proof: ClientReadProof,
    },
    Completed {
        proof: ClientReadProof,
        result: Option<SharedPayload>,
        completed_at: u64,
    },
    Rejected {
        completed_at: u64,
    },
    Canceled {
        completed_at: u64,
    },
}

#[derive(Clone, Copy, Debug, Hash)]
pub(crate) struct ClientReadProof {
    pub(crate) application_epoch: u64,
    pub(crate) read_index: LogIndex,
    pub(crate) local_applied_index: LogIndex,
}
