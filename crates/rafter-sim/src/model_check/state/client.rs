use std::collections::BTreeMap;

use rafter::{LocalProposalDropReason, LogIndex, NodeId, SharedPayload, Term};

use crate::records::LocalProposalEvent;
use crate::{Applied, Cluster};

use super::super::{helpers::proposal_payload, ProposalId};
use super::ExplorationState;

#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct ClientHistory {
    pub(crate) initial_value: Option<SharedPayload>,
    pub(crate) next_event: u64,
    pub(crate) writes: BTreeMap<ProposalId, ClientWrite>,
    pub(crate) reads: BTreeMap<u64, ClientRead>,
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

#[derive(Clone, Debug, Hash)]
pub(crate) struct ClientRead {
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
}

#[derive(Clone, Copy, Debug, Hash)]
pub(crate) struct ClientReadProof {
    pub(crate) application_epoch: u64,
    pub(crate) read_index: LogIndex,
    pub(crate) local_applied_index: LogIndex,
}

pub(super) fn register_value_at(
    applied: &[Applied],
    node_id: NodeId,
    application_epoch: u64,
    read_index: LogIndex,
) -> Option<SharedPayload> {
    applied
        .iter()
        .filter(|applied| {
            applied.node_id == node_id
                && applied.application_epoch == application_epoch
                && applied.index <= read_index
        })
        .max_by_key(|applied| applied.index)
        .map(|applied| applied.payload.clone())
}

pub(super) fn initial_register_value(cluster: &Cluster) -> Option<SharedPayload> {
    cluster
        .applied()
        .iter()
        .filter(|applied| applied.application_epoch == cluster.application_epoch(applied.node_id))
        .max_by_key(|applied| applied.index)
        .map(|applied| applied.payload.clone())
}

impl ExplorationState {
    pub(in crate::model_check) fn record_client_proposal(
        &mut self,
        node_id: NodeId,
        proposal_id: ProposalId,
        stale_leader: bool,
    ) {
        let started_at = self.client_history.next_event();
        let status = if stale_leader {
            ClientWriteStatus::Unknown {
                reason: ClientWriteUnknownReason::StaleLeader,
            }
        } else {
            ClientWriteStatus::Pending
        };
        self.client_history.writes.insert(
            proposal_id,
            ClientWrite {
                proposal_id,
                node_id,
                payload: proposal_payload(proposal_id).into(),
                started_at,
                status,
            },
        );
    }

    pub(in crate::model_check) fn record_client_read(
        &mut self,
        node_id: NodeId,
        request_id: u64,
        committed_floor: LogIndex,
    ) {
        let started_at = self.client_history.next_event();
        self.client_history.reads.insert(
            request_id,
            ClientRead {
                node_id,
                request_id,
                committed_floor,
                started_at,
                outcome: ClientReadOutcome::Pending,
            },
        );
    }

    pub(in crate::model_check) fn refresh_client_history(&mut self) {
        let mut next_event = self.client_history.next_event;
        self.refresh_client_reads(&mut next_event);
        self.client_history.next_event = next_event;
    }

    pub(in crate::model_check) fn record_local_proposal_events(
        &mut self,
        events: &[LocalProposalEvent],
    ) {
        for event in events {
            self.record_local_proposal_event(event);
        }
    }

    fn record_local_proposal_event(&mut self, event: &LocalProposalEvent) {
        match event {
            LocalProposalEvent::Appended {
                node_id,
                proposal_id,
                index,
                term,
            } => {
                self.record_local_proposal_appended(*node_id, proposal_id.0, *index, *term);
            }
            LocalProposalEvent::Applied {
                node_id,
                proposal_id,
                index,
                term,
                payload,
            } => {
                self.record_local_proposal_applied(*node_id, proposal_id.0, *index, *term, payload);
            }
            LocalProposalEvent::Dropped {
                node_id,
                proposal_id,
                index,
                term,
                reason,
            } => {
                self.record_local_proposal_dropped(*node_id, proposal_id.0, *index, *term, *reason);
            }
            LocalProposalEvent::Rejected {
                node_id,
                proposal_id,
                ..
            } => {
                self.record_local_proposal_rejected(*node_id, proposal_id.0);
            }
        }
    }

    fn record_local_proposal_appended(
        &mut self,
        node_id: NodeId,
        proposal_id: u64,
        index: LogIndex,
        term: Term,
    ) {
        let proposal_id = ProposalId(proposal_id);
        let Some(write) = self.client_history.writes.get(&proposal_id) else {
            return;
        };
        let matches_operation = write.node_id == node_id
            && self
                .cluster
                .bootstrap_state(node_id)
                .log
                .into_iter()
                .find(|entry| entry.index == index)
                .is_some_and(|entry| {
                    entry.term == term
                        && entry.kind.application_payload() == Some(write.payload.as_slice())
                });
        if matches_operation && matches!(write.status, ClientWriteStatus::Pending) {
            if let Some(write) = self.client_history.writes.get_mut(&proposal_id) {
                write.status = ClientWriteStatus::Accepted {
                    node_id,
                    index,
                    term,
                };
            }
        }
    }

    fn record_local_proposal_applied(
        &mut self,
        node_id: NodeId,
        proposal_id: u64,
        index: LogIndex,
        term: Term,
        payload: &SharedPayload,
    ) {
        let proposal_id = ProposalId(proposal_id);
        let Some(write) = self.client_history.writes.get(&proposal_id) else {
            return;
        };
        let exact_acceptance = matches!(
            write.status,
            ClientWriteStatus::Accepted {
                node_id: accepted_by,
                index: accepted_index,
                term: accepted_term,
            } if accepted_by == node_id && accepted_index == index && accepted_term == term
        );
        if !exact_acceptance || write.payload != *payload {
            return;
        }
        let completed_at = self.client_history.next_event();
        if let Some(write) = self.client_history.writes.get_mut(&proposal_id) {
            write.status = ClientWriteStatus::Completed {
                node_id,
                index,
                completed_at,
            };
        }
    }

    fn record_local_proposal_dropped(
        &mut self,
        node_id: NodeId,
        proposal_id: u64,
        index: LogIndex,
        term: Term,
        _reason: LocalProposalDropReason,
    ) {
        let proposal_id = ProposalId(proposal_id);
        let Some(write) = self.client_history.writes.get_mut(&proposal_id) else {
            return;
        };
        if matches!(
            write.status,
            ClientWriteStatus::Accepted {
                node_id: accepted_by,
                index: accepted_index,
                term: accepted_term,
            } if accepted_by == node_id && accepted_index == index && accepted_term == term
        ) {
            write.status = ClientWriteStatus::Unknown {
                reason: ClientWriteUnknownReason::LocalTrackingDropped,
            };
        }
    }

    fn record_local_proposal_rejected(&mut self, node_id: NodeId, proposal_id: u64) {
        let Some(write) = self.client_history.writes.get_mut(&ProposalId(proposal_id)) else {
            return;
        };
        if write.node_id == node_id && matches!(write.status, ClientWriteStatus::Pending) {
            write.status = ClientWriteStatus::Rejected;
        }
    }

    fn refresh_client_reads(&mut self, next_event: &mut u64) {
        let applied = &self.cluster.applied;
        for read in self.client_history.reads.values_mut() {
            if matches!(&read.outcome, ClientReadOutcome::Completed { .. }) {
                continue;
            }
            let Some(grant) =
                self.cluster.read_grants().iter().find(|grant| {
                    grant.node_id == read.node_id && grant.request_id == read.request_id
                })
            else {
                continue;
            };
            let proof = ClientReadProof {
                application_epoch: grant.application_epoch,
                read_index: grant.read_index,
                local_applied_index: self.cluster.local_applied_index(read.node_id),
            };
            read.outcome = if proof.local_applied_index >= proof.read_index {
                let result = register_value_at(
                    applied,
                    read.node_id,
                    proof.application_epoch,
                    proof.read_index,
                );
                let outcome = ClientReadOutcome::Completed {
                    proof,
                    result,
                    completed_at: *next_event,
                };
                *next_event += 1;
                outcome
            } else {
                ClientReadOutcome::ProofGranted { proof }
            };
        }
    }
}
