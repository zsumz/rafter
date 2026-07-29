use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use rafter::{LocalProposalDropReason, LogIndex, NodeId, SharedPayload, Term};

use crate::records::{LocalProposalEvent, ReadTerminalOutput};
use crate::Cluster;

use super::super::{helpers::proposal_payload, ProposalId};
use super::ExplorationState;

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

pub(super) fn register_value_at(
    cluster: &Cluster,
    node_id: NodeId,
    application_epoch: u64,
    read_index: LogIndex,
) -> Option<SharedPayload> {
    let applied = cluster
        .applied()
        .iter()
        .filter(|applied| {
            applied.node_id == node_id
                && applied.application_epoch == application_epoch
                && applied.index <= read_index
        })
        .max_by_key(|applied| applied.index)
        .map(|applied| (applied.index, applied.payload.clone()));
    let installed = cluster
        .snapshot_installs()
        .iter()
        .filter(|snapshot| {
            snapshot.node_id == node_id
                && snapshot.application_epoch == application_epoch
                && snapshot.last_included_index <= read_index
        })
        .max_by_key(|snapshot| snapshot.last_included_index)
        .map(|snapshot| {
            (
                snapshot.last_included_index,
                snapshot.payload.clone().into(),
            )
        });
    let current = (cluster.application_epoch(node_id) == application_epoch)
        .then(|| cluster.node(node_id).snapshot())
        .flatten()
        .filter(|snapshot| snapshot.metadata.last_included_index <= read_index)
        .and_then(|snapshot| {
            cluster.snapshot_payload(node_id, snapshot).map(|payload| {
                (
                    snapshot.metadata.last_included_index,
                    payload.to_vec().into(),
                )
            })
        });
    [applied, installed, current]
        .into_iter()
        .flatten()
        .max_by_key(|(index, _)| *index)
        .map(|(_, value)| value)
}

pub(super) fn initial_register_value(cluster: &Cluster) -> Option<SharedPayload> {
    cluster
        .nodes
        .keys()
        .map(|node_id| {
            let epoch = cluster.application_epoch(*node_id);
            let applied = cluster.local_applied_index(*node_id);
            (
                applied,
                register_value_at(cluster, *node_id, epoch, applied),
            )
        })
        .filter_map(|(index, value)| value.map(|value| (index, value)))
        .max_by_key(|(index, _)| *index)
        .map(|(_, value)| value)
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
        registration: &crate::ReadRegistered,
    ) {
        let started_at = self.client_history.next_event();
        self.client_history.reads.insert(
            registration.operation_id,
            ClientRead {
                operation_id: registration.operation_id,
                node_id: registration.node_id,
                request_id: registration.request_id,
                committed_floor: registration.committed_floor,
                started_at,
                outcome: ClientReadOutcome::Pending,
            },
        );
    }

    #[cfg(test)]
    pub(in crate::model_check) fn record_client_read_completion_corruption(
        &mut self,
        operation_id: u64,
        proof: ClientReadProof,
        result: Option<SharedPayload>,
    ) -> Result<(), &'static str> {
        let completed_at = self.client_history.next_event();
        let read = self
            .client_history
            .reads
            .get_mut(&operation_id)
            .ok_or("read recorder corruption requires a registered read")?;
        if !matches!(read.outcome, ClientReadOutcome::Pending) {
            return Err("read recorder corruption requires a pending read");
        }
        read.outcome = ClientReadOutcome::Completed {
            proof,
            result,
            completed_at,
        };
        Ok(())
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
            self.record_client_instrumentation_error(
                proposal_id,
                "appended",
                format!("no client write exists for {node_id} at ({index}, term {term})"),
            );
            return;
        };
        let expected_node = write.node_id;
        let status = write.status;
        let log_has_entry = self
            .cluster
            .bootstrap_state(node_id)
            .log
            .into_iter()
            .find(|entry| entry.index == index)
            .is_some_and(|entry| entry.term == term && entry.kind.application_payload().is_some());
        let tracked = TrackedProposalEntry {
            node_id,
            index,
            term,
        };
        let existing_matches = self
            .client_history
            .tracked_entries
            .get(&proposal_id)
            .is_none_or(|existing| *existing == tracked);
        let status_accepts_append = matches!(
            status,
            ClientWriteStatus::Pending | ClientWriteStatus::Unknown { .. }
        ) || matches!(
            status,
            ClientWriteStatus::Accepted {
                    node_id: accepted_by,
                    index: accepted_index,
                    term: accepted_term,
                } if accepted_by == node_id && accepted_index == index && accepted_term == term
        );
        if expected_node != node_id || !log_has_entry || !existing_matches || !status_accepts_append
        {
            self.record_client_instrumentation_error(
                proposal_id,
                "appended",
                format!(
                    "event=({node_id}, {index}, term {term}), expected_node={expected_node}, status={status:?}, log_has_entry={log_has_entry}, existing_matches={existing_matches}"
                ),
            );
            return;
        }
        self.client_history
            .tracked_entries
            .insert(proposal_id, tracked);
        if matches!(status, ClientWriteStatus::Pending) {
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
            self.record_client_instrumentation_error(
                proposal_id,
                "applied",
                format!("no client write exists for {node_id} at ({index}, term {term})"),
            );
            return;
        };
        let status = write.status;
        let expected_payload = write.payload.clone();
        let tracked = TrackedProposalEntry {
            node_id,
            index,
            term,
        };
        let exact_tracking =
            self.client_history.tracked_entries.get(&proposal_id) == Some(&tracked);
        let status_accepts_apply = matches!(
            status,
            ClientWriteStatus::Accepted {
                node_id: accepted_by,
                index: accepted_index,
                term: accepted_term,
            } if accepted_by == node_id && accepted_index == index && accepted_term == term
        ) || matches!(status, ClientWriteStatus::Unknown { .. });
        if !exact_tracking || !status_accepts_apply || expected_payload != *payload {
            self.record_client_instrumentation_error(
                proposal_id,
                "applied",
                format!(
                    "event=({node_id}, {index}, term {term}), status={status:?}, exact_tracking={exact_tracking}, payload_matches={} ",
                    expected_payload == *payload
                ),
            );
            return;
        }
        if matches!(status, ClientWriteStatus::Unknown { .. }) {
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
        let Some(write) = self.client_history.writes.get(&proposal_id) else {
            self.record_client_instrumentation_error(
                proposal_id,
                "dropped",
                format!("no client write exists for {node_id} at ({index}, term {term})"),
            );
            return;
        };
        let status = write.status;
        let exact_tracking = self.client_history.tracked_entries.get(&proposal_id)
            == Some(&TrackedProposalEntry {
                node_id,
                index,
                term,
            });
        let status_accepts_drop = matches!(
            status,
            ClientWriteStatus::Accepted {
                    node_id: accepted_by,
                    index: accepted_index,
                    term: accepted_term,
                } if accepted_by == node_id && accepted_index == index && accepted_term == term
        ) || matches!(
            status,
            ClientWriteStatus::Unknown {
                reason: ClientWriteUnknownReason::StaleLeader
            }
        );
        if !exact_tracking || !status_accepts_drop {
            self.record_client_instrumentation_error(
                proposal_id,
                "dropped",
                format!(
                    "event=({node_id}, {index}, term {term}), status={status:?}, exact_tracking={exact_tracking}"
                ),
            );
            return;
        }
        if let Some(write) = self.client_history.writes.get_mut(&proposal_id) {
            write.status = ClientWriteStatus::Unknown {
                reason: ClientWriteUnknownReason::LocalTrackingDropped,
            };
        }
    }

    fn record_local_proposal_rejected(&mut self, node_id: NodeId, proposal_id: u64) {
        let proposal_id = ProposalId(proposal_id);
        let Some(write) = self.client_history.writes.get(&proposal_id) else {
            self.record_client_instrumentation_error(
                proposal_id,
                "rejected",
                format!("no client write exists for {node_id}"),
            );
            return;
        };
        let expected_node = write.node_id;
        let status = write.status;
        if expected_node != node_id
            || !matches!(
                status,
                ClientWriteStatus::Pending
                    | ClientWriteStatus::Unknown {
                        reason: ClientWriteUnknownReason::StaleLeader
                    }
            )
        {
            self.record_client_instrumentation_error(
                proposal_id,
                "rejected",
                format!("event_node={node_id}, expected_node={expected_node}, status={status:?}"),
            );
            return;
        }
        if let Some(write) = self.client_history.writes.get_mut(&proposal_id) {
            write.status = ClientWriteStatus::Rejected;
        }
    }

    fn record_client_instrumentation_error(
        &mut self,
        proposal_id: ProposalId,
        event: &'static str,
        detail: String,
    ) {
        self.client_history
            .instrumentation_errors
            .insert(ClientInstrumentationError {
                proposal_id,
                event,
                detail,
            });
    }

    fn refresh_client_reads(&mut self, next_event: &mut u64) {
        let mut instrumentation_errors = Vec::new();
        for read in self.client_history.reads.values_mut() {
            if matches!(
                &read.outcome,
                ClientReadOutcome::Completed { .. }
                    | ClientReadOutcome::Rejected { .. }
                    | ClientReadOutcome::Canceled { .. }
            ) {
                continue;
            }
            if let Some(terminal) = self
                .cluster
                .read_terminal_outputs()
                .iter()
                .copied()
                .find(|terminal| terminal.matches_operation(read.operation_id))
            {
                read.outcome = match terminal {
                    ReadTerminalOutput::Rejected { .. } => ClientReadOutcome::Rejected {
                        completed_at: *next_event,
                    },
                    ReadTerminalOutput::Canceled { .. } => ClientReadOutcome::Canceled {
                        completed_at: *next_event,
                    },
                };
                *next_event += 1;
                continue;
            }
            let Some(grant) = self
                .cluster
                .read_grants()
                .iter()
                .find(|grant| grant.operation_id == Some(read.operation_id))
            else {
                continue;
            };
            let current_epoch = self.cluster.application_epoch(read.node_id);
            if grant.application_epoch != current_epoch {
                instrumentation_errors.push(format!(
                    "read operation {} for request {} retained grant epoch {} after node {} advanced to application epoch {}",
                    read.operation_id,
                    read.request_id,
                    grant.application_epoch,
                    read.node_id,
                    current_epoch
                ));
                continue;
            }
            let proof = ClientReadProof {
                application_epoch: grant.application_epoch,
                read_index: grant.read_index,
                local_applied_index: self.cluster.local_applied_index(read.node_id),
            };
            read.outcome = if proof.local_applied_index >= proof.read_index {
                let result = register_value_at(
                    &self.cluster,
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
        self.client_history
            .read_instrumentation_errors
            .extend(instrumentation_errors);
    }
}
