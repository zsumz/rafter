//! Client write registration and local-proposal append correlation.
//!
//! The recorder never infers acceptance from payload equality: a write only
//! advances when the kernel's own local-proposal event names it, and any event
//! contradicting recorder state is preserved as an instrumentation error.

use rafter::{LogIndex, NodeId, Term};

use crate::records::LocalProposalEvent;

use super::super::super::{helpers::proposal_payload, ProposalId};
use super::super::ExplorationState;
use super::{
    ClientInstrumentationError, ClientWrite, ClientWriteStatus, ClientWriteUnknownReason,
    TrackedProposalEntry,
};

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

    pub(super) fn record_client_instrumentation_error(
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
}
