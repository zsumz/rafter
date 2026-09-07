//! Terminal outcomes for tracked client writes.
//!
//! Apply, drop, and rejection events each require the exact tracked entry the
//! append recorded; anything else leaves the write in its current status and
//! records an instrumentation error instead of guessing an outcome.

use rafter::{LocalProposalDropReason, LogIndex, NodeId, SharedPayload, Term};

use super::super::super::ProposalId;
use super::super::ExplorationState;
use super::{ClientWriteStatus, ClientWriteUnknownReason, TrackedProposalEntry};

impl ExplorationState {
    pub(super) fn record_local_proposal_applied(
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

    pub(super) fn record_local_proposal_dropped(
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

    pub(super) fn record_local_proposal_rejected(&mut self, node_id: NodeId, proposal_id: u64) {
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
}
