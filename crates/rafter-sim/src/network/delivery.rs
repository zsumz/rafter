use rafter::{Input, LogEntryKind, LogIndex, NodeId};

use super::Envelope;
use crate::records::RecordedOutputs;
use crate::{Cluster, ExecutedLogEntry, ExecutionCursor, ExecutionWitness};

#[path = "delivery_error.rs"]
mod error;
#[path = "delivery_proposal.rs"]
mod proposal;

mod execution;
mod instrumentation;
mod outputs;
mod queue;
mod reads;

pub(crate) use error::ExecutionInstrumentationError;

impl Cluster {
    pub(crate) fn deliver_observed(&mut self, envelope: Envelope) -> RecordedOutputs {
        self.record_delivered_acknowledgement(&envelope);
        let to = envelope.to;
        let outputs = self.node_mut(to).step(Input::Message {
            from: envelope.from,
            message: envelope.message,
        });
        self.record_outputs_observed(to, outputs)
    }

    pub(crate) fn deliver(&mut self, envelope: Envelope) -> Vec<Envelope> {
        self.deliver_observed(envelope).emitted
    }

    /// A delivered success acknowledgement raises the sender's durable-loss
    /// floor: from this moment a leader has counted the entries through the
    /// acknowledged index, so a legal lossy restart must keep them.
    fn record_delivered_acknowledgement(&mut self, envelope: &Envelope) {
        let acknowledged = match &envelope.message {
            rafter::Message::AppendEntriesResponse(response) if response.success => {
                Some((response.follower_id, response.match_index))
            }
            rafter::Message::InstallSnapshotResponse(response) if response.success => {
                Some((response.follower_id, response.last_included_index))
            }
            _ => None,
        };
        if let Some((node_id, index)) = acknowledged {
            let floor = self.delivered_ack_floor.entry(node_id).or_default();
            *floor = (*floor).max(index);
        }
    }

    fn record_durable_applied(&mut self, node_id: NodeId, index: LogIndex) {
        let floor = self.durable_applied.entry(node_id).or_default();
        *floor = (*floor).max(index);
    }

    fn record_execution_history(&mut self, node_id: NodeId) {
        self.refresh_execution_epoch(node_id);

        if self.execution_instrumentation_error(node_id).is_some() {
            return;
        }
        let Some(cursor) = self.execution_cursors.get(&node_id).cloned() else {
            return;
        };
        let applied_through = self.local_applied_index(node_id);
        if applied_through <= cursor.applied_through {
            return;
        }

        let first_index = cursor.applied_through.next();
        let entries = self.log_entries_from(node_id, first_index);
        let required = applied_through.0 - cursor.applied_through.0;
        let Ok(required_entries) = usize::try_from(required) else {
            return;
        };

        let application_epoch = self.application_epoch(node_id);
        let commit_index_at_emit = self.commit_index(node_id);
        let mut state = cursor.state;
        for (index, entry) in
            (first_index.0..=applied_through.0).zip(entries.into_iter().take(required_entries))
        {
            let executed = ExecutedLogEntry {
                index: LogIndex(index),
                term: entry.term,
                kind: entry.kind,
            };
            let emitted_application_payload =
                matches!(&executed.kind, LogEntryKind::Application(_))
                    .then(|| {
                        self.applied
                            .iter()
                            .find(|applied| {
                                applied.node_id == node_id
                                    && applied.application_epoch == application_epoch
                                    && applied.index == executed.index
                            })
                            .map(|applied| applied.payload.clone())
                    })
                    .flatten();
            let prior_state = state;
            let resulting_state = Self::apply_reference_transition(
                &prior_state,
                &executed,
                emitted_application_payload.as_ref(),
            );
            self.execution_history.push(ExecutionWitness {
                node_id,
                application_epoch,
                commit_index_at_emit,
                entry: executed,
                emitted_application_payload,
                prior_state: prior_state.clone(),
                resulting_state: resulting_state.clone(),
            });
            state = resulting_state;
        }

        self.execution_cursors.insert(
            node_id,
            ExecutionCursor {
                application_epoch,
                applied_through,
                state,
            },
        );
        self.record_durable_applied(node_id, applied_through);
    }
}

#[cfg(test)]
#[path = "delivery_read_tests.rs"]
mod read_tests;

#[cfg(test)]
#[path = "delivery_execution_tests.rs"]
mod tests;
