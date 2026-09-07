//! Read-barrier registration and completion for the client history.
//!
//! A registered read completes only once its granting node has applied through
//! the granted read index in the same application epoch; a grant that outlives
//! its epoch is recorded as an instrumentation error rather than a result.

#[cfg(test)]
use rafter::SharedPayload;

use crate::records::ReadTerminalOutput;

use super::super::ExplorationState;
use super::{register_value_at, ClientRead, ClientReadOutcome, ClientReadProof};

impl ExplorationState {
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
