//! Read-index registration correlation.
//!
//! A caller-visible `ReadId` may be reused, so grants and terminal outputs are
//! bound to the simulator-local registration generation instead. Restarts
//! retire the generations they discard so later reuse cannot match them.

use rafter::NodeId;

use crate::Cluster;

impl Cluster {
    pub(super) fn pending_read_operation_id(
        &self,
        node_id: NodeId,
        request_id: u64,
    ) -> Option<u64> {
        self.read_registrations
            .iter()
            .filter(|registration| {
                registration.node_id == node_id && registration.request_id == request_id
            })
            .map(|registration| registration.operation_id)
            .find(|operation_id| {
                !self.retired_read_operations.contains(operation_id)
                    && !self
                        .read_grants
                        .iter()
                        .any(|grant| grant.operation_id == Some(*operation_id))
                    && !self
                        .read_terminal_outputs
                        .iter()
                        .any(|terminal| terminal.operation_id() == Some(*operation_id))
            })
    }

    pub(crate) fn retire_pending_reads(&mut self, node_id: NodeId) {
        let pending = self
            .read_registrations
            .iter()
            .filter(|registration| registration.node_id == node_id)
            .map(|registration| registration.operation_id)
            .filter(|operation_id| {
                !self.retired_read_operations.contains(operation_id)
                    && !self
                        .read_grants
                        .iter()
                        .any(|grant| grant.operation_id == Some(*operation_id))
                    && !self
                        .read_terminal_outputs
                        .iter()
                        .any(|terminal| terminal.operation_id() == Some(*operation_id))
            })
            .collect::<Vec<_>>();
        self.retired_read_operations.extend(pending);
    }

    pub(super) fn record_read_output_correlation(
        &mut self,
        node_id: NodeId,
        request_id: u64,
        operation_id: Option<u64>,
        output: &str,
    ) {
        if operation_id.is_none() {
            self.read_output_correlation_errors.insert(format!(
                "{node_id} emitted read {output} for uncorrelated request {request_id}"
            ));
        }
    }
}
