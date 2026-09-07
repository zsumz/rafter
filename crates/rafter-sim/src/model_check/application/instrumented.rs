//! Mutating and inspection capabilities of the instrumented cluster wrapper.
//!
//! Production code reaches the wrapped cluster only through `Deref`, so every
//! mutation is either an observed transition or an explicitly test-gated
//! fixture hook.

use rafter::NodeId;

use crate::Cluster;

use super::InstrumentedCluster;

impl InstrumentedCluster {
    pub(in crate::model_check::state) const fn new(cluster: Cluster) -> Self {
        Self(cluster)
    }

    pub(super) fn initialize_snapshot_seed_epoch_floor(
        &mut self,
        node_id: NodeId,
        snapshot_boundary: rafter::LogIndex,
    ) {
        let application_epoch = self.application_epoch(node_id);
        let has_epoch_history = self.0.applied.iter().any(|applied| {
            applied.node_id == node_id && applied.application_epoch == application_epoch
        }) || self.0.execution_history.as_slice().iter().any(|witness| {
            witness.node_id == node_id && witness.application_epoch == application_epoch
        }) || self.0.snapshot_installs.iter().any(|install| {
            install.node_id == node_id && install.application_epoch == application_epoch
        });
        if has_epoch_history {
            return;
        }
        let floor = self
            .0
            .application_epoch_start_floors
            .entry((node_id, application_epoch))
            .or_default();
        *floor = (*floor).max(snapshot_boundary);
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn inject_applied_record(&mut self, applied: crate::Applied) {
        self.0.applied.push(applied);
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn clear_execution_cursors(&mut self) {
        self.0.execution_cursors.clear();
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn clear_initial_reference_states(&mut self) {
        self.0.initial_reference_states.clear();
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn clear_application_epochs(&mut self) {
        self.0.application_epochs.clear();
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn remove_execution_cursor(
        &mut self,
        node_id: rafter::NodeId,
    ) {
        self.0.execution_cursors.remove(&node_id);
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn inject_read_grant(&mut self, grant: crate::ReadGranted) {
        self.0.read_grants.push(grant);
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn inject_read_terminal_output(
        &mut self,
        output: crate::ReadTerminalOutput,
    ) {
        self.0.read_terminal_outputs.push(output);
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn inject_blocked_pair(
        &mut self,
        from: rafter::NodeId,
        to: rafter::NodeId,
    ) {
        self.0.blocked_pairs.insert((from, to));
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn restart_node_from_bootstrap(
        &mut self,
        node_id: rafter::NodeId,
        bootstrap: rafter::BootstrapState,
    ) -> Result<(), rafter::BootstrapValidationError> {
        self.0.restart_node_from_bootstrap(node_id, bootstrap)
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn seed_snapshot_payload(
        &mut self,
        node_id: rafter::NodeId,
        snapshot: &rafter::RaftSnapshot,
        payload: Vec<u8>,
    ) {
        self.0.seed_snapshot_payload(node_id, snapshot, payload);
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn queue_message(
        &mut self,
        from: rafter::NodeId,
        to: rafter::NodeId,
        message: rafter::Message,
    ) {
        self.0.queue_message(from, to, message);
    }

    #[cfg(test)]
    pub(in crate::model_check::state) fn drop_matching(
        &mut self,
        predicate: impl FnMut(&crate::Envelope) -> bool,
    ) -> usize {
        self.0.drop_matching(predicate)
    }
}
