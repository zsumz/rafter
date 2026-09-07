//! Deterministic projections of live cluster state.
//!
//! Bounded explorers fingerprint clusters through these two views: the
//! protocol-state hash that defines model-check state identity, and the
//! pre-transition snapshot invariant observers compare against.

use std::hash::{Hash, Hasher};

use crate::{records::ExecutionLedger, Cluster};

impl Cluster {
    /// Hashes the live protocol and simulator-operational state.
    ///
    /// Append-only recorder histories are verifier evidence rather than inputs
    /// to future protocol transitions, so they are deliberately excluded. Live
    /// Durability, restart, network, and snapshot state remains part of the
    /// projection because it can change later simulator behavior.
    pub(crate) fn hash_protocol_state<H: Hasher>(&self, state: &mut H) {
        let Self {
            clock,
            configs,
            nodes,
            network,
            rng,
            applied: _,
            execution_history: _,
            execution_cursors: _,
            initial_reference_states: _,
            application_epochs: _,
            application_epoch_start_floors: _,
            durable_applied,
            snapshot_installs: _,
            snapshot_sources,
            snapshot_staging,
            read_grants: _,
            read_registrations: _,
            read_terminal_outputs: _,
            retired_read_operations: _,
            read_output_correlation_errors: _,
            proposal_rejections: _,
            transfer_rejections: _,
            blocked_pairs,
            delivered_ack_floor,
            synced_marks,
        } = self;

        clock.hash(state);
        configs.hash(state);
        nodes.hash(state);
        network.hash(state);
        rng.hash(state);
        durable_applied.hash(state);
        snapshot_sources.hash(state);
        snapshot_staging.hash(state);
        blocked_pairs.hash(state);
        delivered_ack_floor.hash(state);
        synced_marks.hash(state);
    }

    /// Captures the pre-transition state consumed by invariant observers.
    ///
    /// Execution witnesses are immutable append-only evidence, and no
    /// transition observer reads the old ledger. Omitting its payload-rich
    /// prefix keeps per-step observation cost independent of retained history.
    pub(crate) fn transition_observation_snapshot(&self) -> Self {
        Self {
            clock: self.clock.clone(),
            configs: self.configs.clone(),
            nodes: self.nodes.clone(),
            network: self.network.clone(),
            rng: self.rng.clone(),
            applied: self.applied.clone(),
            execution_history: ExecutionLedger::default(),
            execution_cursors: self.execution_cursors.clone(),
            initial_reference_states: self.initial_reference_states.clone(),
            application_epochs: self.application_epochs.clone(),
            application_epoch_start_floors: self.application_epoch_start_floors.clone(),
            durable_applied: self.durable_applied.clone(),
            snapshot_installs: self.snapshot_installs.clone(),
            snapshot_sources: self.snapshot_sources.clone(),
            snapshot_staging: self.snapshot_staging.clone(),
            read_grants: self.read_grants.clone(),
            read_registrations: self.read_registrations.clone(),
            read_terminal_outputs: self.read_terminal_outputs.clone(),
            retired_read_operations: self.retired_read_operations.clone(),
            read_output_correlation_errors: self.read_output_correlation_errors.clone(),
            proposal_rejections: self.proposal_rejections.clone(),
            transfer_rejections: self.transfer_rejections.clone(),
            blocked_pairs: self.blocked_pairs.clone(),
            delivered_ack_floor: self.delivered_ack_floor.clone(),
            synced_marks: self.synced_marks.clone(),
        }
    }
}
