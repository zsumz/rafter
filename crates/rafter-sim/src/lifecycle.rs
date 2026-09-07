//! Cluster construction from static node configurations.
//!
//! Every simulated node starts from an empty reference application state, a
//! zeroed durable applied floor, and application epoch zero, so restart,
//! replay, and snapshot transitions all measure from the same canonical floor.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rafter::{InMemorySnapshotChunkSource, LogIndex, Node, NodeConfig, NodeId};

use crate::{
    records::{ExecutionCursor, ExecutionLedger},
    time::SimRng,
    Cluster, ReferenceState, SimClock, SimSeed,
};

impl Cluster {
    /// Builds a cluster with the default deterministic seed.
    #[must_use]
    pub fn new(configs: Vec<NodeConfig>) -> Self {
        Self::new_with_seed(configs, SimSeed::default())
    }

    /// Builds a cluster with an explicit deterministic seed.
    #[must_use]
    pub fn new_with_seed(configs: Vec<NodeConfig>, seed: SimSeed) -> Self {
        let configs_by_id = configs
            .iter()
            .cloned()
            .map(|config| (config.id(), config))
            .collect();
        let nodes: BTreeMap<NodeId, Node> = configs
            .into_iter()
            .map(|config| (config.id(), Node::new(config)))
            .collect();
        let snapshot_sources = nodes
            .keys()
            .map(|node_id| (*node_id, InMemorySnapshotChunkSource::new()))
            .collect();
        let durable_applied = nodes
            .keys()
            .map(|node_id| (*node_id, LogIndex::ZERO))
            .collect();
        let application_epochs = nodes.keys().map(|node_id| (*node_id, 0)).collect();
        let application_epoch_start_floors = nodes
            .keys()
            .map(|node_id| ((*node_id, 0), LogIndex::ZERO))
            .collect();
        let initial_reference_states: BTreeMap<_, _> = nodes
            .iter()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    ReferenceState {
                        application_value: Vec::new().into(),
                        committed_membership: node.committed_membership(),
                        committed_configuration: None,
                    },
                )
            })
            .collect();
        let execution_cursors = initial_reference_states
            .iter()
            .map(|(node_id, state)| {
                (
                    *node_id,
                    ExecutionCursor {
                        application_epoch: 0,
                        applied_through: LogIndex::ZERO,
                        state: state.clone(),
                    },
                )
            })
            .collect();

        Self {
            clock: SimClock::default(),
            configs: configs_by_id,
            nodes,
            network: VecDeque::new(),
            rng: SimRng::new(seed),
            applied: Vec::new(),
            execution_history: ExecutionLedger::default(),
            execution_cursors,
            initial_reference_states,
            application_epochs,
            application_epoch_start_floors,
            durable_applied,
            snapshot_installs: Vec::new(),
            snapshot_sources,
            snapshot_staging: BTreeMap::new(),
            read_grants: Vec::new(),
            read_terminal_outputs: Vec::new(),
            retired_read_operations: BTreeSet::new(),
            read_output_correlation_errors: BTreeSet::new(),
            proposal_rejections: Vec::new(),
            transfer_rejections: Vec::new(),
            blocked_pairs: BTreeSet::new(),
            delivered_ack_floor: BTreeMap::new(),
            synced_marks: BTreeMap::new(),
            read_registrations: Vec::new(),
        }
    }
}
