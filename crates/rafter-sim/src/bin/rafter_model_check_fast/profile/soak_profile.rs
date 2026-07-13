use rafter::{NodeConfig, NodeId};
use rafter_sim::{model_check::SoakConfig, SimSeed};

pub(crate) const SOAK_EXECUTION_CONTRACT_ID: &str = "rafter-soak-execution-v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SoakCheckKind {
    Standard,
    Lease,
    Membership,
}

impl SoakCheckKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Lease => "lease",
            Self::Membership => "membership",
        }
    }

    const fn suffix(self) -> &'static str {
        match self {
            Self::Standard => "",
            Self::Lease => "-lease",
            Self::Membership => "-membership",
        }
    }

    const fn node_config_id(self) -> &'static str {
        match self {
            Self::Standard => "three-node-standard-v1",
            Self::Lease => "three-node-lease-v1",
            Self::Membership => "four-node-future-learner-v1",
        }
    }

    const fn node_count(self) -> usize {
        match self {
            Self::Standard | Self::Lease => 3,
            Self::Membership => 4,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SoakExecutionContract {
    pub(crate) contract_id: &'static str,
    pub(crate) profile_id: &'static str,
    pub(crate) check_id: String,
    pub(crate) check_kind: SoakCheckKind,
    pub(crate) node_config_id: &'static str,
    pub(crate) node_count: usize,
    pub(crate) steps: usize,
    pub(crate) max_proposals: usize,
    pub(crate) max_restarts: usize,
    pub(crate) max_read_indexes: usize,
    pub(crate) max_membership_changes: usize,
    pub(crate) max_transfers: usize,
    pub(crate) max_partitions: usize,
    pub(crate) max_lossy_restarts: usize,
    pub(crate) snapshot_catchup_probe: bool,
    pub(crate) tick_skew_node_id: Option<NodeId>,
    pub(crate) tick_skew_weight: Option<u32>,
}

impl SoakExecutionContract {
    pub(crate) fn from_config(
        profile_id: &'static str,
        check_id: &str,
        kind: SoakCheckKind,
        config: SoakConfig,
    ) -> Self {
        let parameters = config.execution_parameters();
        let (tick_skew_node_id, tick_skew_weight) = parameters
            .tick_skew
            .map_or((None, None), |(node_id, weight)| {
                (Some(node_id), Some(weight))
            });
        Self {
            contract_id: SOAK_EXECUTION_CONTRACT_ID,
            profile_id,
            check_id: check_id.to_owned(),
            check_kind: kind,
            node_config_id: kind.node_config_id(),
            node_count: kind.node_count(),
            steps: parameters.steps,
            max_proposals: parameters.max_proposals,
            max_restarts: parameters.max_restarts,
            max_read_indexes: parameters.max_read_indexes,
            max_membership_changes: parameters.max_membership_changes,
            max_transfers: parameters.max_transfers,
            max_partitions: parameters.max_partitions,
            max_lossy_restarts: parameters.max_lossy_restarts,
            snapshot_catchup_probe: parameters.snapshot_catchup_probe,
            tick_skew_node_id,
            tick_skew_weight,
        }
    }

    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "contract_id": self.contract_id,
            "profile_id": self.profile_id,
            "check_id": self.check_id,
            "check_kind": self.check_kind.as_str(),
            "node_config_id": self.node_config_id,
            "node_count": self.node_count,
            "steps": self.steps,
            "max_proposals": self.max_proposals,
            "max_restarts": self.max_restarts,
            "max_read_indexes": self.max_read_indexes,
            "max_membership_changes": self.max_membership_changes,
            "max_transfers": self.max_transfers,
            "max_partitions": self.max_partitions,
            "max_lossy_restarts": self.max_lossy_restarts,
            "snapshot_catchup_probe": self.snapshot_catchup_probe,
            "tick_skew_node_id": self.tick_skew_node_id.map(|node_id| node_id.0),
            "tick_skew_weight": self.tick_skew_weight,
        })
    }

    pub(crate) fn validate_node_configs(&self, configs: &[NodeConfig]) -> Result<(), String> {
        if configs.len() == self.node_count {
            Ok(())
        } else {
            Err(format!(
                "soak execution contract {} expects {} nodes, found {}",
                self.check_id,
                self.node_count,
                configs.len()
            ))
        }
    }

    pub(crate) fn validate_config(&self, config: SoakConfig) -> Result<(), String> {
        let observed = Self::from_config(self.profile_id, &self.check_id, self.check_kind, config);
        if observed == *self {
            Ok(())
        } else {
            Err(format!(
                "soak execution contract {} does not match the actual SoakConfig",
                self.check_id
            ))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SoakProfile {
    pub(crate) name: &'static str,
    pub(crate) seeds: &'static [SimSeed],
    pub(crate) steps: usize,
    pub(crate) max_proposals: usize,
    pub(crate) max_restarts: usize,
    pub(crate) max_read_indexes: usize,
    pub(crate) max_membership_changes: usize,
    pub(crate) max_transfers: usize,
    pub(crate) max_partitions: usize,
    pub(crate) max_lossy_restarts: usize,
    /// Tick-skew weight for node 1 (one = no skew).
    pub(crate) tick_skew_weight: u32,
}

impl SoakProfile {
    pub(crate) fn execution_contract(self, kind: SoakCheckKind) -> SoakExecutionContract {
        SoakExecutionContract {
            contract_id: SOAK_EXECUTION_CONTRACT_ID,
            profile_id: self.name,
            check_id: format!("{}{}", self.name, kind.suffix()),
            check_kind: kind,
            node_config_id: kind.node_config_id(),
            node_count: kind.node_count(),
            steps: self.steps,
            max_proposals: self.max_proposals,
            max_restarts: self.max_restarts,
            max_read_indexes: self.max_read_indexes,
            max_membership_changes: self.max_membership_changes,
            max_transfers: self.max_transfers,
            max_partitions: self.max_partitions,
            max_lossy_restarts: self.max_lossy_restarts,
            snapshot_catchup_probe: true,
            tick_skew_node_id: Some(NodeId(1)),
            tick_skew_weight: Some(self.tick_skew_weight),
        }
    }

    pub(crate) const fn soak_config(self, seed: SimSeed) -> SoakConfig {
        SoakConfig::new(seed, self.steps)
            .with_max_proposals(self.max_proposals)
            .with_max_restarts(self.max_restarts)
            .with_max_read_indexes(self.max_read_indexes)
            .with_max_membership_changes(self.max_membership_changes)
            .with_max_transfers(self.max_transfers)
            .with_max_partitions(self.max_partitions)
            .with_max_lossy_restarts(self.max_lossy_restarts)
            .with_snapshot_catchup_probe()
            .with_tick_skew(NodeId(1), self.tick_skew_weight)
    }

    pub(crate) const fn raft_deep() -> Self {
        Self {
            name: "raft-deep-soak",
            seeds: &[SimSeed(0x9103), SimSeed(0x9104)],
            steps: 160,
            max_proposals: 12,
            max_restarts: 6,
            max_read_indexes: 4,
            max_membership_changes: 4,
            max_transfers: 2,
            max_partitions: 2,
            max_lossy_restarts: 2,
            tick_skew_weight: 3,
        }
    }

    pub(crate) const fn raft_soak() -> Self {
        Self {
            name: "raft-soak",
            seeds: &[
                SimSeed(0x9103),
                SimSeed(0x9104),
                SimSeed(0x9105),
                SimSeed(0x9106),
            ],
            steps: 320,
            max_proposals: 24,
            max_restarts: 12,
            max_read_indexes: 4,
            max_membership_changes: 8,
            max_transfers: 2,
            max_partitions: 2,
            max_lossy_restarts: 2,
            tick_skew_weight: 3,
        }
    }

    pub(crate) const fn raft_nightly() -> Self {
        Self {
            name: "raft-nightly-soak",
            seeds: &[
                SimSeed(0x9103_0001),
                SimSeed(0x9103_0002),
                SimSeed(0x9103_0003),
                SimSeed(0x9103_0004),
                SimSeed(0x9103_0005),
                SimSeed(0x9103_0006),
            ],
            steps: 1024,
            max_proposals: 64,
            max_restarts: 32,
            max_read_indexes: 4,
            max_membership_changes: 16,
            max_transfers: 2,
            max_partitions: 2,
            max_lossy_restarts: 2,
            tick_skew_weight: 3,
        }
    }

    pub(crate) const fn raft_weekly() -> Self {
        Self {
            name: "raft-weekly-soak",
            seeds: &[
                SimSeed(0x9203_0001),
                SimSeed(0x9203_0002),
                SimSeed(0x9203_0003),
                SimSeed(0x9203_0004),
                SimSeed(0x9203_0005),
                SimSeed(0x9203_0006),
                SimSeed(0x9203_0007),
                SimSeed(0x9203_0008),
                SimSeed(0x9203_0009),
                SimSeed(0x9203_000a),
            ],
            steps: 4096,
            max_proposals: 192,
            max_restarts: 96,
            max_read_indexes: 16,
            max_membership_changes: 48,
            max_transfers: 8,
            max_partitions: 8,
            max_lossy_restarts: 8,
            tick_skew_weight: 5,
        }
    }
}
