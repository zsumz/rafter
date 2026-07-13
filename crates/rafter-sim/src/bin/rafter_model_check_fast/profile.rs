use std::{error::Error, fmt};

use rafter_sim::SimSeed;

#[path = "profile/cli.rs"]
mod cli;
#[path = "profile/soak_profile.rs"]
mod soak_profile;

pub(crate) use cli::parse_profile;
pub(crate) use soak_profile::{SoakCheckKind, SoakExecutionContract, SoakProfile};

pub(crate) const SCHEDULE_CLASSES: &str =
    "proposal, failover, snapshot_transfer, restart, application_loss_restart, drop, delay, duplicate, reorder, partition, lossy_restart, tick_skew, read_index, transfer, membership_change";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Profile {
    Fast,
    RaftDeep,
    RaftSoak,
    RaftNightly,
    RaftWeekly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExhaustiveTargets {
    pub(crate) protocol_states: usize,
    pub(crate) verifier_states: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LocalModelBounds {
    pub(crate) election_depth: usize,
    pub(crate) commit_depth: usize,
    pub(crate) commit_proposals: usize,
    pub(crate) commit_production_depth: usize,
    pub(crate) membership_depth: usize,
    pub(crate) membership_changes: usize,
    pub(crate) membership_restart_snapshot_depth: usize,
    pub(crate) membership_restart_snapshot_restarts: usize,
    pub(crate) seeded_commit_depth: usize,
    pub(crate) seeded_commit_restarts: usize,
    pub(crate) leadership_noop_depth: usize,
    pub(crate) restart_snapshot_depth: usize,
    pub(crate) restart_snapshot_restarts: usize,
    pub(crate) prevote_depth: usize,
    pub(crate) read_index_depth: usize,
    pub(crate) read_indexes: usize,
    pub(crate) lease_read_depth: usize,
    pub(crate) lease_read_indexes: usize,
}

const FAST_MODEL_BOUNDS: LocalModelBounds = LocalModelBounds {
    election_depth: 8,
    commit_depth: 9,
    commit_proposals: 2,
    commit_production_depth: 7,
    membership_depth: 6,
    membership_changes: 1,
    membership_restart_snapshot_depth: 8,
    membership_restart_snapshot_restarts: 1,
    seeded_commit_depth: 1,
    seeded_commit_restarts: 1,
    leadership_noop_depth: 8,
    restart_snapshot_depth: 10,
    restart_snapshot_restarts: 1,
    prevote_depth: 9,
    read_index_depth: 7,
    read_indexes: 1,
    lease_read_depth: 6,
    lease_read_indexes: 2,
};

const DEEP_MODEL_BOUNDS: LocalModelBounds = LocalModelBounds {
    election_depth: 8,
    commit_depth: 9,
    commit_proposals: 2,
    commit_production_depth: 8,
    membership_depth: 6,
    membership_changes: 2,
    membership_restart_snapshot_depth: 9,
    membership_restart_snapshot_restarts: 2,
    seeded_commit_depth: 2,
    seeded_commit_restarts: 1,
    leadership_noop_depth: 8,
    restart_snapshot_depth: 10,
    restart_snapshot_restarts: 2,
    prevote_depth: 9,
    read_index_depth: 7,
    read_indexes: 2,
    lease_read_depth: 7,
    lease_read_indexes: 2,
};

impl Profile {
    pub(crate) const ALL: [Self; 5] = [
        Self::Fast,
        Self::RaftDeep,
        Self::RaftSoak,
        Self::RaftNightly,
        Self::RaftWeekly,
    ];

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Fast => "fast",
            Self::RaftDeep => "raft-deep",
            Self::RaftSoak => "raft-soak",
            Self::RaftNightly => "raft-nightly",
            Self::RaftWeekly => "raft-weekly",
        }
    }

    pub(crate) const fn expected_runtime(self) -> &'static str {
        match self {
            Self::Fast => "per-commit",
            Self::RaftDeep | Self::RaftSoak => "local-minutes",
            Self::RaftNightly => "scheduled",
            Self::RaftWeekly => "weekly-scheduled",
        }
    }

    pub(crate) const fn exhaustive_targets(self) -> Option<ExhaustiveTargets> {
        match self {
            Self::RaftNightly => Some(ExhaustiveTargets {
                protocol_states: 100_000_000,
                verifier_states: 100_000_000,
            }),
            Self::RaftWeekly => Some(ExhaustiveTargets {
                protocol_states: 250_000_000,
                verifier_states: 250_000_000,
            }),
            Self::Fast | Self::RaftDeep | Self::RaftSoak => None,
        }
    }

    pub(crate) const fn local_model_bounds(self) -> Option<LocalModelBounds> {
        match self {
            Self::Fast => Some(FAST_MODEL_BOUNDS),
            Self::RaftDeep => Some(DEEP_MODEL_BOUNDS),
            Self::RaftSoak | Self::RaftNightly | Self::RaftWeekly => None,
        }
    }

    pub(crate) fn bounds_summary(self) -> String {
        match self {
            Self::Fast => local_bounds_summary(FAST_MODEL_BOUNDS, true),
            Self::RaftDeep => local_bounds_summary(DEEP_MODEL_BOUNDS, false),
            Self::RaftSoak => {
                "soak-only: 4 curated seeds x 320 steps + lease + membership".to_string()
            }
            Self::RaftNightly => "election=8, commit=11x3+window1+prevote+prod8, membership=7x3+joint_restart_snapshot, seeded=2, noop_seeded=8, restart=10/12, read=8, lease_read=7, per-check wall_clock=20m unique_cap=120M, soak=6 fresh seeds x1024 + lease + membership (--seed replays)".to_string(),
            Self::RaftWeekly => "election=9, commit=11x4+window1+prevote+checkquorum+prod9, membership=8x4[minimal+prevote+checkquorum]+joint_restart_snapshot, seeded=3, noop_seeded=8, restart=11/12, prevote=8, read=8, lease_read=8, per-check wall_clock=60m unique_cap=300M, soak=10 fresh seeds x4096 + lease + membership (--seed replays)".to_string(),
        }
    }
}

fn local_bounds_summary(bounds: LocalModelBounds, semantic_witnesses: bool) -> String {
    let semantic_witnesses = if semantic_witnesses {
        ", semantic_witnesses=bounded"
    } else {
        ""
    };
    format!(
        "election={}, commit={}x{}+prod{}, membership={}x{}+joint_restart_snapshot, seeded={}, noop_seeded={}, restart={}/12, prevote={}{}, read={}, lease_read={}",
        bounds.election_depth,
        bounds.commit_depth,
        bounds.commit_proposals,
        bounds.commit_production_depth,
        bounds.membership_depth,
        bounds.membership_changes,
        bounds.seeded_commit_depth,
        bounds.leadership_noop_depth,
        bounds.restart_snapshot_depth,
        bounds.prevote_depth,
        semantic_witnesses,
        bounds.read_index_depth,
        bounds.lease_read_depth,
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProfileSelection {
    Run(ProfileRun),
    List,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProfileRun {
    pub(crate) profile: Profile,
    pub(crate) seed_override: Option<Vec<SimSeed>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CliError(String);

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for CliError {}

pub(crate) fn print_profiles() {
    for profile in Profile::ALL {
        let targets = profile.exhaustive_targets();
        let protocol_target = targets.map_or_else(
            || "none".to_string(),
            |targets| targets.protocol_states.to_string(),
        );
        let verifier_target = targets.map_or_else(
            || "none".to_string(),
            |targets| targets.verifier_states.to_string(),
        );
        println!(
            "{} expected_runtime={} exhaustive_target_protocol_states={} exhaustive_target_verifier_states={} bounds=\"{}\" schedule_classes={}",
            profile.name(),
            profile.expected_runtime(),
            protocol_target,
            verifier_target,
            profile.bounds_summary(),
            SCHEDULE_CLASSES
        );
    }
}
