use std::{error::Error, fmt};

use rafter_sim::SimSeed;

#[path = "profile/cli.rs"]
mod cli;
#[path = "profile/soak_profile.rs"]
mod soak_profile;

pub(crate) use cli::parse_profile;
pub(crate) use soak_profile::{SoakCheckKind, SoakExecutionContract, SoakProfile};

pub(crate) const SCHEDULE_CLASSES: &str =
    "proposal, failover, snapshot_transfer, restart, drop, delay, duplicate, reorder, partition, lossy_restart, tick_skew, read_index, transfer, membership_change";

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

    pub(crate) const fn bounds_summary(self) -> &'static str {
        match self {
            Self::Fast => {
                "election=7, commit=8x2+prod7, membership=5x1+joint_restart_snapshot, seeded=1, noop_seeded=8, restart=8/12, prevote=7, semantic_witnesses=bounded, read=6, lease_read=6"
            }
            Self::RaftDeep => {
                "election=7, commit=9x2+prod8, membership=6x2+joint_restart_snapshot, seeded=2, noop_seeded=8, restart=9/12, prevote=7, read=7, lease_read=7"
            }
            Self::RaftSoak => "soak-only: 4 curated seeds x 320 steps + lease + membership",
            Self::RaftNightly => {
                "election=8, commit=11x3+window1+prevote+prod8, membership=7x3+joint_restart_snapshot, seeded=2, noop_seeded=8, restart=10/12, read=8, lease_read=7, per-check wall_clock=20m unique_cap=120M, soak=6 fresh seeds x1024 + lease + membership (--seed replays)"
            }
            Self::RaftWeekly => {
                "election=9, commit=11x4+window1+prevote+checkquorum+prod9, membership=8x4[minimal+prevote+checkquorum]+joint_restart_snapshot, seeded=3, noop_seeded=8, restart=11/12, prevote=8, read=8, lease_read=8, per-check wall_clock=60m unique_cap=300M, soak=10 fresh seeds x4096 + lease + membership (--seed replays)"
            }
        }
    }
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
