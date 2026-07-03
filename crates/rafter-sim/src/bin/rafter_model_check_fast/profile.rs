use std::{error::Error, fmt};

use rafter_sim::SimSeed;

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

    pub(crate) const fn exhaustive_target_unique_states(self) -> Option<usize> {
        match self {
            Self::RaftNightly => Some(100_000_000),
            Self::RaftWeekly => Some(250_000_000),
            Self::Fast | Self::RaftDeep | Self::RaftSoak => None,
        }
    }

    pub(crate) const fn bounds_summary(self) -> &'static str {
        match self {
            Self::Fast => {
                "election=7, commit=8x2+prod7, membership=5x1+joint_restart_snapshot, seeded=1, noop_seeded=8, restart=8/12, prevote=7, read=6, lease_read=6"
            }
            Self::RaftDeep => {
                "election=7, commit=9x2+prod8, membership=6x2+joint_restart_snapshot, seeded=2, noop_seeded=8, restart=9/12, prevote=7, read=7, lease_read=7"
            }
            Self::RaftSoak => "soak-only: 4 curated seeds x 320 steps + lease + membership",
            Self::RaftNightly => {
                "election=8, commit=11x3+prod8, membership=7x3+joint_restart_snapshot, seeded=2, noop_seeded=8, restart=10/12, read=8, lease_read=7, per-check wall_clock=20m unique_cap=120M, soak=6 fresh seeds x1024 + lease + membership (--seed replays)"
            }
            Self::RaftWeekly => {
                "election=9, commit=11x4+prod9, membership=8x4+joint_restart_snapshot, seeded=3, noop_seeded=8, restart=11/12, prevote=8, read=8, lease_read=8, per-check wall_clock=60m unique_cap=300M, soak=10 fresh seeds x4096 + lease + membership (--seed replays)"
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

pub(crate) fn parse_profile<I>(args: I) -> Result<ProfileSelection, CliError>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    let mut profile = None;
    let mut seeds = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--list-profiles" if args.len() == 1 => return Ok(ProfileSelection::List),
            "--list-profiles" => {
                return Err(CliError(
                    "`--list-profiles` cannot be combined with other arguments".to_string(),
                ));
            }
            "--profile" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(usage());
                };
                if profile.replace(profile_by_name(value)?).is_some() {
                    return Err(CliError(
                        "model-check profile specified more than once".to_string(),
                    ));
                }
            }
            "--seed" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(usage());
                };
                seeds.extend(parse_seed_list(value)?);
            }
            value if !value.starts_with('-') => {
                if profile.replace(profile_by_name(value)?).is_some() {
                    return Err(CliError(
                        "model-check profile specified more than once".to_string(),
                    ));
                }
            }
            _ => return Err(usage()),
        }
        index += 1;
    }
    let profile = profile.unwrap_or(Profile::Fast);
    let seed_override = (!seeds.is_empty()).then_some(seeds);
    if seed_override.is_some() && profile == Profile::Fast {
        return Err(CliError(
            "`--seed` only applies to profiles with soak workloads".to_string(),
        ));
    }
    Ok(ProfileSelection::Run(ProfileRun {
        profile,
        seed_override,
    }))
}

fn profile_by_name(value: &str) -> Result<Profile, CliError> {
    match value {
        "fast" => Ok(Profile::Fast),
        "raft-deep" => Ok(Profile::RaftDeep),
        "raft-soak" => Ok(Profile::RaftSoak),
        "raft-nightly" => Ok(Profile::RaftNightly),
        "raft-weekly" => Ok(Profile::RaftWeekly),
        _ => Err(CliError(format!("unknown model-check profile `{value}`"))),
    }
}

fn parse_seed_list(value: &str) -> Result<Vec<SimSeed>, CliError> {
    value.split(',').map(parse_seed).collect()
}

fn parse_seed(value: &str) -> Result<SimSeed, CliError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CliError(
            "model-check seed list contains an empty seed".to_string(),
        ));
    }
    let parsed = if let Some(hex) = value.strip_prefix("0x") {
        u64::from_str_radix(hex, 16)
    } else {
        value.parse()
    }
    .map_err(|_| CliError(format!("invalid model-check seed `{value}`")))?;
    Ok(SimSeed(parsed))
}

fn usage() -> CliError {
    CliError(
        "usage: rafter-model-check-fast [--list-profiles | [--profile] <fast|raft-deep|raft-soak|raft-nightly|raft-weekly> [--seed <seed>[,<seed>...]]]"
            .to_string(),
    )
}

pub(crate) fn print_profiles() {
    for profile in Profile::ALL {
        let target = profile
            .exhaustive_target_unique_states()
            .map_or_else(|| "none".to_string(), |states| states.to_string());
        println!(
            "{} expected_runtime={} exhaustive_target_unique_states={} bounds=\"{}\" schedule_classes={}",
            profile.name(),
            profile.expected_runtime(),
            target,
            profile.bounds_summary(),
            SCHEDULE_CLASSES
        );
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
