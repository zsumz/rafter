use std::{
    error::Error,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rafter::NodeConfig;
use rafter_sim::model_check::{
    check_raft_commit_safety, check_raft_election_safety,
    check_raft_joint_membership_restart_and_snapshot_safety, check_raft_leadership_noop_safety,
    check_raft_membership_safety, check_raft_read_index_safety,
    check_raft_restart_and_snapshot_safety, check_raft_seeded_commit_safety, run_raft_random_soak,
    Bounds, Failure, SoakConfig, Summary,
};
use rafter_sim::SimSeed;

use super::profile::{Profile, ProfileRun, SoakProfile, SCHEDULE_CLASSES};
use super::raft_config::{
    four_node_future_learner_configs, three_node_configs, three_node_configs_with_inflight_window,
    three_node_lease_configs, three_node_pre_vote_configs, three_node_production_configs,
};
use super::reporting::{
    print_raft_failure, print_raft_summary, print_soak_failure, print_soak_summary,
};

const NIGHTLY_EXHAUSTIVE_MAX_WALL_CLOCK: Duration = Duration::from_secs(1_200);
const WEEKLY_EXHAUSTIVE_MAX_WALL_CLOCK: Duration = Duration::from_secs(3_600);
const NIGHTLY_EXHAUSTIVE_MAX_UNIQUE_STATES: usize = 120_000_000;
const WEEKLY_EXHAUSTIVE_MAX_UNIQUE_STATES: usize = 300_000_000;

pub(crate) fn run_profile(run: ProfileRun) -> Result<(), Box<dyn Error>> {
    let profile = run.profile;
    println!(
        "model-check profile={} expected_runtime={} exhaustive_target_unique_states={} bounds=\"{}\" schedule_classes={}",
        profile.name(),
        profile.expected_runtime(),
        profile
            .exhaustive_target_unique_states()
            .map_or_else(|| "none".to_string(), |states| states.to_string()),
        profile.bounds_summary(),
        SCHEDULE_CLASSES
    );

    match profile {
        Profile::Fast => run_fast_profile(),
        Profile::RaftDeep => run_raft_deep_profile(run.seed_override),
        Profile::RaftSoak => {
            let profile = SoakProfile::raft_soak();
            let (seeds, source) = fixed_or_override(profile.seeds, run.seed_override);
            run_raft_soak_profile(profile, &seeds, source)
        }
        Profile::RaftNightly => run_raft_nightly_profile(run.seed_override),
        Profile::RaftWeekly => run_raft_weekly_profile(run.seed_override),
    }
}

fn run_fast_profile() -> Result<(), Box<dyn Error>> {
    run_raft_check("raft-election", || {
        check_raft_election_safety(three_node_configs(2), Bounds::new(7))
    })?;
    // Both in-flight window regimes are explored explicitly. Two proposals
    // make the window bind: a window of one answers the second proposal
    // with an empty append until the first batch is acknowledged, while a
    // pipelined window streams the second batch immediately.
    run_raft_check("raft-commit", || {
        check_raft_commit_safety(
            three_node_configs_with_inflight_window(2, 3),
            Bounds::new(8).with_max_proposals(2),
        )
    })?;
    run_raft_check("raft-commit-window1", || {
        check_raft_commit_safety(
            three_node_configs_with_inflight_window(2, 1),
            Bounds::new(8).with_max_proposals(2),
        )
    })?;
    run_raft_check("raft-commit-production", || {
        check_raft_commit_safety(
            three_node_production_configs(3),
            Bounds::new(7).with_max_proposals(1),
        )
    })?;
    run_raft_check("raft-membership", || {
        check_raft_membership_safety(
            four_node_future_learner_configs(3),
            Bounds::new(5)
                .with_max_proposals(1)
                .with_max_membership_changes(1),
        )
    })?;
    run_raft_check("raft-membership-restart-snapshot", || {
        check_raft_joint_membership_restart_and_snapshot_safety(Bounds::new(8).with_max_restarts(1))
    })?;
    run_raft_check("raft-commit-seeded", || {
        check_raft_seeded_commit_safety(three_node_configs(2), Bounds::new(1))
    })?;
    run_raft_check("raft-leadership-noop-seeded", || {
        check_raft_leadership_noop_safety(Bounds::new(8))
    })?;
    run_raft_check("raft-restart-snapshot", || {
        check_raft_restart_and_snapshot_safety(Bounds::new(8).with_max_restarts(1))
    })?;
    run_raft_check("raft-election-prevote", || {
        check_raft_election_safety(three_node_pre_vote_configs(2), Bounds::new(7))
    })?;
    run_raft_check("raft-read-index", || {
        check_raft_read_index_safety(
            three_node_configs(2),
            Bounds::new(6)
                .with_max_proposals(1)
                .with_max_read_indexes(1),
        )
    })?;
    run_raft_check("raft-lease-read", || {
        check_raft_read_index_safety(
            three_node_lease_configs(8),
            Bounds::new(6)
                .with_max_proposals(1)
                .with_max_read_indexes(2),
        )
    })?;
    Ok(())
}

fn run_raft_deep_profile(seed_override: Option<Vec<SimSeed>>) -> Result<(), Box<dyn Error>> {
    run_raft_check("raft-election-deep", || {
        check_raft_election_safety(three_node_configs(2), Bounds::new(7))
    })?;
    run_raft_check("raft-commit-deep", || {
        check_raft_commit_safety(three_node_configs(2), Bounds::new(9).with_max_proposals(2))
    })?;
    run_raft_check("raft-commit-production-deep", || {
        check_raft_commit_safety(
            three_node_production_configs(3),
            Bounds::new(8).with_max_proposals(1),
        )
    })?;
    run_raft_check("raft-membership-deep", || {
        check_raft_membership_safety(
            four_node_future_learner_configs(3),
            Bounds::new(6)
                .with_max_proposals(1)
                .with_max_membership_changes(2),
        )
    })?;
    run_raft_check("raft-membership-restart-snapshot-deep", || {
        check_raft_joint_membership_restart_and_snapshot_safety(Bounds::new(9).with_max_restarts(2))
    })?;
    run_raft_check("raft-commit-seeded-deep", || {
        check_raft_seeded_commit_safety(three_node_configs(2), Bounds::new(2))
    })?;
    run_raft_check("raft-leadership-noop-seeded-deep", || {
        check_raft_leadership_noop_safety(Bounds::new(8))
    })?;
    run_raft_check("raft-restart-snapshot-deep", || {
        check_raft_restart_and_snapshot_safety(Bounds::new(9).with_max_restarts(2))
    })?;
    run_raft_check("raft-election-prevote-deep", || {
        check_raft_election_safety(three_node_pre_vote_configs(2), Bounds::new(7))
    })?;
    run_raft_check("raft-read-index-deep", || {
        check_raft_read_index_safety(
            three_node_configs(2),
            Bounds::new(7)
                .with_max_proposals(1)
                .with_max_read_indexes(2),
        )
    })?;
    run_raft_check("raft-lease-read-deep", || {
        check_raft_read_index_safety(
            three_node_lease_configs(8),
            Bounds::new(7)
                .with_max_proposals(1)
                .with_max_read_indexes(2),
        )
    })?;
    let profile = SoakProfile::raft_deep();
    let (seeds, source) = fixed_or_override(profile.seeds, seed_override);
    run_raft_soak_profile(profile, &seeds, source)
}

fn run_raft_nightly_profile(seed_override: Option<Vec<SimSeed>>) -> Result<(), Box<dyn Error>> {
    let scheduled = |bounds| scheduled_exhaustive_bounds(Profile::RaftNightly, bounds);
    let mut exhaustive_unique_states = 0;
    exhaustive_unique_states += run_raft_check("raft-election-nightly", || {
        check_raft_election_safety(three_node_configs(2), scheduled(Bounds::new(8)))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-commit-nightly", || {
        check_raft_commit_safety(
            three_node_configs(2),
            scheduled(Bounds::new(11).with_max_proposals(3)),
        )
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-commit-production-nightly", || {
        check_raft_commit_safety(
            three_node_production_configs(3),
            scheduled(Bounds::new(8).with_max_proposals(1)),
        )
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-membership-nightly", || {
        check_raft_membership_safety(
            four_node_future_learner_configs(3),
            scheduled(
                Bounds::new(7)
                    .with_max_proposals(2)
                    .with_max_membership_changes(3),
            ),
        )
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-membership-restart-snapshot-nightly", || {
        check_raft_joint_membership_restart_and_snapshot_safety(scheduled(
            Bounds::new(10).with_max_restarts(2),
        ))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-commit-seeded-nightly", || {
        check_raft_seeded_commit_safety(three_node_configs(2), scheduled(Bounds::new(2)))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-leadership-noop-seeded-nightly", || {
        check_raft_leadership_noop_safety(scheduled(Bounds::new(8)))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-restart-snapshot-nightly", || {
        check_raft_restart_and_snapshot_safety(scheduled(Bounds::new(10).with_max_restarts(2)))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-read-index-nightly", || {
        check_raft_read_index_safety(
            three_node_configs(2),
            scheduled(
                Bounds::new(8)
                    .with_max_proposals(1)
                    .with_max_read_indexes(2),
            ),
        )
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-lease-read-nightly", || {
        check_raft_read_index_safety(
            three_node_lease_configs(8),
            scheduled(
                Bounds::new(7)
                    .with_max_proposals(1)
                    .with_max_read_indexes(2),
            ),
        )
    })?
    .unique_states();
    assert_exhaustive_target(Profile::RaftNightly, exhaustive_unique_states)?;
    let profile = SoakProfile::raft_nightly();
    let (seeds, source) = scheduled_seeds(profile, seed_override);
    run_raft_soak_profile(profile, &seeds, source)
}

fn run_raft_weekly_profile(seed_override: Option<Vec<SimSeed>>) -> Result<(), Box<dyn Error>> {
    let scheduled = |bounds| scheduled_exhaustive_bounds(Profile::RaftWeekly, bounds);
    let mut exhaustive_unique_states = 0;
    exhaustive_unique_states += run_raft_check("raft-election-weekly", || {
        check_raft_election_safety(three_node_configs(2), scheduled(Bounds::new(9)))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-commit-weekly", || {
        check_raft_commit_safety(
            three_node_configs(2),
            scheduled(Bounds::new(11).with_max_proposals(4)),
        )
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-commit-production-weekly", || {
        check_raft_commit_safety(
            three_node_production_configs(3),
            scheduled(Bounds::new(9).with_max_proposals(1)),
        )
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-membership-weekly", || {
        check_raft_membership_safety(
            four_node_future_learner_configs(3),
            scheduled(
                Bounds::new(8)
                    .with_max_proposals(2)
                    .with_max_membership_changes(4),
            ),
        )
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-membership-restart-snapshot-weekly", || {
        check_raft_joint_membership_restart_and_snapshot_safety(scheduled(
            Bounds::new(11).with_max_restarts(3),
        ))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-commit-seeded-weekly", || {
        check_raft_seeded_commit_safety(three_node_configs(2), scheduled(Bounds::new(3)))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-leadership-noop-seeded-weekly", || {
        check_raft_leadership_noop_safety(scheduled(Bounds::new(8)))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-restart-snapshot-weekly", || {
        check_raft_restart_and_snapshot_safety(scheduled(Bounds::new(11).with_max_restarts(3)))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-election-prevote-weekly", || {
        check_raft_election_safety(three_node_pre_vote_configs(2), scheduled(Bounds::new(8)))
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-read-index-weekly", || {
        check_raft_read_index_safety(
            three_node_configs(2),
            scheduled(
                Bounds::new(8)
                    .with_max_proposals(2)
                    .with_max_read_indexes(3),
            ),
        )
    })?
    .unique_states();
    exhaustive_unique_states += run_raft_check("raft-lease-read-weekly", || {
        check_raft_read_index_safety(
            three_node_lease_configs(8),
            scheduled(
                Bounds::new(8)
                    .with_max_proposals(1)
                    .with_max_read_indexes(3),
            ),
        )
    })?
    .unique_states();
    assert_exhaustive_target(Profile::RaftWeekly, exhaustive_unique_states)?;
    let profile = SoakProfile::raft_weekly();
    let (seeds, source) = scheduled_seeds(profile, seed_override);
    run_raft_soak_profile(profile, &seeds, source)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SoakSeedSource {
    Curated,
    Fresh,
    Replay,
}

impl SoakSeedSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Curated => "curated",
            Self::Fresh => "fresh",
            Self::Replay => "replay",
        }
    }
}

fn run_raft_soak_profile(
    profile: SoakProfile,
    seeds: &[SimSeed],
    source: SoakSeedSource,
) -> Result<(), Box<dyn Error>> {
    println!(
        "model-check {} seeds source={} seeds={}",
        profile.name,
        source.as_str(),
        format_seed_list(seeds)
    );
    run_raft_soak_profile_for_configs(profile.name, profile, seeds, &three_node_configs(2))?;
    run_raft_soak_profile_for_configs(
        &format!("{}-lease", profile.name),
        profile,
        seeds,
        &three_node_lease_configs(8),
    )?;
    run_raft_soak_profile_for_configs(
        &format!("{}-membership", profile.name),
        profile,
        seeds,
        &four_node_future_learner_configs(3),
    )
}

fn run_raft_soak_profile_for_configs(
    name: &str,
    profile: SoakProfile,
    seeds: &[SimSeed],
    configs: &[NodeConfig],
) -> Result<(), Box<dyn Error>> {
    for seed in seeds.iter().copied() {
        let config = SoakConfig::new(seed, profile.steps)
            .with_max_proposals(profile.max_proposals)
            .with_max_restarts(profile.max_restarts)
            .with_max_read_indexes(profile.max_read_indexes)
            .with_max_membership_changes(profile.max_membership_changes)
            .with_max_transfers(profile.max_transfers)
            .with_max_partitions(profile.max_partitions)
            .with_max_lossy_restarts(profile.max_lossy_restarts)
            .with_tick_skew(rafter::NodeId(1), profile.tick_skew_weight);
        let started = Instant::now();
        let summary = run_raft_random_soak(configs.to_owned(), config).inspect_err(|failure| {
            print_soak_failure(name, failure);
        })?;
        print_soak_summary(name, &summary, started.elapsed());
    }
    Ok(())
}

fn fixed_or_override(
    fixed: &[SimSeed],
    seed_override: Option<Vec<SimSeed>>,
) -> (Vec<SimSeed>, SoakSeedSource) {
    match seed_override {
        Some(seeds) => (seeds, SoakSeedSource::Replay),
        None => (fixed.to_vec(), SoakSeedSource::Curated),
    }
}

fn scheduled_seeds(
    profile: SoakProfile,
    seed_override: Option<Vec<SimSeed>>,
) -> (Vec<SimSeed>, SoakSeedSource) {
    match seed_override {
        Some(seeds) => (seeds, SoakSeedSource::Replay),
        None => (
            fresh_soak_seeds(profile.name, profile.seeds.len()),
            SoakSeedSource::Fresh,
        ),
    }
}

fn fresh_soak_seeds(name: &str, count: usize) -> Vec<SimSeed> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let mut state = now.as_secs()
        ^ u64::from(now.subsec_nanos()).rotate_left(17)
        ^ u64::from(std::process::id()).rotate_left(31)
        ^ hash_name(name);
    (0..count)
        .map(|_| {
            state = splitmix64(state);
            SimSeed(state)
        })
        .collect()
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

fn hash_name(name: &str) -> u64 {
    name.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn format_seed_list(seeds: &[SimSeed]) -> String {
    seeds
        .iter()
        .map(|seed| format!("{:#x}", seed.0))
        .collect::<Vec<_>>()
        .join(",")
}

fn run_raft_check(
    name: &str,
    check: impl FnOnce() -> Result<Summary, Failure>,
) -> Result<Summary, Failure> {
    let started = Instant::now();
    let result = check();
    let duration = started.elapsed();
    let summary = result.inspect_err(|failure| {
        print_raft_failure(name, failure);
    })?;
    print_raft_summary(name, summary, duration);
    Ok(summary)
}

fn assert_exhaustive_target(profile: Profile, unique_states: usize) -> Result<(), Box<dyn Error>> {
    let Some(target) = profile.exhaustive_target_unique_states() else {
        return Ok(());
    };
    println!(
        "model-check profile-total profile={} unique_states={} target_unique_states={}",
        profile.name(),
        unique_states,
        target
    );
    if unique_states < target {
        return Err(format!(
            "{} explored {unique_states} unique states, below target {target}",
            profile.name()
        )
        .into());
    }
    Ok(())
}

fn scheduled_exhaustive_bounds(profile: Profile, bounds: Bounds) -> Bounds {
    match profile {
        Profile::RaftNightly => bounds
            .with_max_unique_states(NIGHTLY_EXHAUSTIVE_MAX_UNIQUE_STATES)
            .with_max_wall_clock(NIGHTLY_EXHAUSTIVE_MAX_WALL_CLOCK),
        Profile::RaftWeekly => bounds
            .with_max_unique_states(WEEKLY_EXHAUSTIVE_MAX_UNIQUE_STATES)
            .with_max_wall_clock(WEEKLY_EXHAUSTIVE_MAX_WALL_CLOCK),
        Profile::Fast | Profile::RaftDeep | Profile::RaftSoak => bounds,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        assert_exhaustive_target, scheduled_exhaustive_bounds,
        NIGHTLY_EXHAUSTIVE_MAX_UNIQUE_STATES, NIGHTLY_EXHAUSTIVE_MAX_WALL_CLOCK,
        WEEKLY_EXHAUSTIVE_MAX_UNIQUE_STATES, WEEKLY_EXHAUSTIVE_MAX_WALL_CLOCK,
    };
    use crate::profile::Profile;
    use rafter_sim::model_check::Bounds;

    #[test]
    fn exhaustive_target_gate_uses_unique_state_counts() {
        let target = Profile::RaftNightly
            .exhaustive_target_unique_states()
            .expect("nightly has an exhaustive target");

        assert!(assert_exhaustive_target(Profile::RaftNightly, target).is_ok());
        let error = assert_exhaustive_target(Profile::RaftNightly, target - 1)
            .expect_err("below-target unique states should fail");

        assert!(error.to_string().contains("unique states"));
    }

    #[test]
    fn scheduled_profiles_apply_wall_clock_and_unique_state_caps() {
        let nightly = scheduled_exhaustive_bounds(Profile::RaftNightly, Bounds::new(8));
        assert_eq!(
            nightly.max_unique_states(),
            Some(NIGHTLY_EXHAUSTIVE_MAX_UNIQUE_STATES)
        );
        assert_eq!(
            nightly.max_wall_clock(),
            Some(NIGHTLY_EXHAUSTIVE_MAX_WALL_CLOCK)
        );

        let weekly = scheduled_exhaustive_bounds(Profile::RaftWeekly, Bounds::new(9));
        assert_eq!(
            weekly.max_unique_states(),
            Some(WEEKLY_EXHAUSTIVE_MAX_UNIQUE_STATES)
        );
        assert_eq!(
            weekly.max_wall_clock(),
            Some(WEEKLY_EXHAUSTIVE_MAX_WALL_CLOCK)
        );

        let fast = scheduled_exhaustive_bounds(Profile::Fast, Bounds::new(7));
        assert_eq!(fast.max_unique_states(), None);
        assert_eq!(fast.max_wall_clock(), None);
    }
}
