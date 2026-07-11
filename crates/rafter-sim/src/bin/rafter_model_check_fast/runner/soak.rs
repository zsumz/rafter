use std::{
    error::Error,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use rafter::NodeConfig;
use rafter_sim::model_check::{run_raft_random_soak, SoakConfig};
use rafter_sim::SimSeed;

use crate::profile::SoakProfile;
use crate::raft_config::{
    four_node_future_learner_configs, three_node_configs, three_node_lease_configs,
};
use crate::reporting::{print_soak_failure, print_soak_summary};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SoakSeedSource {
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

pub(super) fn run_raft_soak_profile(
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

pub(super) fn fixed_or_override(
    fixed: &[SimSeed],
    seed_override: Option<Vec<SimSeed>>,
) -> (Vec<SimSeed>, SoakSeedSource) {
    match seed_override {
        Some(seeds) => (seeds, SoakSeedSource::Replay),
        None => (fixed.to_vec(), SoakSeedSource::Curated),
    }
}

pub(super) fn scheduled_seeds(
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
