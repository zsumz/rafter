use std::{
    error::Error,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use rafter::NodeConfig;
use rafter_sim::model_check::run_raft_random_soak;
use rafter_sim::SimSeed;

use crate::profile::{SoakCheckKind, SoakExecutionContract, SoakProfile};
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
    for (kind, configs) in [
        (SoakCheckKind::Standard, three_node_configs(2)),
        (SoakCheckKind::Lease, three_node_lease_configs(8)),
        (
            SoakCheckKind::Membership,
            four_node_future_learner_configs(3),
        ),
    ] {
        let contract = profile.execution_contract(kind);
        run_raft_soak_profile_for_configs(profile, seeds, &configs, &contract)?;
    }
    Ok(())
}

fn run_raft_soak_profile_for_configs(
    profile: SoakProfile,
    seeds: &[SimSeed],
    configs: &[NodeConfig],
    contract: &SoakExecutionContract,
) -> Result<(), Box<dyn Error>> {
    contract.validate_node_configs(configs)?;
    for seed in seeds.iter().copied() {
        let config = profile.soak_config(seed);
        contract.validate_config(config)?;
        let started = Instant::now();
        let summary = run_raft_random_soak(configs.to_owned(), config).inspect_err(|failure| {
            print_soak_failure(&contract.check_id, failure);
        })?;
        print_soak_summary(contract, &summary, config, started.elapsed());
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
