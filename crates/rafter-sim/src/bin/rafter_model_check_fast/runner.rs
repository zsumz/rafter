use std::error::Error;

#[path = "runner/checks.rs"]
mod checks;
#[path = "runner/exhaustive.rs"]
mod exhaustive;
#[path = "runner/scheduled.rs"]
mod scheduled;
#[path = "runner/soak.rs"]
mod soak;

use super::profile::{Profile, ProfileRun, SoakProfile, SCHEDULE_CLASSES};

pub(crate) fn run_profile(run: ProfileRun) -> Result<(), Box<dyn Error>> {
    let profile = run.profile;
    let targets = profile.exhaustive_targets();
    println!(
        "model-check profile={} expected_runtime={} exhaustive_target_protocol_states={} exhaustive_target_verifier_states={} bounds=\"{}\" schedule_classes={}",
        profile.name(),
        profile.expected_runtime(),
        targets.map_or_else(
            || "none".to_string(),
            |targets| targets.protocol_states.to_string()
        ),
        targets.map_or_else(
            || "none".to_string(),
            |targets| targets.verifier_states.to_string()
        ),
        profile.bounds_summary(),
        SCHEDULE_CLASSES
    );

    match profile {
        Profile::Fast => checks::run_fast_profile(),
        Profile::RaftDeep => checks::run_raft_deep_profile(run.seed_override),
        Profile::RaftSoak => {
            let profile = SoakProfile::raft_soak();
            let (seeds, source) = soak::fixed_or_override(profile.seeds, run.seed_override);
            soak::run_raft_soak_profile(profile, &seeds, source)
        }
        Profile::RaftNightly => scheduled::run_raft_nightly_profile(run.seed_override),
        Profile::RaftWeekly => scheduled::run_raft_weekly_profile(run.seed_override),
    }
}
