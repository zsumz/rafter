//! State-count targets and resource caps for the scheduled profiles.
//!
//! A scheduled profile passes only when it explores at least its target count
//! of both unique protocol states and unique verifier states; falling short of
//! either is an error. Wall-clock and unique-state caps bound the run so a
//! scheduled job terminates; the local profiles carry no targets at all.

use std::{error::Error, time::Duration};

use rafter_sim::model_check::Bounds;

use crate::profile::Profile;
use crate::reporting::print_profile_total;

const NIGHTLY_EXHAUSTIVE_MAX_WALL_CLOCK: Duration = Duration::from_secs(1_200);
const WEEKLY_EXHAUSTIVE_MAX_WALL_CLOCK: Duration = Duration::from_secs(3_600);
const NIGHTLY_EXHAUSTIVE_MAX_UNIQUE_STATES: usize = 120_000_000;
const WEEKLY_EXHAUSTIVE_MAX_UNIQUE_STATES: usize = 300_000_000;

pub(super) fn assert_exhaustive_targets(
    profile: Profile,
    unique_protocol_states: usize,
    unique_verifier_states: usize,
) -> Result<(), Box<dyn Error>> {
    let Some(targets) = profile.exhaustive_targets() else {
        return Ok(());
    };
    println!(
        "model-check profile-total profile={} unique_protocol_states={} unique_verifier_states={} target_protocol_states={} target_verifier_states={}",
        profile.name(),
        unique_protocol_states,
        unique_verifier_states,
        targets.protocol_states,
        targets.verifier_states
    );
    print_profile_total(
        profile.name(),
        unique_protocol_states,
        unique_verifier_states,
        targets.protocol_states,
        targets.verifier_states,
    );
    if unique_protocol_states < targets.protocol_states {
        return Err(format!(
            "{} explored {unique_protocol_states} unique protocol states, below target {}",
            profile.name(),
            targets.protocol_states,
        )
        .into());
    }
    if unique_verifier_states < targets.verifier_states {
        return Err(format!(
            "{} explored {unique_verifier_states} unique verifier states, below target {}",
            profile.name(),
            targets.verifier_states,
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
fn target_values(profile: Profile) -> (usize, usize) {
    let targets = profile.exhaustive_targets().expect("scheduled target");
    (targets.protocol_states, targets.verifier_states)
}

pub(super) fn scheduled_exhaustive_bounds(profile: Profile, bounds: Bounds) -> Bounds {
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
#[path = "exhaustive_test.rs"]
mod tests;
