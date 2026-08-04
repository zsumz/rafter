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
mod tests {
    use super::*;

    #[test]
    fn exhaustive_target_gate_requires_protocol_and_verifier_state_counts() {
        assert_eq!(
            target_values(Profile::RaftNightly),
            (13_000_000, 13_000_000)
        );
        assert_eq!(
            target_values(Profile::RaftWeekly),
            (250_000_000, 250_000_000)
        );
        assert_eq!(Profile::Fast.exhaustive_targets(), None);
        let target = 13_000_000;

        assert!(assert_exhaustive_targets(Profile::RaftNightly, target, target).is_ok());
        let protocol_error = assert_exhaustive_targets(Profile::RaftNightly, target - 1, target)
            .expect_err("below-target protocol states should fail");
        let verifier_error = assert_exhaustive_targets(Profile::RaftNightly, target, target - 1)
            .expect_err("below-target verifier states should fail");

        assert!(protocol_error.to_string().contains("protocol states"));
        assert!(verifier_error.to_string().contains("verifier states"));
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
