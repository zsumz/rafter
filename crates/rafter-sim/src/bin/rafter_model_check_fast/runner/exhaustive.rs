use std::{error::Error, time::Duration};

use rafter_sim::model_check::Bounds;

use crate::profile::Profile;

const NIGHTLY_EXHAUSTIVE_MAX_WALL_CLOCK: Duration = Duration::from_secs(1_200);
const WEEKLY_EXHAUSTIVE_MAX_WALL_CLOCK: Duration = Duration::from_secs(3_600);
const NIGHTLY_EXHAUSTIVE_MAX_UNIQUE_STATES: usize = 120_000_000;
const WEEKLY_EXHAUSTIVE_MAX_UNIQUE_STATES: usize = 300_000_000;

pub(super) fn assert_exhaustive_target(
    profile: Profile,
    unique_states: usize,
) -> Result<(), Box<dyn Error>> {
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
