use std::error::Error;

use rafter_sim::model_check::{
    check_raft_commit_safety, check_raft_election_safety,
    check_raft_joint_membership_restart_and_snapshot_safety, check_raft_leadership_noop_safety,
    check_raft_membership_safety, check_raft_read_index_safety,
    check_raft_restart_and_snapshot_safety, check_raft_seeded_commit_safety, Bounds,
};
use rafter_sim::SimSeed;

use crate::profile::{Profile, SoakProfile};
use crate::raft_config::{
    four_node_future_learner_configs, three_node_configs, three_node_lease_configs,
    three_node_pre_vote_configs, three_node_production_configs,
};

use super::checks::run_raft_check;
use super::exhaustive::{assert_exhaustive_target, scheduled_exhaustive_bounds};
use super::soak::{run_raft_soak_profile, scheduled_seeds};

pub(super) fn run_raft_nightly_profile(
    seed_override: Option<Vec<SimSeed>>,
) -> Result<(), Box<dyn Error>> {
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

pub(super) fn run_raft_weekly_profile(
    seed_override: Option<Vec<SimSeed>>,
) -> Result<(), Box<dyn Error>> {
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
