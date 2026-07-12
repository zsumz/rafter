use std::error::Error;

use rafter_sim::model_check::{
    check_raft_commit_safety, check_raft_election_safety,
    check_raft_joint_membership_restart_and_snapshot_safety, check_raft_leadership_noop_safety,
    check_raft_membership_safety, check_raft_read_index_safety,
    check_raft_restart_and_snapshot_safety, check_raft_seeded_commit_safety, Bounds, Summary,
};
use rafter_sim::SimSeed;

use crate::profile::{Profile, SoakProfile};
use crate::raft_config::{
    four_node_future_learner_check_quorum_configs, four_node_future_learner_configs,
    four_node_future_learner_pre_vote_configs, three_node_check_quorum_configs, three_node_configs,
    three_node_configs_with_inflight_window, three_node_lease_configs, three_node_pre_vote_configs,
    three_node_production_configs,
};

use super::checks::run_raft_check;
use super::exhaustive::{assert_exhaustive_targets, scheduled_exhaustive_bounds};
use super::soak::{run_raft_soak_profile, scheduled_seeds};

pub(super) fn run_raft_nightly_profile(
    seed_override: Option<Vec<SimSeed>>,
) -> Result<(), Box<dyn Error>> {
    let scheduled = |bounds| scheduled_exhaustive_bounds(Profile::RaftNightly, bounds);
    let mut totals = StateTotals::default();
    totals.record(run_raft_check("raft-election-nightly", || {
        check_raft_election_safety(three_node_configs(2), scheduled(Bounds::new(8)))
    })?);
    totals.record(run_raft_check("raft-commit-nightly", || {
        check_raft_commit_safety(
            three_node_configs(2),
            scheduled(Bounds::new(11).with_max_proposals(3)),
        )
    })?);
    totals.record(run_raft_check("raft-commit-window1-nightly", || {
        check_raft_commit_safety(
            three_node_configs_with_inflight_window(2, 1),
            scheduled(Bounds::new(11).with_max_proposals(3)),
        )
    })?);
    totals.record(run_raft_check("raft-commit-prevote-nightly", || {
        check_raft_commit_safety(
            three_node_pre_vote_configs(2),
            scheduled(Bounds::new(11).with_max_proposals(3)),
        )
    })?);
    totals.record(run_raft_check("raft-commit-production-nightly", || {
        check_raft_commit_safety(
            three_node_production_configs(3),
            scheduled(Bounds::new(8).with_max_proposals(1)),
        )
    })?);
    totals.record(run_raft_check("raft-membership-nightly", || {
        check_raft_membership_safety(
            four_node_future_learner_configs(3),
            scheduled(
                Bounds::new(7)
                    .with_max_proposals(2)
                    .with_max_membership_changes(3),
            ),
        )
    })?);
    totals.record(run_raft_check(
        "raft-membership-restart-snapshot-nightly",
        || {
            check_raft_joint_membership_restart_and_snapshot_safety(scheduled(
                Bounds::new(10).with_max_restarts(2),
            ))
        },
    )?);
    totals.record(run_raft_check("raft-commit-seeded-nightly", || {
        check_raft_seeded_commit_safety(three_node_configs(2), scheduled(Bounds::new(2)))
    })?);
    totals.record(run_raft_check(
        "raft-leadership-noop-seeded-nightly",
        || check_raft_leadership_noop_safety(scheduled(Bounds::new(8))),
    )?);
    totals.record(run_raft_check("raft-restart-snapshot-nightly", || {
        check_raft_restart_and_snapshot_safety(scheduled(Bounds::new(10).with_max_restarts(2)))
    })?);
    totals.record(run_raft_check("raft-read-index-nightly", || {
        check_raft_read_index_safety(
            three_node_configs(2),
            scheduled(
                Bounds::new(8)
                    .with_max_proposals(1)
                    .with_max_read_indexes(2),
            ),
        )
    })?);
    totals.record(run_raft_check("raft-lease-read-nightly", || {
        check_raft_read_index_safety(
            three_node_lease_configs(8),
            scheduled(
                Bounds::new(7)
                    .with_max_proposals(1)
                    .with_max_read_indexes(2),
            ),
        )
    })?);
    totals.require(Profile::RaftNightly)?;
    let profile = SoakProfile::raft_nightly();
    let (seeds, source) = scheduled_seeds(profile, seed_override);
    run_raft_soak_profile(profile, &seeds, source)
}

pub(super) fn run_raft_weekly_profile(
    seed_override: Option<Vec<SimSeed>>,
) -> Result<(), Box<dyn Error>> {
    let scheduled = |bounds| scheduled_exhaustive_bounds(Profile::RaftWeekly, bounds);
    let mut totals = StateTotals::default();
    totals.record(run_raft_check("raft-election-weekly", || {
        check_raft_election_safety(three_node_configs(2), scheduled(Bounds::new(9)))
    })?);
    totals.record(run_raft_check("raft-commit-weekly", || {
        check_raft_commit_safety(
            three_node_configs(2),
            scheduled(Bounds::new(11).with_max_proposals(4)),
        )
    })?);
    totals.record(run_raft_check("raft-commit-window1-weekly", || {
        check_raft_commit_safety(
            three_node_configs_with_inflight_window(2, 1),
            scheduled(Bounds::new(11).with_max_proposals(4)),
        )
    })?);
    totals.record(run_raft_check("raft-commit-prevote-weekly", || {
        check_raft_commit_safety(
            three_node_pre_vote_configs(2),
            scheduled(Bounds::new(11).with_max_proposals(4)),
        )
    })?);
    totals.record(run_raft_check("raft-commit-check-quorum-weekly", || {
        check_raft_commit_safety(
            three_node_check_quorum_configs(2),
            scheduled(Bounds::new(11).with_max_proposals(4)),
        )
    })?);
    totals.record(run_raft_check("raft-commit-production-weekly", || {
        check_raft_commit_safety(
            three_node_production_configs(3),
            scheduled(Bounds::new(9).with_max_proposals(1)),
        )
    })?);
    record_weekly_membership_checks(&mut totals, scheduled)?;
    totals.record(run_raft_check(
        "raft-membership-restart-snapshot-weekly",
        || {
            check_raft_joint_membership_restart_and_snapshot_safety(scheduled(
                Bounds::new(11).with_max_restarts(3),
            ))
        },
    )?);
    totals.record(run_raft_check("raft-commit-seeded-weekly", || {
        check_raft_seeded_commit_safety(three_node_configs(2), scheduled(Bounds::new(3)))
    })?);
    totals.record(run_raft_check(
        "raft-leadership-noop-seeded-weekly",
        || check_raft_leadership_noop_safety(scheduled(Bounds::new(8))),
    )?);
    totals.record(run_raft_check("raft-restart-snapshot-weekly", || {
        check_raft_restart_and_snapshot_safety(scheduled(Bounds::new(11).with_max_restarts(3)))
    })?);
    totals.record(run_raft_check("raft-election-prevote-weekly", || {
        check_raft_election_safety(three_node_pre_vote_configs(2), scheduled(Bounds::new(8)))
    })?);
    totals.record(run_raft_check("raft-read-index-weekly", || {
        check_raft_read_index_safety(
            three_node_configs(2),
            scheduled(
                Bounds::new(8)
                    .with_max_proposals(2)
                    .with_max_read_indexes(3),
            ),
        )
    })?);
    totals.record(run_raft_check("raft-lease-read-weekly", || {
        check_raft_read_index_safety(
            three_node_lease_configs(8),
            scheduled(
                Bounds::new(8)
                    .with_max_proposals(1)
                    .with_max_read_indexes(3),
            ),
        )
    })?);
    totals.require(Profile::RaftWeekly)?;
    let profile = SoakProfile::raft_weekly();
    let (seeds, source) = scheduled_seeds(profile, seed_override);
    run_raft_soak_profile(profile, &seeds, source)
}

fn record_weekly_membership_checks(
    totals: &mut StateTotals,
    scheduled: impl Fn(Bounds) -> Bounds + Copy,
) -> Result<(), Box<dyn Error>> {
    let bounds = || {
        scheduled(
            Bounds::new(8)
                .with_max_proposals(2)
                .with_max_membership_changes(4),
        )
    };
    for (name, configs) in [
        (
            "raft-membership-weekly",
            four_node_future_learner_configs(3),
        ),
        (
            "raft-membership-prevote-weekly",
            four_node_future_learner_pre_vote_configs(3),
        ),
        (
            "raft-membership-check-quorum-weekly",
            four_node_future_learner_check_quorum_configs(3),
        ),
    ] {
        totals.record(run_raft_check(name, || {
            check_raft_membership_safety(configs, bounds())
        })?);
    }
    Ok(())
}

#[derive(Default)]
struct StateTotals {
    protocol: usize,
    verifier: usize,
}

impl StateTotals {
    fn record(&mut self, summary: Summary) {
        self.protocol += summary.unique_protocol_states();
        self.verifier += summary.unique_verifier_states();
    }

    fn require(self, profile: Profile) -> Result<(), Box<dyn Error>> {
        assert_exhaustive_targets(profile, self.protocol, self.verifier)
    }
}
