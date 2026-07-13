use std::{error::Error, io, time::Instant};

use rafter_sim::model_check::{
    check_raft_commit_safety, check_raft_election_safety,
    check_raft_joint_membership_restart_and_snapshot_safety, check_raft_leadership_noop_safety,
    check_raft_membership_safety, check_raft_read_index_safety,
    check_raft_restart_and_snapshot_safety, check_raft_seeded_commit_safety,
    check_raft_semantic_witness_safety, Bounds, ExplorationCompletion, Failure, Summary,
};
use rafter_sim::SimSeed;

use crate::profile::{Profile, SoakProfile};
use crate::raft_config::{
    four_node_future_learner_configs, three_node_configs, three_node_configs_with_inflight_window,
    three_node_lease_configs, three_node_pre_vote_configs, three_node_production_configs,
};
use crate::reporting::{print_raft_failure, print_raft_summary};

use super::soak::{fixed_or_override, run_raft_soak_profile};

pub(super) fn run_fast_profile() -> Result<(), Box<dyn Error>> {
    let bounds = Profile::Fast
        .local_model_bounds()
        .expect("fast profile has local model bounds");
    run_raft_check("raft-election", || {
        check_raft_election_safety(three_node_configs(2), Bounds::new(bounds.election_depth))
    })?;
    // These depths back the reviewed per-evidence state floors in
    // verification/raft-invariants.yaml. Both in-flight window regimes are
    // explored explicitly. Two proposals
    // make the window bind: a window of one answers the second proposal
    // with an empty append until the first batch is acknowledged, while a
    // pipelined window streams the second batch immediately.
    run_raft_check("raft-commit", || {
        check_raft_commit_safety(
            three_node_configs_with_inflight_window(2, 3),
            Bounds::new(bounds.commit_depth).with_max_proposals(bounds.commit_proposals),
        )
    })?;
    run_raft_check("raft-commit-window1", || {
        check_raft_commit_safety(
            three_node_configs_with_inflight_window(2, 1),
            Bounds::new(bounds.commit_depth).with_max_proposals(bounds.commit_proposals),
        )
    })?;
    run_raft_check("raft-commit-production", || {
        check_raft_commit_safety(
            three_node_production_configs(3),
            Bounds::new(bounds.commit_production_depth).with_max_proposals(1),
        )
    })?;
    run_raft_check("raft-membership", || {
        check_raft_membership_safety(
            four_node_future_learner_configs(3),
            Bounds::new(bounds.membership_depth)
                .with_max_proposals(1)
                .with_max_membership_changes(bounds.membership_changes),
        )
    })?;
    run_raft_check("raft-membership-restart-snapshot", || {
        check_raft_joint_membership_restart_and_snapshot_safety(
            Bounds::new(bounds.membership_restart_snapshot_depth)
                .with_max_restarts(bounds.membership_restart_snapshot_restarts),
        )
    })?;
    run_raft_check("raft-commit-seeded", || {
        check_raft_seeded_commit_safety(
            three_node_configs(2),
            Bounds::new(bounds.seeded_commit_depth)
                .with_max_restarts(bounds.seeded_commit_restarts),
        )
    })?;
    run_raft_check("raft-leadership-noop-seeded", || {
        check_raft_leadership_noop_safety(Bounds::new(bounds.leadership_noop_depth))
    })?;
    run_raft_check("raft-restart-snapshot", || {
        check_raft_restart_and_snapshot_safety(
            Bounds::new(bounds.restart_snapshot_depth)
                .with_max_restarts(bounds.restart_snapshot_restarts),
        )
    })?;
    run_raft_check("raft-election-prevote", || {
        check_raft_election_safety(
            three_node_pre_vote_configs(2),
            Bounds::new(bounds.prevote_depth),
        )
    })?;
    run_raft_check(
        "raft-semantic-witnesses",
        check_raft_semantic_witness_safety,
    )?;
    run_raft_check("raft-read-index", || {
        check_raft_read_index_safety(
            three_node_configs(2),
            Bounds::new(bounds.read_index_depth)
                .with_max_proposals(1)
                .with_max_read_indexes(bounds.read_indexes),
        )
    })?;
    run_raft_check("raft-lease-read", || {
        check_raft_read_index_safety(
            three_node_lease_configs(8),
            Bounds::new(bounds.lease_read_depth)
                .with_max_proposals(1)
                .with_max_read_indexes(bounds.lease_read_indexes),
        )
    })?;
    Ok(())
}

pub(super) fn run_raft_deep_profile(
    seed_override: Option<Vec<SimSeed>>,
) -> Result<(), Box<dyn Error>> {
    let bounds = Profile::RaftDeep
        .local_model_bounds()
        .expect("raft-deep profile has local model bounds");
    run_raft_check("raft-election-deep", || {
        check_raft_election_safety(three_node_configs(2), Bounds::new(bounds.election_depth))
    })?;
    run_raft_check("raft-commit-deep", || {
        check_raft_commit_safety(
            three_node_configs(2),
            Bounds::new(bounds.commit_depth).with_max_proposals(bounds.commit_proposals),
        )
    })?;
    run_raft_check("raft-commit-production-deep", || {
        check_raft_commit_safety(
            three_node_production_configs(3),
            Bounds::new(bounds.commit_production_depth).with_max_proposals(1),
        )
    })?;
    run_raft_check("raft-membership-deep", || {
        check_raft_membership_safety(
            four_node_future_learner_configs(3),
            Bounds::new(bounds.membership_depth)
                .with_max_proposals(1)
                .with_max_membership_changes(bounds.membership_changes),
        )
    })?;
    run_raft_check("raft-membership-restart-snapshot-deep", || {
        check_raft_joint_membership_restart_and_snapshot_safety(
            Bounds::new(bounds.membership_restart_snapshot_depth)
                .with_max_restarts(bounds.membership_restart_snapshot_restarts),
        )
    })?;
    run_raft_check("raft-commit-seeded-deep", || {
        check_raft_seeded_commit_safety(
            three_node_configs(2),
            Bounds::new(bounds.seeded_commit_depth)
                .with_max_restarts(bounds.seeded_commit_restarts),
        )
    })?;
    run_raft_check("raft-leadership-noop-seeded-deep", || {
        check_raft_leadership_noop_safety(Bounds::new(bounds.leadership_noop_depth))
    })?;
    run_raft_check("raft-restart-snapshot-deep", || {
        check_raft_restart_and_snapshot_safety(
            Bounds::new(bounds.restart_snapshot_depth)
                .with_max_restarts(bounds.restart_snapshot_restarts),
        )
    })?;
    run_raft_check("raft-election-prevote-deep", || {
        check_raft_election_safety(
            three_node_pre_vote_configs(2),
            Bounds::new(bounds.prevote_depth),
        )
    })?;
    run_raft_check("raft-read-index-deep", || {
        check_raft_read_index_safety(
            three_node_configs(2),
            Bounds::new(bounds.read_index_depth)
                .with_max_proposals(1)
                .with_max_read_indexes(bounds.read_indexes),
        )
    })?;
    run_raft_check("raft-lease-read-deep", || {
        check_raft_read_index_safety(
            three_node_lease_configs(8),
            Bounds::new(bounds.lease_read_depth)
                .with_max_proposals(1)
                .with_max_read_indexes(bounds.lease_read_indexes),
        )
    })?;
    let profile = SoakProfile::raft_deep();
    let (seeds, source) = fixed_or_override(profile.seeds, seed_override);
    run_raft_soak_profile(profile, &seeds, source)
}

pub(super) fn run_raft_check(
    name: &str,
    check: impl FnOnce() -> Result<Summary, Failure>,
) -> Result<Summary, Box<dyn Error>> {
    let started = Instant::now();
    let result = check();
    let duration = started.elapsed();
    let summary = result.inspect_err(|failure| {
        print_raft_failure(name, failure);
    })?;
    print_raft_summary(name, summary, duration);
    if summary.completion() != ExplorationCompletion::FrontierExhausted {
        eprintln!(
            "ERROR test model coverage name={name} completion={} configured_depth={} reached_depth={}",
            summary.completion(),
            summary.max_depth(),
            summary.reached_depth()
        );
        return Err(
            io::Error::other(format!("model-check {name} did not exhaust its frontier")).into(),
        );
    }
    Ok(summary)
}
