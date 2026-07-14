use std::fs;

use super::support::*;
use crate::producer::tla_output::parse;

pub(super) fn shorter_authoritative_log_repairs_an_uncommitted_suffix() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "shorter-conflict-repair",
        &raft,
        &detector,
        SHORTER_CONFLICT_REPAIR_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse shorter conflict-repair output");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stdout)
    );
    assert!(summary.completed_without_error);
    assert!(summary.process_finished);
    assert!(summary.distinct_states >= 12);
    assert!(summary.search_depth >= 12);
}

pub(super) fn delayed_append_uses_frozen_sender_authority_after_self_removal() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let baseline = run_tlc_with_config(
        &root,
        "frozen-append-authority",
        &raft,
        &detector,
        FROZEN_APPEND_AUTHORITY_CONFIG,
    );
    let baseline_summary = parse(&baseline.stdout).expect("parse frozen-authority output");
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stdout)
    );
    assert!(baseline_summary.completed_without_error);
    assert!(baseline_summary.process_finished);
    assert!(baseline_summary.distinct_states >= 18);
    assert!(baseline_summary.search_depth >= 18);

    let dynamic_sender_authority = replace_exactly_once_in_operator(
        &raft,
        "DeliverAppend(m)",
        "RoleAfterCommit(node, selfRemoval)",
        "/\\ AppendSenderAuthorized(m)\n                /\\ AppendReceiverEligible(m)",
        "/\\ AppendSenderAuthorized(m)\n                /\\ \\/ m.from \\in ActiveVoters(EffectiveConfiguration(m.from).config)\n                    \\/ PendingSelfRemoval(m.from)\n                /\\ AppendReceiverEligible(m)",
    );
    let mutation = run_tlc_with_config(
        &root,
        "dynamic-append-authority-mutation",
        &dynamic_sender_authority,
        &detector,
        FROZEN_APPEND_AUTHORITY_CONFIG,
    );
    let mutation_summary = parse(&mutation.stdout).expect("parse authority mutation output");
    assert_eq!(mutation.status.code(), Some(12));
    assert_eq!(
        mutation_summary.violated_invariant.as_deref(),
        Some("FrozenAppendAuthorityInvariant")
    );
}

pub(super) fn removed_candidate_vote_requires_membership_and_freshness_guards() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let baseline = run_tlc_with_config(
        &root,
        "removed-candidate-vote-baseline",
        &raft,
        &detector,
        REMOVED_CANDIDATE_VOTE_GUARD_CONFIG,
    );
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stdout)
    );

    let membership_disabled =
        replace_operator(&raft, "VoteMembershipEligible(m)", "VoteIsFresh(m)", "TRUE");
    let membership_control = run_tlc_with_config(
        &root,
        "removed-candidate-membership-only-mutation",
        &membership_disabled,
        &detector,
        REMOVED_CANDIDATE_VOTE_GUARD_CONFIG,
    );
    assert!(
        membership_control.status.success(),
        "{}",
        String::from_utf8_lossy(&membership_control.stdout)
    );

    let freshness_disabled =
        replace_operator(&raft, "VoteIsFresh(m)", "DeliverRequestVote(m)", "TRUE");
    let freshness_control = run_tlc_with_config(
        &root,
        "removed-candidate-freshness-only-mutation",
        &freshness_disabled,
        &detector,
        REMOVED_CANDIDATE_VOTE_GUARD_CONFIG,
    );
    assert!(
        freshness_control.status.success(),
        "{}",
        String::from_utf8_lossy(&freshness_control.stdout)
    );

    let combined = replace_operator(
        &membership_disabled,
        "VoteIsFresh(m)",
        "DeliverRequestVote(m)",
        "TRUE",
    );
    let result = run_tlc_with_config(
        &root,
        "removed-candidate-membership-and-freshness-mutation",
        &combined,
        &detector,
        REMOVED_CANDIDATE_VOTE_GUARD_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse removed-candidate mutation output");
    assert_eq!(result.status.code(), Some(12));
    assert_eq!(
        summary.violated_invariant.as_deref(),
        Some("RemovedCandidateVoteGuardInvariant")
    );
}

pub(super) fn leader_completeness_uses_commit_authority_term() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let baseline = run_tlc_with_config(
        &root,
        "commit-authority-term-baseline",
        &raft,
        &detector,
        COMMIT_AUTHORITY_TERM_REGRESSION_CONFIG,
    );
    let baseline_summary = parse(&baseline.stdout).expect("parse commit-term baseline output");
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stdout)
    );
    assert!(baseline_summary.completed_without_error);
    assert!(baseline_summary.distinct_states >= 20);
    assert!(baseline_summary.search_depth >= 20);

    let mutated = replace_exactly_once(
        &raft,
        "currentTerm[leader] > committed.committedInTerm",
        "currentTerm[leader] > committed.entry.term",
    );
    let result = run_tlc_with_config(
        &root,
        "commit-authority-term-entry-term-mutation",
        &mutated,
        &detector,
        COMMIT_AUTHORITY_TERM_REGRESSION_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse commit-term mutation output");
    assert_eq!(result.status.code(), Some(12));
    assert_eq!(
        summary.violated_invariant.as_deref(),
        Some("CommitAuthorityTermRegressionInvariant")
    );
}

pub(super) fn self_removing_leader_commits_final_configuration_and_steps_down() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "self-removal-commit",
        &raft,
        &detector,
        SELF_REMOVAL_COMMIT_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse self-removal commit output");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stdout)
    );
    assert!(summary.completed_without_error);
    assert!(summary.process_finished);
    assert!(summary.distinct_states >= 2);
    assert!(summary.search_depth >= 2);
}

pub(super) fn missing_self_removal_step_down_breaks_commit_regression() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RoleAfterCommit(node, selfRemoval)",
        "Commit(n, i)",
        "role",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "missing-self-removal-step-down",
        &mutated,
        &detector,
        SELF_REMOVAL_COMMIT_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse missing step-down output");
    assert_eq!(result.status.code(), Some(12));
    assert_eq!(summary.violated_invariant.as_deref(), Some("TypeOK"));
}

pub(super) fn unfrozen_effective_membership_breaks_commit_witness_regression() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "FrozenCommitContext(\n    leaderRole, leaderTerm, effectiveView, authorityView)",
        "MatchingReplicasFrom(logs, snapshotIndexes, snapshotPrefixes, node, index)",
        "[leaderRole |-> leaderRole,\n   leaderTerm |-> leaderTerm,\n   effectiveMembership |-> authorityView,\n   authorityMembership |-> authorityView]",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "unfrozen-self-removal-effective-membership",
        &mutated,
        &detector,
        SELF_REMOVAL_COMMIT_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse unfrozen commit context output");
    assert_eq!(result.status.code(), Some(12));
    assert_eq!(
        summary.violated_invariant.as_deref(),
        Some("CommittedEntriesHaveQuorum")
    );
}

pub(super) fn applied_membership_quorum_mutation_breaks_joint_regression() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_exactly_once(
        &raft,
        "MembershipQuorum(\n         preEffectiveMembership, MatchingReplicas(n, i))",
        "MembershipQuorum(\n         AppliedConfiguration(n).config, MatchingReplicas(n, i))",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "applied-membership-joint-quorum",
        &mutated,
        &detector,
        JOINT_QUORUM_REGRESSION_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse joint quorum mutation output");
    assert_eq!(result.status.code(), Some(12));
    assert_eq!(
        summary.violated_invariant.as_deref(),
        Some("JointQuorumOldSideCannotCommit")
    );
}

pub(super) fn missing_effective_recomputation_breaks_overwrite_regression() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "EffectiveConfiguration(node)",
        "SnapshotIdentitySoundFor(logs, snapshotIndexes, snapshotPrefixes, compactionPendings)",
        "AppliedConfiguration(node)",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "missing-effective-overwrite-recomputation",
        &mutated,
        &detector,
        EFFECTIVE_OVERWRITE_REGRESSION_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse effective overwrite mutation output");
    assert_eq!(result.status.code(), Some(12));
    assert_eq!(
        summary.violated_invariant.as_deref(),
        Some("EffectiveOverwriteRegressionInvariant")
    );
}

pub(super) fn follower_recomputation_breaks_delayed_heartbeat_regression() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "EffectiveConfiguration(node)",
        "SnapshotIdentitySoundFor(logs, snapshotIndexes, snapshotPrefixes, compactionPendings)",
        "AppliedConfiguration(node)",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "follower-recomputes-effective-configuration",
        &mutated,
        &detector,
        DELAYED_HEARTBEAT_REGRESSION_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse delayed heartbeat mutation output");
    assert_eq!(result.status.code(), Some(12));
    assert_eq!(
        summary.violated_invariant.as_deref(),
        Some("DelayedHeartbeatRegressionInvariant")
    );
}
