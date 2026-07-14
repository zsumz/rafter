use std::{fs, path::Path, process::Command};

use super::detector_qualified;
use crate::producer::tla_output::{parse, render_detector_config, DetectorProbe, DETECTOR_PROBES};

const ELECTION_PROBE: DetectorProbe = DetectorProbe {
    predicate: "ElectionSafety",
    mode: "ElectionRecorderOnly",
};
const LOG_MATCHING_PROBE: DetectorProbe = DetectorProbe {
    predicate: "LogMatching",
    mode: "LogMatchingRecorderOnly",
};
const SNAPSHOT_PREFIX_PROBE: DetectorProbe = DetectorProbe {
    predicate: "LogMatching",
    mode: "SnapshotPrefixRecorderOnly",
};
const LEADER_COMPLETENESS_PROBE: DetectorProbe = DetectorProbe {
    predicate: "LeaderCompleteness",
    mode: "LeaderCompletenessRecorderOnly",
};
const COMMITTED_PREFIX_PROBE: DetectorProbe = DetectorProbe {
    predicate: "CommittedPrefixStability",
    mode: "CommittedPrefixRecorderOnly",
};
const HIGHER_TERM_PROBE: DetectorProbe = DetectorProbe {
    predicate: "StaleLeaderFencing",
    mode: "HigherTermRecorderOnly",
};
const STALE_AUTHORITY_PROBE: DetectorProbe = DetectorProbe {
    predicate: "StaleLeaderFencing",
    mode: "StaleAuthorityRecorderOnly",
};
const APPLICATION_PROBE: DetectorProbe = DetectorProbe {
    predicate: "StateMachineSafety",
    mode: "ApplicationRecorderOnly",
};
const APPLICATION_EPOCH_PROBE: DetectorProbe = DetectorProbe {
    predicate: "StateMachineSafety",
    mode: "ApplicationEpochRecorderOnly",
};
const COMMIT_QUORUM_PROBE: DetectorProbe = DetectorProbe {
    predicate: "CommittedEntriesHaveQuorum",
    mode: "CommitQuorumRecorderOnly",
};
const READ_BARRIER_PROBE: DetectorProbe = DetectorProbe {
    predicate: "ReadBarrierLinearizability",
    mode: "ReadBarrierRecorderOnly",
};

const JOINT_QUORUM_REGRESSION_CONFIG: &str = r#"SPECIFICATION JointQuorumRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT JointQuorumOldSideCannotCommit
INVARIANT CommittedEntriesHaveQuorum
INVARIANT StateMachineSafety

PROPERTY JointQuorumRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

const EFFECTIVE_OVERWRITE_REGRESSION_CONFIG: &str = r#"SPECIFICATION EffectiveOverwriteRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT EffectiveOverwriteRegressionInvariant
INVARIANT CommittedEntriesHaveQuorum

PROPERTY EffectiveOverwriteRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

const DELAYED_HEARTBEAT_REGRESSION_CONFIG: &str = r#"SPECIFICATION DelayedHeartbeatRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT DelayedHeartbeatRegressionInvariant
INVARIANT CommittedEntriesHaveQuorum
INVARIANT StateMachineSafety

PROPERTY DelayedHeartbeatRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

const SNAPSHOT_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION SnapshotLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LogMatching"

INVARIANT TypeOK
INVARIANT SnapshotLifecycleInvariant

PROPERTY SnapshotLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

const APPLICATION_EPOCH_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION ApplicationEpochLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "StateMachineSafety"

INVARIANT TypeOK
INVARIANT ApplicationEpochLifecycleInvariant

PROPERTY ApplicationEpochLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

const SELF_REMOVAL_COMMIT_CONFIG: &str = r#"SPECIFICATION SelfRemovalCommitSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT SelfRemovalCommitInvariant
INVARIANT CommittedEntriesHaveQuorum

PROPERTY SelfRemovalCommitCompletes

CHECK_DEADLOCK FALSE
"#;

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn recorder_only_fixtures_qualify_before_mutation() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    for (name, probe) in [
        ("election-recorder-baseline", ELECTION_PROBE),
        ("log-matching-recorder-baseline", LOG_MATCHING_PROBE),
        ("snapshot-prefix-recorder-baseline", SNAPSHOT_PREFIX_PROBE),
        (
            "leader-completeness-recorder-baseline",
            LEADER_COMPLETENESS_PROBE,
        ),
        ("committed-prefix-recorder-baseline", COMMITTED_PREFIX_PROBE),
        ("higher-term-recorder-baseline", HIGHER_TERM_PROBE),
        ("stale-authority-recorder-baseline", STALE_AUTHORITY_PROBE),
        ("application-recorder-baseline", APPLICATION_PROBE),
        (
            "application-epoch-recorder-baseline",
            APPLICATION_EPOCH_PROBE,
        ),
        ("commit-quorum-recorder-baseline", COMMIT_QUORUM_PROBE),
        ("read-barrier-recorder-baseline", READ_BARRIER_PROBE),
    ] {
        let result = run_tlc_mutation(&root, name, &raft, &detector, probe);
        let summary = parse(&result.stdout).expect("parse TLC recorder baseline output");
        assert!(
            detector_qualified(result.status.code(), false, Some(&summary), probe.predicate),
            "{name} did not qualify:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn every_required_detector_probe_reaches_its_named_counterexample() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    for probe in DETECTOR_PROBES {
        let name = format!("required-{}-{}", probe.predicate, probe.mode);
        let result = run_tlc_mutation(&root, &name, &raft, &detector, probe);
        let summary = parse(&result.stdout).expect("parse TLC detector output");
        assert!(
            detector_qualified(result.status.code(), false, Some(&summary), probe.predicate),
            "required detector {}:{} did not qualify: {}",
            probe.predicate,
            probe.mode,
            String::from_utf8_lossy(&result.stdout)
        );
    }
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn snapshot_lifecycle_preserves_logical_identity_through_restart() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "snapshot-lifecycle",
        &raft,
        &detector,
        SNAPSHOT_LIFECYCLE_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse snapshot lifecycle output");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stdout)
    );
    assert!(summary.completed_without_error);
    assert!(summary.process_finished);
    assert!(summary.distinct_states >= 7);
    assert!(summary.search_depth >= 7);
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn application_epoch_loss_replays_identically_without_erasing_history() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "application-epoch-lifecycle",
        &raft,
        &detector,
        APPLICATION_EPOCH_LIFECYCLE_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse application epoch lifecycle output");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stdout)
    );
    assert!(summary.completed_without_error);
    assert!(summary.process_finished);
    assert!(summary.distinct_states >= 4);
    assert!(summary.search_depth >= 4);
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn self_removing_leader_commits_final_configuration_and_steps_down() {
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

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn missing_self_removal_step_down_breaks_commit_regression() {
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

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn unfrozen_effective_membership_breaks_commit_witness_regression() {
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

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn corrupted_snapshot_install_breaks_lifecycle_identity() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "InstallSnapshotLog(node, prefix)",
        "InstallSnapshot",
        "<<Entry(prefix[1].term, CHOOSE value \\in Values : value # prefix[1].input)>>",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "corrupted-snapshot-install",
        &mutated,
        &detector,
        SNAPSHOT_LIFECYCLE_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse corrupted snapshot output");
    assert_eq!(result.status.code(), Some(12));
    assert_eq!(
        summary.violated_invariant.as_deref(),
        Some("SnapshotLifecycleInvariant")
    );
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn true_mutation_of_real_predicate_cannot_qualify() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "ElectionSafety",
        "LogMatchingFor(logs, snapshotIndexes, snapshotPrefixes)",
        "TRUE",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_mutation(&root, "true-predicate", &mutated, &detector, ELECTION_PROBE);
    let summary = parse(&result.stdout).expect("parse TLC mutation output");
    assert!(result.status.success());
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        "ElectionSafety"
    ));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn non_violating_fixture_cannot_qualify() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let non_violating = replace_operator(&detector, "FixtureNext", "FixtureSpec", "UNCHANGED vars");
    let result = run_tlc_mutation(
        &root,
        "non-violating-fixture",
        &raft,
        &non_violating,
        ELECTION_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse TLC mutation output");
    assert!(result.status.success());
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        "ElectionSafety"
    ));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn applied_membership_quorum_mutation_breaks_joint_regression() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_exactly_once(
        &raft,
        "MembershipQuorum(\n         preEffectiveMembership, MatchingReplicas(n, i))",
        "MembershipQuorum(membership, MatchingReplicas(n, i))",
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

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn missing_effective_recomputation_breaks_overwrite_regression() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "EffectiveConfigurationFor(entries)",
        "LogicalPrefixFrom(logs, snapshotIndexes, snapshotPrefixes, node, index)",
        "[configIndex |-> effectiveConfigIndex, config |-> effectiveMembership]",
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
    assert_eq!(summary.violated_invariant.as_deref(), Some("TypeOK"));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn follower_recomputation_breaks_delayed_heartbeat_regression() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "AuthoritativeLogReplacement(message, accepted)",
        "RecordElection(node)",
        "accepted",
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

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn missing_higher_term_recorder_cannot_qualify_fencing() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordHigherTermOutcome(node, evidenceTerm, observedHigherTerm)",
        "RecordAuthorityAcceptance(authorityTerm, knownTerm, accepted)",
        "/\\ UNCHANGED higherTermStepDownFailed",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_mutation(
        &root,
        "missing-higher-term-recorder",
        &mutated,
        &detector,
        HIGHER_TERM_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse TLC mutation output");
    assert!(result.status.success());
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        HIGHER_TERM_PROBE.predicate
    ));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn missing_stale_authority_recorder_cannot_qualify_fencing() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordAuthorityAcceptance(authorityTerm, knownTerm, accepted)",
        "RecordApplication(node, index, entry, priorState, resultState)",
        "/\\ UNCHANGED staleAuthorityAccepted",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_mutation(
        &root,
        "missing-stale-authority-recorder",
        &mutated,
        &detector,
        STALE_AUTHORITY_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse TLC mutation output");
    assert!(result.status.success());
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        STALE_AUTHORITY_PROBE.predicate
    ));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn missing_election_recorder_cannot_qualify_election_safety() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordElection(node)",
        "RecordHigherTermOutcome(node, evidenceTerm, observedHigherTerm)",
        "/\\ UNCHANGED electedLeaders",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_mutation(
        &root,
        "missing-election-recorder",
        &mutated,
        &detector,
        ELECTION_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse TLC mutation output");
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        ELECTION_PROBE.predicate
    ));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn missing_application_recorder_cannot_qualify_state_machine_safety() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordApplication(node, index, entry, priorState, resultState)",
        "RequestVoteMessages",
        "/\\ UNCHANGED applied",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    for (name, probe) in [
        ("missing-application-recorder", APPLICATION_PROBE),
        (
            "missing-application-epoch-recorder",
            APPLICATION_EPOCH_PROBE,
        ),
    ] {
        let result = run_tlc_mutation(&root, name, &mutated, &detector, probe);
        let summary = parse(&result.stdout).expect("parse TLC mutation output");
        assert!(!detector_qualified(
            result.status.code(),
            false,
            Some(&summary),
            probe.predicate
        ));
    }
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn missing_log_prefix_recorder_cannot_qualify_log_or_snapshot_paths() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordLogicalPrefixes(logs, snapshotIndexes, snapshotPrefixes)",
        "LogicalPrefixLedgerSound",
        "/\\ UNCHANGED logicalPrefixLedger",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    for (name, probe) in [
        ("missing-log-matching-recorder", LOG_MATCHING_PROBE),
        ("missing-snapshot-prefix-recorder", SNAPSHOT_PREFIX_PROBE),
    ] {
        let result = run_tlc_mutation(&root, name, &mutated, &detector, probe);
        let summary = parse(&result.stdout).expect("parse TLC mutation output");
        assert!(result.status.success());
        assert!(!detector_qualified(
            result.status.code(),
            false,
            Some(&summary),
            probe.predicate
        ));
    }
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn missing_commit_ledger_recorder_cannot_qualify_history_predicates() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordCommittedEntries(\n    logs, snapshotIndexes, snapshotPrefixes, node, oldFloor, newFloor)",
        "ConfigurationMembershipAt(\n    logs, snapshotIndexes, snapshotPrefixes, node, configIndex)",
        "/\\ UNCHANGED committedLedger",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    for (name, probe) in [
        (
            "missing-leader-completeness-recorder",
            LEADER_COMPLETENESS_PROBE,
        ),
        ("missing-committed-prefix-recorder", COMMITTED_PREFIX_PROBE),
    ] {
        let result = run_tlc_mutation(&root, name, &mutated, &detector, probe);
        let summary = parse(&result.stdout).expect("parse TLC mutation output");
        assert!(result.status.success());
        assert!(!detector_qualified(
            result.status.code(),
            false,
            Some(&summary),
            probe.predicate
        ));
    }
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn missing_commit_witness_recorder_cannot_qualify_quorum_predicate() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordCommitWitnesses(witnesses)",
        "ReadGrantOK(grant)",
        "UNCHANGED commitWitnesses",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_mutation(
        &root,
        "missing-commit-witness-recorder",
        &mutated,
        &detector,
        COMMIT_QUORUM_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse TLC mutation output");
    assert!(result.status.success());
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        COMMIT_QUORUM_PROBE.predicate
    ));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn unvalidated_commit_certificate_cannot_qualify_quorum_predicate() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordCommitWitnesses(witnesses)",
        "ReadGrantOK(grant)",
        "commitWitnesses' = CommitWitnessHistory(\n  commitWitnesses.witnessedCommits \\cup CommitWitnessKeys(witnesses),\n  commitWitnesses.invalidCertificateSeen)",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_mutation(
        &root,
        "unvalidated-commit-certificate",
        &mutated,
        &detector,
        COMMIT_QUORUM_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse TLC mutation output");
    assert!(result.status.success());
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        COMMIT_QUORUM_PROBE.predicate
    ));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn missing_read_grant_recorder_cannot_qualify_read_barrier_predicate() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordReadGrant(grant)",
        "CanAdoptLog(n, entries)",
        "/\\ UNCHANGED readBarrierViolationSeen",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_mutation(
        &root,
        "missing-read-grant-recorder",
        &mutated,
        &detector,
        READ_BARRIER_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse TLC mutation output");
    assert!(result.status.success());
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        READ_BARRIER_PROBE.predicate
    ));
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn unvalidated_read_grant_cannot_qualify_read_barrier_predicate() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "ReadGrantOK(grant)",
        "RecordReadGrant(grant)",
        "TRUE",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_mutation(
        &root,
        "unvalidated-read-grant",
        &mutated,
        &detector,
        READ_BARRIER_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse TLC mutation output");
    assert!(result.status.success());
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        READ_BARRIER_PROBE.predicate
    ));
}

fn workspace_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn replace_operator(source: &str, operator: &str, next: &str, body: &str) -> String {
    let start = format!("{operator} ==");
    let end = format!("{next} ==");
    let (prefix, rest) = source.split_once(&start).expect("operator exists");
    let (_, suffix) = rest.split_once(&end).expect("next operator exists");
    format!("{prefix}{operator} == {body}\n\n{end}{suffix}")
}

fn replace_exactly_once(source: &str, from: &str, to: &str) -> String {
    assert_eq!(source.matches(from).count(), 1, "mutation target is exact");
    source.replacen(from, to, 1)
}

fn run_tlc_mutation(
    root: &Path,
    name: &str,
    raft: &str,
    detector: &str,
    probe: DetectorProbe,
) -> std::process::Output {
    let template =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.cfg"))
            .expect("read detector config");
    let config = render_detector_config(&template, probe).expect("render detector config");
    run_tlc_with_config(root, name, raft, detector, &config)
}

fn run_tlc_with_config(
    root: &Path,
    name: &str,
    raft: &str,
    detector: &str,
    config: &str,
) -> std::process::Output {
    let directory = root
        .join("target/rafter-invariants/tla-mutations")
        .join(format!("{}-{name}", std::process::id()));
    if directory.exists() {
        fs::remove_dir_all(&directory).expect("remove stale mutation directory");
    }
    fs::create_dir_all(&directory).expect("create mutation directory");
    fs::write(directory.join("Raft.tla"), raft).expect("write mutated Raft spec");
    fs::write(
        directory.join("RafterInvariantDetectorNegative.tla"),
        detector,
    )
    .expect("write mutated detector spec");
    fs::write(
        directory.join("RafterInvariantDetectorNegative.cfg"),
        config,
    )
    .expect("write detector config");
    Command::new("java")
        .args([
            "-XX:+UseParallelGC",
            "-cp",
            &root.join("tools/cache/tla2tools.jar").to_string_lossy(),
            "tlc2.TLC",
            "-tool",
            "-workers",
            "1",
            "-seed",
            "2026071101",
            "-fp",
            "0",
            "-metadir",
            &directory.join("states").to_string_lossy(),
            "-config",
            "RafterInvariantDetectorNegative.cfg",
            "RafterInvariantDetectorNegative.tla",
        ])
        .current_dir(&directory)
        .output()
        .expect("run TLC mutation")
}
