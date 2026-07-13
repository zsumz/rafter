use std::{fs, path::Path, process::Command};

use super::detector_qualified;
use crate::producer::tla_output::{parse, render_detector_config, DetectorProbe};

const ELECTION_PROBE: DetectorProbe = DetectorProbe {
    predicate: "ElectionSafety",
    mode: "ElectionRecorderOnly",
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
        ("higher-term-recorder-baseline", HIGHER_TERM_PROBE),
        ("stale-authority-recorder-baseline", STALE_AUTHORITY_PROBE),
        ("application-recorder-baseline", APPLICATION_PROBE),
    ] {
        let result = run_tlc_mutation(&root, name, &raft, &detector, probe);
        let summary = parse(&result.stdout).expect("parse TLC recorder baseline output");
        assert!(detector_qualified(
            result.status.code(),
            false,
            Some(&summary),
            probe.predicate
        ));
    }
}

#[test]
#[ignore = "requires the pinned TLC tool and Java"]
fn true_mutation_of_real_predicate_cannot_qualify() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(&raft, "ElectionSafety", "LogMatching", "TRUE");
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
    let mutated = replace_operator(
        &raft,
        "QuorumNodes(ns)",
        "MatchingReplicas(n, i)",
        "MembershipQuorum(membership, ns)",
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
        "AuthoritativeLogReplacement(message, accepted)",
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
        "/\\ UNCHANGED << higherTermEvidenceSeen, higherTermStepDownFailed >>",
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
    let result = run_tlc_mutation(
        &root,
        "missing-application-recorder",
        &mutated,
        &detector,
        APPLICATION_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse TLC mutation output");
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        APPLICATION_PROBE.predicate
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
