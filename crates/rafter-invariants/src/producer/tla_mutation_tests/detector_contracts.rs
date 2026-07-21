//! TLA+ detector recorder and predicate-mutation contract scenarios.

use std::fs;

use super::super::detector_qualified;
use super::support::*;
use crate::producer::tla_output::{parse, DETECTOR_PROBES};

pub(super) fn recorder_only_fixtures_qualify_before_mutation() {
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

pub(super) fn every_required_detector_probe_reaches_its_named_counterexample() {
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

pub(super) fn true_mutation_of_real_predicate_cannot_qualify() {
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

pub(super) fn non_violating_fixture_cannot_qualify() {
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
