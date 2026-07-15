use std::fs;

use super::super::detector_qualified;
use super::support::*;
use crate::producer::tla_output::parse;

pub(super) fn missing_higher_term_recorder_cannot_qualify_fencing() {
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

pub(super) fn missing_stale_authority_recorder_cannot_qualify_fencing() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordAuthorityAcceptance(authorityTerm, knownTerm, accepted)",
        "RecordAppendOutcome(message, knownTerm, accepted, receiverWouldAccept)",
        "/\\ UNCHANGED <<staleAuthorityAccepted, frozenAppendAuthorityFailed>>",
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

pub(super) fn missing_election_recorder_cannot_qualify_election_safety() {
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

pub(super) fn missing_application_recorder_cannot_qualify_state_machine_safety() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordApplication(node, entry, resultState)",
        "CommittedEntry(index, entry, committedInTerm)",
        "/\\ UNCHANGED applicationVars",
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

pub(super) fn sanitized_application_result_cannot_qualify_detector_fixture() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let sanitized_transition = replace_exactly_once_in_operator(
        &raft,
        "RecordApplication(node, entry, resultState)",
        "CommittedEntry(index, entry, committedInTerm)",
        "node, index, ApplicationState(node), entry, resultState)",
        "node, index, ApplicationState(node), entry,\n        ApplyEntry(ApplicationState(node), entry))",
    );
    let sanitized = replace_exactly_once_in_operator(
        &sanitized_transition,
        "RecordApplication(node, entry, resultState)",
        "CommittedEntry(index, entry, committedInTerm)",
        "AppliedCursor(ApplicationEpoch(node), index, resultState)",
        "AppliedCursor(\n           ApplicationEpoch(node), index,\n           ApplyEntry(ApplicationState(node), entry))",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_mutation(
        &root,
        "sanitized-application-result",
        &sanitized,
        &detector,
        APPLICATION_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse sanitized application output");
    assert!(result.status.success());
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        APPLICATION_PROBE.predicate
    ));
}

pub(super) fn missing_log_prefix_recorder_cannot_qualify_log_or_snapshot_paths() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordLogicalPrefixes(logs, snapshotIndexes, snapshotPrefixes, terms)",
        "RetireLogicalPrefixes(terms)",
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

pub(super) fn missing_commit_ledger_recorder_cannot_qualify_history_predicates() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordCommittedEntries(\n    logs, snapshotIndexes, snapshotPrefixes, node, oldFloor, newFloor,\n    committedInTerm)",
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

pub(super) fn missing_commit_witness_recorder_cannot_qualify_quorum_predicate() {
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

pub(super) fn unvalidated_commit_certificate_cannot_qualify_quorum_predicate() {
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

pub(super) fn missing_read_grant_recorder_cannot_qualify_read_barrier_predicate() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "RecordReadGrant(grant)",
        "CanAdoptLog(n, entries, authorityTerm)",
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

pub(super) fn unvalidated_read_grant_cannot_qualify_read_barrier_predicate() {
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
