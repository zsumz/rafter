//! Lifecycle scenarios for snapshots and application epochs.

use std::fs;

use super::super::super::detector_qualified;
use super::super::support::*;
use crate::producer::tla_output::parse;

/// Replaces the pair that used to mutate `compactionPending` in `CreateSnapshot`
/// and `CompactSnapshot`. Those two actions are now one atomic action and the
/// flag no longer exists, so neither original mutation is expressible. What the
/// pair asserted was that the snapshot lifecycle is a handshake whose halves
/// both have their effect: creation records that compaction is owed, compaction
/// discharges it. Folded, the same contract is that the one action has both of
/// its effects at once -- it advances the snapshot floor, and it retains the
/// ghost logical history the floor now indexes into. The substitutes break one
/// each. The first stalls the lifecycle exactly as the old first mutation did
/// and is caught by the same liveness property; the second is caught as an
/// invariant violation, because atomicity leaves no half-completed state for a
/// liveness property to get stuck in.
pub(in crate::producer::tla_exec::mutation_tests) fn snapshot_creation_atomically_advances_and_retains_the_snapshot_floor(
) {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");

    // The snapshot floor never advances, so `SnapshotLifecycleNext` can never
    // leave its first disjunct and the lifecycle never completes.
    let stalled = replace_exactly_once(
        &raft,
        "/\\ snapshotIndex' = [snapshotIndex EXCEPT ![n] = index]",
        "/\\ snapshotIndex' = snapshotIndex",
    );
    let result = run_tlc_with_config(
        &root,
        "snapshot-create-does-not-advance-the-snapshot-floor",
        &stalled,
        &detector,
        SNAPSHOT_LIFECYCLE_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse stalled snapshot creation output");
    assert_eq!(result.status.code(), Some(13), "stalled mutation passed");
    assert!(
        !summary.completed_without_error,
        "stalled mutation qualified"
    );
    assert!(
        String::from_utf8_lossy(&result.stdout).contains("SnapshotLifecycleCompletes"),
        "stalled mutation did not fail the lifecycle property: {}",
        String::from_utf8_lossy(&result.stdout)
    );

    // The folded action performs physical compaction instead of retaining the
    // ghost history, so the snapshot floor indexes past the end of the log.
    let truncated = replace_exactly_once(
        &raft,
        "    /\\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,\n                    snapshotTransfer, messages,",
        "    /\\ log' = [log EXCEPT ![n] = SubSeq(@, index + 1, Len(@))]\n    /\\ UNCHANGED <<currentTerm, votedFor, role, commitIndex,\n                    snapshotTransfer, messages,",
    );
    let result = run_tlc_with_config(
        &root,
        "snapshot-create-does-not-retain-the-ghost-history",
        &truncated,
        &detector,
        SNAPSHOT_LIFECYCLE_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse truncating snapshot creation output");
    assert_eq!(result.status.code(), Some(12), "truncating mutation passed");
    // `TypeOK` and not `SnapshotLifecycleInvariant`: the bound this breaks is
    // `snapshotIndex[n] <= Len(log[n])`, which both predicates carry, and the
    // config checks `TypeOK` first.
    assert_eq!(summary.violated_invariant.as_deref(), Some("TypeOK"));
}

pub(in crate::producer::tla_exec::mutation_tests) fn application_epoch_loss_replays_identically_without_erasing_history(
) {
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

    let cleared_history = replace_exactly_once_in_operator(
        &raft,
        "StartApplicationEpoch(node, baseIndex, baseState)",
        "RecordApplication(node, entry, resultState)",
        "/\\ UNCHANGED applicationTransitions",
        "/\\ applicationTransitions' = {}",
    );
    let mutation = run_tlc_with_config(
        &root,
        "application-epoch-clears-history",
        &cleared_history,
        &detector,
        APPLICATION_EPOCH_LIFECYCLE_CONFIG,
    );
    let mutation_summary =
        parse(&mutation.stdout).expect("parse application-history clearing output");
    assert_eq!(mutation.status.code(), Some(12));
    assert_eq!(
        mutation_summary.violated_invariant.as_deref(),
        Some("ApplicationEpochLifecycleInvariant")
    );
}

pub(in crate::producer::tla_exec::mutation_tests) fn missing_application_epoch_recorder_cannot_qualify_state_machine_safety(
) {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "StartApplicationEpoch(node, baseIndex, baseState)",
        "RecordApplication(node, entry, resultState)",
        "/\\ UNCHANGED applicationVars",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_mutation(
        &root,
        "missing-application-epoch-recorder",
        &mutated,
        &detector,
        APPLICATION_EPOCH_PROBE,
    );
    let summary = parse(&result.stdout).expect("parse application epoch recorder mutation output");
    assert!(result.status.success());
    assert!(!detector_qualified(
        result.status.code(),
        false,
        Some(&summary),
        APPLICATION_EPOCH_PROBE.predicate
    ));
}

pub(in crate::producer::tla_exec::mutation_tests) fn corrupted_snapshot_install_breaks_lifecycle_identity(
) {
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

pub(in crate::producer::tla_exec::mutation_tests) fn corrupted_snapshot_restored_state_breaks_empty_epoch_lifecycle(
) {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_exactly_once_in_operator(
        &raft,
        "InstallSnapshot",
        "EnterJoint(n, newVoters)",
        "restoredState == StateAfterEntries(transfer.prefix)",
        "restoredState == InitialApplicationState",
    );
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let result = run_tlc_with_config(
        &root,
        "corrupted-snapshot-restored-state",
        &mutated,
        &detector,
        SNAPSHOT_LIFECYCLE_CONFIG,
    );
    let summary = parse(&result.stdout).expect("parse corrupted snapshot state output");
    assert_eq!(result.status.code(), Some(12));
    assert_eq!(
        summary.violated_invariant.as_deref(),
        Some("SnapshotLifecycleInvariant")
    );
}
