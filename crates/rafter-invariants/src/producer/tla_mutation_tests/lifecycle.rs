use std::fs;

use super::super::detector_qualified;
use super::support::*;
use crate::producer::tla_output::parse;

pub(super) fn snapshot_lifecycle_preserves_logical_identity_through_restart() {
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

    let six_entry = run_tlc_with_config(
        &root,
        "six-entry-application-replay",
        &raft,
        &detector,
        SIX_ENTRY_REPLAY_CONFIG,
    );
    assert!(
        six_entry.status.success(),
        "{}",
        String::from_utf8_lossy(&six_entry.stdout)
    );

    let four_entry_only = replace_exactly_once_in_operator(
        &raft,
        "StateAfterEntries(entries)",
        "AppliedObservation(entry, resultState)",
        "[] Len(entries) = 5 ->\n         ApplyEntry(StateAfterFourEntries(entries), entries[5])\n    [] OTHER ->\n         ApplyEntry(\n           ApplyEntry(StateAfterFourEntries(entries), entries[5]), entries[6])",
        "[] OTHER -> StateAfterFourEntries(entries)",
    );
    let mutation = run_tlc_with_config(
        &root,
        "four-entry-only-application-replay-mutation",
        &four_entry_only,
        &detector,
        SIX_ENTRY_REPLAY_CONFIG,
    );
    assert_eq!(mutation.status.code(), Some(151));
    assert!(String::from_utf8_lossy(&mutation.stdout)
        .contains("The invariant of SixEntryReplayInvariant is equal to FALSE"));
}

pub(super) fn stale_messages_are_retired_when_the_target_term_advances() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let baseline = run_tlc_with_config(
        &root,
        "stale-message-lifecycle",
        &raft,
        &detector,
        STALE_MESSAGE_LIFECYCLE_CONFIG,
    );
    let baseline_summary = parse(&baseline.stdout).expect("parse stale-message lifecycle output");
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stdout)
    );
    assert!(baseline_summary.completed_without_error);
    assert!(baseline_summary.process_finished);
    assert!(baseline_summary.distinct_states >= 4);
    assert!(baseline_summary.search_depth >= 4);

    let mutated = replace_exactly_once_in_operator(
        &raft,
        "Timeout(n)",
        "SendRequestVote(c, v)",
        "/\\ messages' = RetainedMessages(messages, currentTerm')",
        "/\\ messages' = messages",
    );
    let mutation = run_tlc_with_config(
        &root,
        "stale-message-timeout-retirement-missing",
        &mutated,
        &detector,
        STALE_MESSAGE_LIFECYCLE_CONFIG,
    );
    let mutation_summary = parse(&mutation.stdout).expect("parse stale-message mutation output");
    assert_eq!(mutation.status.code(), Some(12));
    assert_eq!(
        mutation_summary.violated_invariant.as_deref(),
        Some("StaleMessageLifecycleInvariant")
    );
}

pub(super) fn closed_term_election_history_is_retired_after_every_node_advances() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let baseline = run_tlc_with_config(
        &root,
        "closed-election-lifecycle",
        &raft,
        &detector,
        CLOSED_ELECTION_LIFECYCLE_CONFIG,
    );
    let baseline_summary = parse(&baseline.stdout).expect("parse closed-election lifecycle output");
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stdout)
    );
    assert!(baseline_summary.completed_without_error);
    assert!(baseline_summary.process_finished);
    assert!(baseline_summary.distinct_states >= 4);
    assert!(baseline_summary.search_depth >= 4);

    let mutated = replace_exactly_once_in_operator(
        &raft,
        "Timeout(n)",
        "SendRequestVote(c, v)",
        "/\\ electedLeaders' = RetainedElections(electedLeaders, currentTerm')",
        "/\\ electedLeaders' = electedLeaders",
    );
    let mutation = run_tlc_with_config(
        &root,
        "closed-election-timeout-retirement-missing",
        &mutated,
        &detector,
        CLOSED_ELECTION_LIFECYCLE_CONFIG,
    );
    let mutation_summary = parse(&mutation.stdout).expect("parse closed-election mutation output");
    assert_eq!(mutation.status.code(), Some(12));
    assert_eq!(
        mutation_summary.violated_invariant.as_deref(),
        Some("ClosedElectionLifecycleInvariant")
    );
}

pub(super) fn snapshot_compaction_pending_tracks_create_and_compact_transitions() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let mutations = [
        (
            "snapshot-create-does-not-mark-compaction-pending",
            "/\\ compactionPending' = [compactionPending EXCEPT ![n] = TRUE]",
            "/\\ compactionPending' = [compactionPending EXCEPT ![n] = FALSE]",
        ),
        (
            "snapshot-compact-does-not-clear-compaction-pending",
            "/\\ compactionPending' = [compactionPending EXCEPT ![n] = FALSE]",
            "/\\ compactionPending' = [compactionPending EXCEPT ![n] = TRUE]",
        ),
    ];

    for (name, source, replacement) in mutations {
        let mutated = replace_exactly_once(&raft, source, replacement);
        let result =
            run_tlc_with_config(&root, name, &mutated, &detector, SNAPSHOT_LIFECYCLE_CONFIG);
        let summary = parse(&result.stdout).expect("parse snapshot compaction mutation output");
        assert_eq!(result.status.code(), Some(13), "{name} unexpectedly passed");
        assert!(!summary.completed_without_error, "{name} qualified");
        assert!(
            String::from_utf8_lossy(&result.stdout).contains("SnapshotLifecycleCompletes"),
            "{name} did not fail the lifecycle property: {}",
            String::from_utf8_lossy(&result.stdout)
        );
    }
}

pub(super) fn application_epoch_loss_replays_identically_without_erasing_history() {
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

pub(super) fn missing_application_epoch_recorder_cannot_qualify_state_machine_safety() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_operator(
        &raft,
        "StartApplicationEpoch(node, baseIndex, baseState)",
        "RecordApplication(node, entry, resultState)",
        "/\\ UNCHANGED applied",
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

pub(super) fn corrupted_snapshot_install_breaks_lifecycle_identity() {
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

pub(super) fn corrupted_snapshot_restored_state_breaks_empty_epoch_lifecycle() {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let mutated = replace_exactly_once_in_operator(
        &raft,
        "InstallSnapshot",
        "CompactSnapshot(n)",
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
