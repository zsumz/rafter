//! Lifecycle scenarios for retained protocol history.

use std::fs;

use super::super::support::*;
use crate::producer::tla_output::parse;

pub(in crate::producer::tla_exec::mutation_tests) fn snapshot_lifecycle_preserves_logical_identity_through_restart(
) {
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
    // Was 7. Folding snapshot creation and compaction into one atomic action
    // removed the one intermediate state this lifecycle used to pass through --
    // measured 7/7 before the fold and 6/6 after, which is the fold's claim
    // stated as a number.
    assert!(summary.distinct_states >= 6);
    assert!(summary.search_depth >= 6);

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
        "AppliedCursor(epoch, through, state)",
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

pub(in crate::producer::tla_exec::mutation_tests) fn stale_messages_are_retired_when_the_target_term_advances(
) {
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

pub(in crate::producer::tla_exec::mutation_tests) fn closed_term_election_history_is_retired_after_every_node_advances(
) {
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

pub(in crate::producer::tla_exec::mutation_tests) fn closed_term_prefix_history_retires_without_erasing_conflicts(
) {
    let root = workspace_root();
    let raft = fs::read_to_string(root.join("specs/tla/raft/Raft.tla")).expect("read Raft spec");
    let detector =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.tla"))
            .expect("read detector spec");
    let baseline = run_tlc_with_config(
        &root,
        "closed-logical-prefix-lifecycle",
        &raft,
        &detector,
        CLOSED_LOGICAL_PREFIX_LIFECYCLE_CONFIG,
    );
    let baseline_summary = parse(&baseline.stdout).expect("parse closed-prefix lifecycle output");
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stdout)
    );
    assert!(baseline_summary.completed_without_error);
    assert!(baseline_summary.process_finished);
    assert!(baseline_summary.distinct_states >= 4);
    assert!(baseline_summary.search_depth >= 4);

    let missing_retirement = replace_exactly_once_in_operator(
        &raft,
        "Timeout(n)",
        "SendRequestVote(c, v)",
        "/\\ RetireLogicalPrefixes(currentTerm')",
        "/\\ UNCHANGED logicalPrefixLedger",
    );
    let missing_result = run_tlc_with_config(
        &root,
        "closed-logical-prefix-retirement-missing",
        &missing_retirement,
        &detector,
        CLOSED_LOGICAL_PREFIX_LIFECYCLE_CONFIG,
    );
    let missing_summary =
        parse(&missing_result.stdout).expect("parse missing prefix-retirement output");
    assert_eq!(missing_result.status.code(), Some(12));
    assert_eq!(
        missing_summary.violated_invariant.as_deref(),
        Some("ClosedLogicalPrefixLifecycleInvariant")
    );

    let conflict = run_tlc_with_config(
        &root,
        "closed-term-prefix-conflict",
        &raft,
        &detector,
        CLOSED_TERM_PREFIX_CONFLICT_CONFIG,
    );
    let conflict_summary =
        parse(&conflict.stdout).expect("parse closed-term prefix conflict output");
    assert_eq!(conflict.status.code(), Some(12));
    assert_eq!(
        conflict_summary.violated_invariant.as_deref(),
        Some("ClosedTermPrefixConflictInvariant")
    );

    let naive_retirement = replace_operator(
        &raft,
        "RetainedLogicalPrefixes(observed, terms)",
        "RecordLogicalPrefixes(logs, snapshotIndexes, terms)",
        "{witness \\in observed : ~TermClosed(terms, witness.term)}",
    );
    let erased_conflict = run_tlc_with_config(
        &root,
        "closed-term-prefix-conflict-erased",
        &naive_retirement,
        &detector,
        CLOSED_TERM_PREFIX_CONFLICT_CONFIG,
    );
    let erased_summary =
        parse(&erased_conflict.stdout).expect("parse erased prefix-conflict output");
    assert!(
        erased_conflict.status.success(),
        "{}",
        String::from_utf8_lossy(&erased_conflict.stdout)
    );
    assert!(erased_summary.completed_without_error);
    assert!(erased_summary.process_finished);
}
