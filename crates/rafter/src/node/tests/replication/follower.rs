//! Follower `AppendEntries` matching, conflict repair, and commit safety.

use super::support::*;
use super::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

#[test]
fn follower_accepts_current_term_heartbeat() {
    let mut follower = node(2, &[1, 3]);

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(3),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert_eq!(follower.role(), Role::Follower);
    assert_eq!(follower.current_term(), Term(3));
    assert_append_entries_response(&outputs, NodeId(1), true, LogIndex::ZERO);
}

#[test]
fn follower_rejects_stale_heartbeat() {
    let mut follower = node(2, &[1, 3]);
    follower.become_follower(Term(4));

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(3),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert_eq!(follower.current_term(), Term(4));
    assert_append_entries_response(&outputs, NodeId(1), false, LogIndex::ZERO);
}

#[test]
fn follower_rejects_append_entries_when_sender_disagrees_with_leader_id() {
    let mut follower = node(1, &[2, 3]);

    let outputs = follower.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(7),
            leader_id: NodeId(3),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(follower.current_term(), Term::default());
    assert_eq!(follower.role(), Role::Follower);
}

#[test]
fn follower_appends_entries_after_matching_prefix() {
    let mut follower = node(2, &[1, 3]);

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: vec![LogEntry::application(Term(2), b"first".to_vec())].into(),
            leader_commit: LogIndex(1),
        }),
    });

    assert_eq!(follower.last_log_index(), LogIndex(1));
    assert_eq!(follower.commit_index(), LogIndex(1));
    assert_eq!(
        outputs,
        vec![
            Output::Apply {
                index: LogIndex(1),
                term: Term(2),
                payload: b"first".to_vec().into(),
                local_proposal_id: None,
            },
            Output::Send {
                to: NodeId(1),
                message: Message::AppendEntriesResponse(AppendEntriesResponse {
                    sequence: 0,
                    term: Term(2),
                    follower_id: NodeId(2),
                    success: true,
                    match_index: LogIndex(1),
                }),
            },
        ]
    );
}

#[test]
fn follower_append_response_reports_last_request_entry_index() {
    let mut follower = node(2, &[1, 3]);
    push_log_entry(&mut follower, Term(2), b"prefix");

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(3),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(2),
            entries: vec![
                LogEntry::application(Term(3), b"second".to_vec()),
                LogEntry::application(Term(3), b"third".to_vec()),
            ]
            .into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert_eq!(follower.last_log_index(), LogIndex(3));
    assert_append_entries_response(&outputs, NodeId(1), true, LogIndex(3));
}

#[test]
fn follower_empty_heartbeat_response_reports_prev_log_index_not_tail() {
    let mut follower = node(2, &[1, 3]);
    push_log_entry(&mut follower, Term(2), b"prefix");
    push_log_entry(&mut follower, Term(99), b"divergent-tail");

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(3),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(2),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert_append_entries_response(&outputs, NodeId(1), true, LogIndex(1));
}

#[test]
fn follower_rejects_append_entries_with_mismatched_previous_log() {
    let mut follower = node(2, &[1, 3]);
    push_log_entry(&mut follower, Term(2), b"local-tail");

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(2),
            prev_log_term: Term(2),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert_append_entries_response(&outputs, NodeId(1), false, LogIndex::ZERO);
}

#[test]
fn follower_truncates_divergent_uncommitted_suffix() {
    let mut follower = node(2, &[1, 3]);
    push_log_entry(&mut follower, Term(2), b"old");
    push_log_entry(&mut follower, Term(2), b"divergent");

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(3),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(2),
            entries: vec![LogEntry::application(Term(3), b"replacement".to_vec())].into(),
            leader_commit: LogIndex(2),
        }),
    });

    assert_eq!(follower.last_log_index(), LogIndex(2));
    assert_eq!(
        follower
            .entry_at(LogIndex(2))
            .and_then(LogEntry::application_payload),
        Some(&b"replacement"[..])
    );
    assert_eq!(
        outputs,
        vec![
            Output::Apply {
                index: LogIndex(1),
                term: Term(2),
                payload: b"old".to_vec().into(),
                local_proposal_id: None,
            },
            Output::Apply {
                index: LogIndex(2),
                term: Term(3),
                payload: b"replacement".to_vec().into(),
                local_proposal_id: None,
            },
            Output::Send {
                to: NodeId(1),
                message: Message::AppendEntriesResponse(AppendEntriesResponse {
                    sequence: 0,
                    term: Term(3),
                    follower_id: NodeId(2),
                    success: true,
                    match_index: LogIndex(2),
                }),
            },
        ]
    );
}

#[test]
fn follower_rejects_append_that_would_truncate_committed_entry() {
    let mut follower = node(2, &[1, 3]);
    push_log_entry(&mut follower, Term(2), b"committed");
    follower.volatile.commit_index = LogIndex(1);
    follower.volatile.applied_index = LogIndex(1);

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(3),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: vec![LogEntry::application(Term(3), b"replacement".to_vec())].into(),
            leader_commit: LogIndex(1),
        }),
    });

    oracle_assert_eq!(follower.last_log_index(), LogIndex(1));
    oracle_assert_eq!(
        follower
            .entry_at(LogIndex(1))
            .and_then(LogEntry::application_payload),
        Some(&b"committed"[..])
    );
    oracle_assert_eq!(follower.commit_index(), LogIndex(1));
    oracle_assert!(append_response_matches(
        &outputs,
        NodeId(1),
        false,
        LogIndex::ZERO
    ));
}

/// An empty pipelined append (probe retry or keep-alive) confirms nothing
/// beyond its prev entry: a follower holding an uncommitted divergent
/// suffix above the confirmed prefix must not commit — let alone apply —
/// its own garbage on the strength of the leader's commit index alone
/// (figure 2: min(leaderCommit, index of last new entry)).
#[test]
fn empty_append_never_commits_the_followers_unconfirmed_suffix() {
    let mut follower = node(2, &[1, 3]);
    let accepted = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            term: Term(1),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            sequence: 1,
            entries: vec![
                LogEntry::application(Term(1), b"shared".to_vec()),
                LogEntry::application(Term(1), b"garbage".to_vec()),
            ]
            .into(),
            leader_commit: LogIndex(0),
        }),
    });
    assert_append_entries_response(&accepted, NodeId(1), true, LogIndex(2));

    // A term-4 leader whose log agreed only through index 1 committed a
    // DIFFERENT entry at index 2; its keep-alive to this lagging follower
    // carries prev = 1, no entries, and leader_commit = 2.
    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            term: Term(4),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(1),
            sequence: 1,
            entries: Vec::new().into(),
            leader_commit: LogIndex(2),
        }),
    });

    assert_eq!(
        follower.commit_index(),
        LogIndex(1),
        "the empty frame confirmed the log only through its prev entry"
    );
    // Index 1 is genuinely confirmed by this frame and applies; the
    // divergent local entry at index 2 must never apply.
    assert!(outputs.iter().any(|output| matches!(
        output,
        Output::Apply {
            index: LogIndex(1),
            ..
        }
    )));
    assert!(!outputs.iter().any(|output| matches!(
        output,
        Output::Apply {
            index: LogIndex(2),
            ..
        }
    )));
}

fn append_response_matches(
    outputs: &[Output],
    to: NodeId,
    success: bool,
    match_index: LogIndex,
) -> bool {
    matches!(
        outputs,
        [Output::Send {
            to: actual_to,
            message: Message::AppendEntriesResponse(response),
        }] if *actual_to == to
            && response.success == success
            && response.match_index == match_index
    )
}

/// Commit-index monotonicity (found by the `node_message_sequences` fuzzer):
/// a leader probing back to a low prev index sends an empty append that
/// confirms little, but its higher `leader_commit` must never drag an
/// already-committed index backwards.
#[test]
fn a_probe_with_a_high_leader_commit_never_regresses_the_commit_index() {
    let mut follower = node(2, &[1, 3]);
    // Commit index 1 legitimately: a frame that appends and confirms it.
    let _ = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            term: Term(1),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            sequence: 1,
            entries: vec![LogEntry::application(Term(1), b"one".to_vec())].into(),
            leader_commit: LogIndex(1),
        }),
    });
    assert_eq!(follower.commit_index(), LogIndex(1));

    // A probe walked back to prev = 0 with an empty batch, still carrying
    // the leader's real (higher) commit index. match_index for this frame
    // is 0; the naive min(leader_commit, match_index) would regress commit
    // to 0.
    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            term: Term(1),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            sequence: 2,
            entries: Vec::new().into(),
            leader_commit: LogIndex(3),
        }),
    });
    oracle_assert_eq!(
        follower.commit_index(),
        LogIndex(1),
        "an empty probe never un-commits an already-committed index"
    );
    oracle_assert!(!outputs.iter().any(|output| matches!(
        output,
        Output::Apply {
            index: LogIndex(0),
            ..
        }
    )));
}
