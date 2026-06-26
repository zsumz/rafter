use super::*;
use rafter_storage::FileRaftLogSegment;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static TEST_FILE_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn follower_conflict_repair_replaces_durable_suffix_from_first_index() {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[PersistedRaftLogEntry::application(
            LogIndex(1),
            Term(1),
            b"old".to_vec(),
        )])
        .expect("initial log persists");
    let mut runtime = durable_node_with_log(2, &[1, 3], hard_state_store(1, None), log_segment);

    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(2),
                leader_id: RaftNodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: vec![LogEntry::application(Term(2), b"replacement".to_vec())],
                leader_commit: LogIndex::ZERO,
            }),
        })
        .expect("durable log repairs the suffix");

    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::Send {
            message: Message::AppendEntriesResponse(response),
            ..
        }] if response.success && response.match_index == LogIndex(1)
    ));
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![PersistedRaftLogEntry::application(
            LogIndex(1),
            Term(2),
            b"replacement".to_vec(),
        )]
    );
}

#[test]
fn follower_conflict_repair_replaces_durable_uncommitted_suffix() {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"prefix".to_vec()),
            PersistedRaftLogEntry::application(
                LogIndex(2),
                Term(1),
                b"old-uncommitted-suffix".to_vec(),
            ),
        ])
        .expect("old leader entries persist");
    let mut runtime = durable_node_with_log(2, &[1, 3], hard_state_store(1, None), log_segment);

    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(2),
                leader_id: RaftNodeId(1),
                prev_log_index: LogIndex(1),
                prev_log_term: Term(1),
                entries: vec![LogEntry::application(
                    Term(2),
                    b"replacement-suffix".to_vec(),
                )],
                leader_commit: LogIndex(1),
            }),
        })
        .expect("durable log repairs the uncommitted suffix");

    assert!(matches!(
        outputs.as_slice(),
        [
            RaftOutput::Apply { index: LogIndex(1), .. },
            RaftOutput::Send {
                message: Message::AppendEntriesResponse(response),
                ..
            },
        ] if response.success && response.match_index == LogIndex(2)
    ));
    assert_eq!(
        runtime.log_entries_from(LogIndex(2)),
        vec![LogEntry::application(
            Term(2),
            b"replacement-suffix".to_vec(),
        )]
    );
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"prefix".to_vec()),
            PersistedRaftLogEntry::application(
                LogIndex(2),
                Term(2),
                b"replacement-suffix".to_vec(),
            ),
        ]
    );
}

#[test]
fn file_backed_follower_conflict_repair_survives_restart() {
    let path = test_raft_log_path("conflict-repair");
    let mut log_segment = FileRaftLogSegment::open(&path).expect("segment opens");
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"prefix".to_vec()),
            PersistedRaftLogEntry::application(
                LogIndex(2),
                Term(1),
                b"old-uncommitted-suffix".to_vec(),
            ),
        ])
        .expect("old leader entries persist");
    let hard_state_store = {
        let mut runtime = durable_node_with_log(2, &[1, 3], hard_state_store(1, None), log_segment);

        runtime
            .step(RaftInput::Message {
                from: RaftNodeId(1),
                message: Message::AppendEntries(AppendEntries {
                    sequence: 0,
                    term: Term(2),
                    leader_id: RaftNodeId(1),
                    prev_log_index: LogIndex(1),
                    prev_log_term: Term(1),
                    entries: vec![LogEntry::application(
                        Term(2),
                        b"replacement-suffix".to_vec(),
                    )],
                    leader_commit: LogIndex(1),
                }),
            })
            .expect("durable log repairs the uncommitted suffix");
        runtime.hard_state_store.clone()
    };

    let reopened = FileRaftLogSegment::open(&path).expect("segment reopens");
    let restarted = durable_node_with_log(2, &[1, 3], hard_state_store, reopened);

    assert_eq!(restarted.last_log_index(), LogIndex(2));
    assert_eq!(
        restarted.log_entries_from(LogIndex(1)),
        vec![
            LogEntry::application(Term(1), b"prefix".to_vec()),
            LogEntry::application(Term(2), b"replacement-suffix".to_vec()),
        ]
    );
    remove_test_file(path);
}

fn test_raft_log_path(name: &str) -> PathBuf {
    let id = TEST_FILE_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "rafter-runtime-{name}-{}-{id}.raftlog",
        std::process::id()
    ))
}

fn remove_test_file(path: PathBuf) {
    let _ = fs::remove_file(path);
}

/// The normal rejoin shape: a follower's persisted uncommitted suffix is
/// spliced out by a new leader whose frame also carries a commit index
/// past the conflict. The divergence is measured against the commit floor
/// of the LAST persist, so the repair truncates and the ack escapes — no
/// poison, no restart loop.
#[test]
fn rejoin_splice_with_commit_past_the_conflict_repairs_instead_of_poisoning() {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"shared".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"stale-suffix".to_vec()),
        ])
        .expect("initial log persists");
    let mut runtime = durable_node_with_log(2, &[1, 3], hard_state_store(1, None), log_segment);

    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(3),
                leader_id: RaftNodeId(1),
                prev_log_index: LogIndex(1),
                prev_log_term: Term(1),
                entries: vec![LogEntry::application(Term(2), b"committed-truth".to_vec())],
                leader_commit: LogIndex(2),
            }),
        })
        .expect("the catch-up splice persists instead of poisoning");

    assert!(outputs.iter().any(|output| matches!(
        output,
        RaftOutput::Send {
            message: Message::AppendEntriesResponse(response),
            ..
        } if response.success && response.match_index == LogIndex(2)
    )));
    assert_eq!(runtime.commit_index(), LogIndex(2));
    assert_eq!(
        runtime.log_segment.replay_entries()[1]
            .kind
            .application_payload(),
        Some(b"committed-truth".as_slice())
    );
}
