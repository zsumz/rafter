//! Structured fuzzing of a live Raft kernel.
//!
//! Derives a bounded sequence (at most 64 steps) of inputs from the raw
//! fuzzer bytes — well-formed peer `Message`s with deliberately tiny
//! id/term/index ranges (0..=6) so messages interact, plus `Tick` and
//! `ClientProposal` — and drives them through `Node::step` on a 3-voter
//! configuration (node 1 with peers 2 and 3). One fuzzed bit selects the
//! posture: the production default (pre-vote + check-quorum on) or the
//! minimal protocol (`.with_pre_vote(false).with_check_quorum(false)`).
//!
//! Invariants asserted after every step:
//! 1. `commit_index <= last_log_index`
//! 2. `commit_index` and `current_term` never decrease
//! 3. `role == Leader` implies `leader_hint == Some(own id)` and
//!    `current_term >= 1`
//!
//! ...plus "no panic anywhere in `step`".
//!
//! Snapshot metadata and membership sets are built through the kernel's
//! validating constructors; when a constructor rejects the fuzzed values the
//! step is skipped rather than forced.
//!
//! # What `corpus/node_message_sequences/` guarantees
//!
//! `cargo fuzz run node_message_sequences` executes every file in that
//! directory against the invariants above before it begins mutating, so a
//! committed entry is checked by both CI tiers on every run. That is the whole
//! guarantee, and it is worth stating precisely: these bytes are consumed
//! *positionally* by `Unstructured` through the `input` grammar below. Change
//! the grammar — reorder a match arm, add a message variant, widen a range —
//! and the same bytes decode to a different step sequence. A corpus entry is
//! therefore an input that is always executed, not a pinned reproduction of
//! the sequence that first found a bug. `seed-commit-index-regression` is the
//! input that once drove a commit-index regression; it is kept because running
//! it is cheap and it costs nothing to keep executing, not because its meaning
//! is frozen.

#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use rafter::{
    AppendEntries, AppendEntriesResponse, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, ConfigurationEntry, ConfigurationId, Input, InstallSnapshot,
    InstallSnapshotChunk, InstallSnapshotResponse, JointMembership, LogEntry, LogIndex,
    MembershipConfig, MembershipSet, Message, Node, NodeConfig, NodeId, PreVote, PreVoteResponse,
    RaftSnapshotMetadata, RequestVote, RequestVoteResponse, Role, SnapshotGroupId,
    SnapshotTransferId, Term, TimeoutNow,
};

/// Bounded number of kernel steps per fuzz case.
const MAX_STEPS: usize = 64;
/// Small enough that 64 steps cover several election timeouts.
const ELECTION_TIMEOUT_TICKS: u64 = 3;
/// Ids, terms, indexes, offsets, and sequences all draw from 0..=6 so that
/// independently generated messages talk about the same nodes and log
/// positions.
const SMALL_MAX: u64 = 6;

const GROUP_IDS: [&str; 2] = ["g1", "g2"];
const SNAPSHOT_KINDS: [&str; 2] = ["kv", "idx"];

fn small(u: &mut Unstructured<'_>) -> Option<u64> {
    u.int_in_range(0..=SMALL_MAX).ok()
}

fn term(u: &mut Unstructured<'_>) -> Option<Term> {
    Some(Term(small(u)?))
}

fn node_id(u: &mut Unstructured<'_>) -> Option<NodeId> {
    Some(NodeId(small(u)?))
}

fn log_index(u: &mut Unstructured<'_>) -> Option<LogIndex> {
    Some(LogIndex(small(u)?))
}

fn flag(u: &mut Unstructured<'_>) -> Option<bool> {
    u.arbitrary().ok()
}

fn tiny_payload(u: &mut Unstructured<'_>) -> Option<Vec<u8>> {
    let len = u.int_in_range(0..=4usize).ok()?;
    let mut payload = Vec::with_capacity(len);
    for _ in 0..len {
        payload.push(u.arbitrary().ok()?);
    }
    Some(payload)
}

fn node_ids(u: &mut Unstructured<'_>, min: usize, max: usize) -> Option<Vec<NodeId>> {
    let count = u.int_in_range(min..=max).ok()?;
    (0..count).map(|_| node_id(u)).collect()
}

/// May legitimately fail validation (duplicate voters, voter/learner
/// overlap); the caller skips the step on `None`.
fn membership_set(u: &mut Unstructured<'_>) -> Option<MembershipSet> {
    let voters = node_ids(u, 1, 3)?;
    let learners = node_ids(u, 0, 2)?;
    MembershipSet::new(voters, learners).ok()
}

fn membership_config(u: &mut Unstructured<'_>) -> Option<MembershipConfig> {
    if flag(u)? {
        Some(MembershipConfig::stable(membership_set(u)?))
    } else {
        Some(MembershipConfig::joint(
            membership_set(u)?,
            membership_set(u)?,
        ))
    }
}

fn configuration_entry(u: &mut Unstructured<'_>) -> Option<ConfigurationEntry> {
    let config_id = ConfigurationId(small(u)?);
    if flag(u)? {
        Some(ConfigurationEntry::stable(config_id, membership_set(u)?))
    } else {
        Some(ConfigurationEntry::joint(
            config_id,
            JointMembership::new(membership_set(u)?, membership_set(u)?),
        ))
    }
}

fn log_entry(u: &mut Unstructured<'_>) -> Option<LogEntry> {
    let entry_term = term(u)?;
    if flag(u)? {
        Some(LogEntry::application(entry_term, tiny_payload(u)?))
    } else {
        Some(LogEntry::configuration(entry_term, configuration_entry(u)?))
    }
}

fn log_entries(u: &mut Unstructured<'_>) -> Option<Vec<LogEntry>> {
    let count = u.int_in_range(0..=3usize).ok()?;
    (0..count).map(|_| log_entry(u)).collect()
}

/// Built through the validating constructor: zero index/term or a
/// last-included term ahead of the hard-state term is rejected and the step
/// is skipped.
fn snapshot_metadata(u: &mut Unstructured<'_>) -> Option<RaftSnapshotMetadata> {
    let group_id = SnapshotGroupId::new(*u.choose(&GROUP_IDS).ok()?).ok()?;
    let kind = ApplicationSnapshotKind::new(*u.choose(&SNAPSHOT_KINDS).ok()?).ok()?;
    let version = ApplicationSnapshotVersion::new(u.int_in_range(0..=3u16).ok()?).ok()?;
    let mut metadata = RaftSnapshotMetadata::new(
        group_id,
        node_id(u)?,
        log_index(u)?,
        term(u)?,
        term(u)?,
        ApplicationSnapshotMetadata::new(kind, version),
    )
    .ok()?;
    if flag(u)? {
        metadata = metadata.with_committed_membership(membership_config(u)?);
    }
    Some(metadata)
}

fn message(u: &mut Unstructured<'_>) -> Option<Message> {
    Some(match u.int_in_range(0..=9u8).ok()? {
        0 => Message::RequestVote(RequestVote {
            term: term(u)?,
            candidate_id: node_id(u)?,
            last_log_index: log_index(u)?,
            last_log_term: term(u)?,
        }),
        1 => Message::RequestVoteResponse(RequestVoteResponse {
            term: term(u)?,
            voter_id: node_id(u)?,
            vote_granted: flag(u)?,
        }),
        2 => Message::PreVote(PreVote {
            term: term(u)?,
            candidate_id: node_id(u)?,
            last_log_index: log_index(u)?,
            last_log_term: term(u)?,
        }),
        3 => Message::PreVoteResponse(PreVoteResponse {
            term: term(u)?,
            voter_id: node_id(u)?,
            vote_granted: flag(u)?,
        }),
        4 => Message::TimeoutNow(TimeoutNow {
            term: term(u)?,
            leader_id: node_id(u)?,
        }),
        5 => Message::AppendEntries(AppendEntries {
            term: term(u)?,
            leader_id: node_id(u)?,
            prev_log_index: log_index(u)?,
            prev_log_term: term(u)?,
            sequence: small(u)?,
            entries: log_entries(u)?.into(),
            leader_commit: log_index(u)?,
        }),
        6 => Message::AppendEntriesResponse(AppendEntriesResponse {
            term: term(u)?,
            follower_id: node_id(u)?,
            success: flag(u)?,
            match_index: log_index(u)?,
            sequence: small(u)?,
        }),
        7 => Message::InstallSnapshot(InstallSnapshot {
            term: term(u)?,
            leader_id: node_id(u)?,
            metadata: snapshot_metadata(u)?,
            application_payload: tiny_payload(u)?,
        }),
        8 => Message::InstallSnapshotChunk(InstallSnapshotChunk {
            term: term(u)?,
            leader_id: node_id(u)?,
            transfer_id: SnapshotTransferId(small(u)?),
            metadata: snapshot_metadata(u)?,
            total_payload_len: small(u)?,
            application_payload_crc32: u.arbitrary().ok()?,
            offset: small(u)?,
            chunk: tiny_payload(u)?,
            done: flag(u)?,
        }),
        _ => Message::InstallSnapshotResponse(InstallSnapshotResponse {
            term: term(u)?,
            follower_id: node_id(u)?,
            success: flag(u)?,
            last_included_index: log_index(u)?,
            transfer_id: if flag(u)? {
                Some(SnapshotTransferId(small(u)?))
            } else {
                None
            },
            next_offset: small(u)?,
        }),
    })
}

fn input(u: &mut Unstructured<'_>) -> Option<Input> {
    Some(match u.int_in_range(0..=9u8).ok()? {
        // Weight ticks heavily enough that election timeouts fire within a
        // 64-step sequence.
        0..=2 => Input::Tick,
        3..=4 => Input::ClientProposal {
            payload: tiny_payload(u)?,
        },
        _ => Input::Message {
            from: node_id(u)?,
            message: message(u)?,
        },
    })
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);

    // One fuzzed bit chooses the posture: production defaults (pre-vote +
    // check-quorum on) or the minimal protocol.
    let minimal_posture = u.arbitrary::<bool>().unwrap_or(false);
    let config = NodeConfig::new(
        NodeId(1),
        vec![NodeId(2), NodeId(3)],
        ELECTION_TIMEOUT_TICKS,
    )
    .expect("static 3-voter config is valid");
    let config = if minimal_posture {
        config.with_pre_vote(false).with_check_quorum(false)
    } else {
        config
    };

    let mut node = Node::new(config);
    let mut commit_watermark = node.commit_index();
    let mut term_watermark = node.current_term();

    for _ in 0..MAX_STEPS {
        if u.is_empty() {
            break;
        }
        // A validating constructor rejecting fuzzed values skips the step.
        let Some(input) = input(&mut u) else {
            continue;
        };

        // Invariant 0: no panic.
        let _outputs = node.step(input);

        // Invariant 1: the commit index never runs past the log.
        assert!(
            node.commit_index() <= node.last_log_index(),
            "commit_index {} > last_log_index {}",
            node.commit_index(),
            node.last_log_index(),
        );

        // Invariant 2: commit index and term are monotone.
        assert!(
            node.commit_index() >= commit_watermark,
            "commit_index regressed: {} < {}",
            node.commit_index(),
            commit_watermark,
        );
        assert!(
            node.current_term() >= term_watermark,
            "current_term regressed: {} < {}",
            node.current_term(),
            term_watermark,
        );
        commit_watermark = node.commit_index();
        term_watermark = node.current_term();

        // Invariant 3: a leader believes in itself and has won an election
        // in some term >= 1.
        if node.role() == Role::Leader {
            assert_eq!(
                node.leader_hint(),
                Some(node.id()),
                "leader without a self leader_hint",
            );
            assert!(node.current_term() >= Term(1), "leader at term 0",);
        }
    }
});
