//! The transport receive-limit recipe, checked against real encodings.
//!
//! Every figure `WIRE_FORMAT_V1.md` and `limits.rs` state is pinned here by
//! encoding the frame and measuring it, and the closure claim — "this covers
//! every frame a leader can emit" — is enforced against the v1 tag registry
//! rather than asserted in prose.

use rafter::{
    AppendEntries, AppendEntriesResponse, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, CommittedConfiguration, ConfigurationEntry, ConfigurationId,
    InstallSnapshotChunk, InstallSnapshotResponse, JointMembership, LogEntry, LogIndex,
    MembershipConfig, MembershipSet, Message, NodeConfig, NodeId, PreVote, PreVoteResponse,
    RaftSnapshotMetadata, RequestVote, RequestVoteResponse, SnapshotCommittedConfiguration,
    SnapshotGroupId, SnapshotTransferId, Term, TimeoutNow,
};

use crate::{
    decode_message, encode_message, max_receive_frame_bytes, v1::MessageTag,
    MAX_CONFIGURATION_APPEND_FRAME_BYTES, MAX_INSTALL_SNAPSHOT_CHUNK_FRAME_BYTES,
};

/// The recipe's only quantified `AppendEntries` term: one maximum application
/// entry under the default config, plus append-frame overhead.
const DOCUMENTED_APPEND_RECIPE_BYTES: usize = 524_299;

/// `rafter`'s private `INSTALL_SNAPSHOT_CHUNK_BYTES`, the cap a conforming
/// sender applies to one chunk. Named here because the snapshot term of the
/// recipe rests on it and this crate cannot observe it.
const SENDER_SNAPSHOT_CHUNK_CAP_BYTES: usize = 64 * 1024;

/// The documented maximum length of a snapshot group id or application kind.
const MAX_SNAPSHOT_ID_BYTES: usize = 128;

fn default_config() -> NodeConfig {
    NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 5).expect("static 3-voter config")
}

/// `MembershipSet::new` imposes no size limit, so the only ceiling is this
/// format's `u16` member counts.
fn max_membership_set(voter_lo: u64, learner_lo: u64) -> MembershipSet {
    MembershipSet::new(
        (voter_lo..voter_lo + 65_535).map(NodeId).collect(),
        (learner_lo..learner_lo + 65_535).map(NodeId).collect(),
    )
    .expect("a wire-maximum membership set is structurally valid")
}

fn append_entries(entries: Vec<LogEntry>) -> Message {
    Message::AppendEntries(AppendEntries {
        term: Term(u64::MAX),
        leader_id: NodeId(u64::MAX),
        prev_log_index: LogIndex(u64::MAX),
        prev_log_term: Term(u64::MAX),
        entries: entries.into(),
        leader_commit: LogIndex(u64::MAX),
        sequence: u64::MAX,
    })
}

fn max_joint_configuration_entry() -> LogEntry {
    LogEntry::configuration(
        Term(u64::MAX),
        ConfigurationEntry::joint(
            ConfigurationId(u64::MAX),
            JointMembership::new(
                max_membership_set(1, 1_000_000),
                max_membership_set(2_000_000, 3_000_000),
            ),
        ),
    )
}

fn max_snapshot_chunk() -> Message {
    let id = "a".repeat(MAX_SNAPSHOT_ID_BYTES);
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new(id.clone()).expect("128 bytes is the documented maximum"),
        NodeId(u64::MAX),
        LogIndex(9),
        Term(3),
        Term(3),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new(id).expect("128 bytes is the documented maximum"),
            ApplicationSnapshotVersion::new(u16::MAX).expect("nonzero version"),
        ),
    )
    .expect("metadata is valid")
    .with_committed_configuration(SnapshotCommittedConfiguration::new(
        Some(CommittedConfiguration {
            index: LogIndex(9),
            config_id: ConfigurationId(u64::MAX),
        }),
        MembershipConfig::Joint(JointMembership::new(
            max_membership_set(1, 1_000_000),
            max_membership_set(2_000_000, 3_000_000),
        )),
    ));

    Message::InstallSnapshotChunk(InstallSnapshotChunk {
        term: Term(u64::MAX),
        leader_id: NodeId(u64::MAX),
        transfer_id: SnapshotTransferId(u64::MAX),
        metadata,
        total_payload_len: u64::MAX,
        application_payload_crc32: u32::MAX,
        offset: u64::MAX,
        chunk: vec![0x5a; SENDER_SNAPSHOT_CHUNK_CAP_BYTES],
        done: true,
    })
}

fn encoded_len(message: &Message) -> usize {
    let bytes = encode_message(message).expect("a maximal valid message encodes");
    assert_eq!(
        decode_message(&bytes).as_ref(),
        Ok(message),
        "a frame this recipe must accommodate must also decode"
    );
    bytes.len()
}

// ---------------------------------------------------------------------------
// The three frames the recipe has to cover.
// ---------------------------------------------------------------------------

#[test]
fn the_append_recipe_is_exact_for_application_entries() {
    let budget = default_config().max_append_entries_bytes();
    assert_eq!(budget, 512 * 1024, "default batching target");

    let frame = encoded_len(&append_entries(vec![LogEntry::application(
        Term(u64::MAX),
        vec![0xA5; budget - 64],
    )]));
    assert_eq!(
        frame, DOCUMENTED_APPEND_RECIPE_BYTES,
        "the recipe's append term is exact"
    );
    assert_eq!(
        max_receive_frame_bytes(budget).min(frame),
        frame,
        "and the published recipe accommodates it"
    );
}

#[test]
fn the_append_bound_rests_on_replication_bytes_over_charging_the_wire() {
    // The append term above is right only because `LogEntry::replication_bytes`
    // charges every entry kind more than it costs on the wire. The leader
    // admits a proposal on the charged figure, so a bound derived from the
    // charged figure is never exceeded by the encoding. Nothing outside this
    // test says so, and the two numbers live in different crates.
    let base = encoded_len(&append_entries(Vec::new()));
    for (kind, entry, charged, encoded) in [
        (
            "application",
            LogEntry::application(Term(u64::MAX), Vec::new()),
            64,
            13,
        ),
        ("noop", LogEntry::noop(Term(u64::MAX)), 16, 9),
    ] {
        let wire = encoded_len(&append_entries(vec![entry.clone()])) - base;
        assert_eq!(entry.replication_bytes(), charged, "{kind}: charged bytes");
        assert_eq!(wire, encoded, "{kind}: encoded bytes");
        assert!(
            entry.replication_bytes() > wire,
            "{kind}: the append bound depends on this being an over-charge, not an \
             under-charge; charged {} vs encoded {wire}",
            entry.replication_bytes()
        );
    }
}

#[test]
fn a_configuration_entry_escapes_the_batching_budget_and_the_append_recipe() {
    // Only application payloads are checked against `max_append_entries_bytes`
    // (node/replication/proposal.rs), and batch assembly always includes the
    // first entry whatever its size (node/log.rs). A configuration entry is
    // budget-exempt in both directions.
    let entry = max_joint_configuration_entry();
    let budget = default_config().max_append_entries_bytes();
    assert!(
        entry.replication_bytes() > budget,
        "the entry alone exceeds the whole default batch budget: {}",
        entry.replication_bytes()
    );

    let frame = encoded_len(&append_entries(vec![entry]));
    assert_eq!(
        frame, MAX_CONFIGURATION_APPEND_FRAME_BYTES,
        "the published configuration-frame maximum is exact"
    );
    assert!(
        frame > DOCUMENTED_APPEND_RECIPE_BYTES * 3,
        "one valid AppendEntries frame reaches {frame} bytes, {}% of the append term of \
         {DOCUMENTED_APPEND_RECIPE_BYTES}",
        frame * 100 / DOCUMENTED_APPEND_RECIPE_BYTES
    );
}

#[test]
fn a_snapshot_chunk_frame_is_dominated_by_metadata_not_by_chunk_data() {
    let frame = encoded_len(&max_snapshot_chunk());
    assert_eq!(
        frame, MAX_INSTALL_SNAPSHOT_CHUNK_FRAME_BYTES,
        "the published snapshot-chunk maximum is exact"
    );
    // "snapshot-chunk metadata plus up to 64 KiB of chunk data" left "metadata"
    // unquantified; a generous 512-byte allowance undershoots by ~33x.
    let naive = SENDER_SNAPSHOT_CHUNK_CAP_BYTES + 512;
    assert!(
        frame > naive * 30,
        "one valid InstallSnapshotChunk frame is {frame} bytes, {}% of a naive reading of \
         the old recipe ({naive} B)",
        frame * 100 / naive
    );
}

// ---------------------------------------------------------------------------
// Closure: the recipe covers every frame a leader can emit.
// ---------------------------------------------------------------------------

/// Every tag the v1 decoder accepts, derived from the registry's own
/// `TryFrom<u8>` rather than a hand-kept list, so a newly registered frame
/// kind appears here without anyone remembering to add it.
fn every_registered_tag() -> Vec<MessageTag> {
    (0..=u8::MAX)
        .filter_map(|byte| MessageTag::try_from(byte).ok())
        .collect()
}

/// The largest frame of each kind.
///
/// The match is exhaustive on purpose: adding a variant to `MessageTag` stops
/// this compiling until its maximum is stated, which is what makes the closure
/// claim in `max_receive_frame_bytes` enforceable rather than aspirational.
fn largest_frame_bytes(tag: MessageTag) -> usize {
    let message = match tag {
        MessageTag::RequestVote => Message::RequestVote(RequestVote {
            term: Term(u64::MAX),
            candidate_id: NodeId(u64::MAX),
            last_log_index: LogIndex(u64::MAX),
            last_log_term: Term(u64::MAX),
        }),
        MessageTag::RequestVoteResponse => Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(u64::MAX),
            voter_id: NodeId(u64::MAX),
            vote_granted: true,
        }),
        MessageTag::AppendEntries => {
            return encoded_len(&append_entries(vec![max_joint_configuration_entry()]))
        }
        MessageTag::AppendEntriesResponse => {
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(u64::MAX),
                follower_id: NodeId(u64::MAX),
                success: true,
                match_index: LogIndex(u64::MAX),
                sequence: u64::MAX,
            })
        }
        MessageTag::InstallSnapshotResponse => {
            Message::InstallSnapshotResponse(InstallSnapshotResponse {
                term: Term(u64::MAX),
                follower_id: NodeId(u64::MAX),
                success: true,
                last_included_index: LogIndex(u64::MAX),
                transfer_id: Some(SnapshotTransferId(u64::MAX)),
                next_offset: u64::MAX,
            })
        }
        MessageTag::InstallSnapshotChunk => return encoded_len(&max_snapshot_chunk()),
        MessageTag::PreVote => Message::PreVote(PreVote {
            term: Term(u64::MAX),
            candidate_id: NodeId(u64::MAX),
            last_log_index: LogIndex(u64::MAX),
            last_log_term: Term(u64::MAX),
        }),
        MessageTag::PreVoteResponse => Message::PreVoteResponse(PreVoteResponse {
            term: Term(u64::MAX),
            voter_id: NodeId(u64::MAX),
            vote_granted: true,
        }),
        MessageTag::TimeoutNow => Message::TimeoutNow(TimeoutNow {
            term: Term(u64::MAX),
            leader_id: NodeId(u64::MAX),
        }),
    };
    encoded_len(&message)
}

#[test]
fn the_published_receive_limit_covers_every_frame_a_leader_can_emit() {
    let budget = default_config().max_append_entries_bytes();
    let limit = max_receive_frame_bytes(budget);

    let tags = every_registered_tag();
    assert!(
        tags.len() >= 9,
        "the v1 registry should still hold every current frame kind, found {}",
        tags.len()
    );

    let mut largest = 0;
    for tag in tags {
        let frame = largest_frame_bytes(tag);
        assert!(
            frame <= limit,
            "a {tag:?} frame reaches {frame} bytes but the published receive limit for a \
             {budget}-byte append budget is only {limit}"
        );
        largest = largest.max(frame);
    }

    assert_eq!(
        largest, limit,
        "the published limit should be exactly the largest frame, not a loose over-estimate"
    );
}

#[test]
fn the_receive_limit_tracks_the_configured_append_budget() {
    // The append term scales with the budget; the configuration and snapshot
    // terms do not. A cluster configured far above the default is bounded by
    // its own budget, and one at or below the default is bounded by the
    // membership-driven frames.
    assert_eq!(max_receive_frame_bytes(512 * 1024), 2_163_036);
    assert_eq!(max_receive_frame_bytes(64 * 1024), 2_163_036);
    assert_eq!(
        max_receive_frame_bytes(8 * 1024 * 1024),
        8 * 1024 * 1024 + 11
    );
    assert_eq!(max_receive_frame_bytes(usize::MAX), usize::MAX);
}

#[test]
fn decode_no_longer_reserves_twelve_times_the_wire_size() {
    // The preserved repro for the reservation defect: `size_of::<LogEntry>()`
    // is still 12.4x the minimum encoded entry, but that ratio no longer
    // reaches the heap, because the reservation is bounded by the bytes in
    // hand rather than by the declared count.
    const MIN_ENCODED_LOG_ENTRY_BYTES: usize = 8 + 1;
    let entry_size = core::mem::size_of::<LogEntry>();
    assert_eq!(entry_size, 112, "LogEntry is 112 bytes on this target");
    assert!(
        entry_size > MIN_ENCODED_LOG_ENTRY_BYTES * 12,
        "the amplification still exists in the types: {entry_size} vs \
         {MIN_ENCODED_LOG_ENTRY_BYTES}"
    );

    let reserved =
        crate::v1::append_entries_entry_capacity(u32::MAX as usize, DOCUMENTED_APPEND_RECIPE_BYTES)
            * entry_size;
    assert!(
        reserved <= DOCUMENTED_APPEND_RECIPE_BYTES,
        "a {DOCUMENTED_APPEND_RECIPE_BYTES}-byte frame may no longer reserve {reserved} bytes"
    );
}
