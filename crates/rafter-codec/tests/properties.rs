//! Generated coverage for two codec contracts.
//!
//! The kernel batches append entries by `LogEntry::replication_bytes`, which
//! is documented as an upper bound of each entry's wire encoding here. The
//! directed `configuration_entry_size_accounting_is_upper_bound_of_encoding`
//! unit test pins three memberships; the first two properties below pin the
//! same claim across arbitrary stable and joint memberships and application
//! payloads. The final property generates every v1-encodable message variant
//! and pins successful encode/decode round trips.
//!
//! # Seed reproduction
//!
//! A failing property prints the shrunken counterexample together with its
//! seed and persists the seed to `proptest-regressions/properties.txt` under
//! this crate's root (the file and directory are created on first failure).
//! The next `cargo test -p rafter-codec --test properties` run replays every
//! persisted seed before generating fresh cases; committing the regression
//! file pins the case forever.

use std::collections::BTreeSet;
use std::ops::RangeInclusive;

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use rafter::{
    AppendEntries, AppendEntriesResponse, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, ConfigurationEntry, ConfigurationId, InstallSnapshotChunk,
    InstallSnapshotResponse, JointMembership, LogEntry, LogIndex, MembershipConfig, MembershipSet,
    Message, NodeId, PreVote, PreVoteResponse, RaftSnapshotMetadata, RequestVote,
    RequestVoteResponse, SnapshotGroupId, SnapshotTransferId, Term, TimeoutNow,
};
use rafter_codec::{decode_message, encode_message};

fn suite_config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/properties.txt",
        ))),
        ..ProptestConfig::default()
    }
}

/// Ids stay small so generated member sets collide and overlap often.
const MAX_NODE_ID: u64 = 15;

fn arb_id_set(sizes: RangeInclusive<usize>) -> impl Strategy<Value = BTreeSet<u64>> {
    proptest::collection::btree_set(0..=MAX_NODE_ID, sizes)
}

/// Valid stable membership sets: candidate voter/learner id sets are fed
/// through the validating constructor and anything it rejects is filtered
/// out.
fn arb_membership_set() -> impl Strategy<Value = MembershipSet> {
    (arb_id_set(1..=7), arb_id_set(0..=7)).prop_filter_map(
        "MembershipSet::new rejected the candidate member sets",
        |(voters, learners)| {
            let learners = learners.difference(&voters).copied().map(NodeId).collect();
            MembershipSet::new(voters.into_iter().map(NodeId).collect(), learners).ok()
        },
    )
}

fn arb_configuration_entry() -> impl Strategy<Value = ConfigurationEntry> {
    prop_oneof![
        (0..=99u64, arb_membership_set())
            .prop_map(|(id, set)| ConfigurationEntry::stable(ConfigurationId(id), set)),
        (0..=99u64, arb_membership_set(), arb_membership_set()).prop_map(|(id, old, new)| {
            ConfigurationEntry::joint(ConfigurationId(id), JointMembership::new(old, new))
        }),
    ]
}

fn arb_membership_config() -> impl Strategy<Value = MembershipConfig> {
    prop_oneof![
        arb_membership_set().prop_map(MembershipConfig::stable),
        (arb_membership_set(), arb_membership_set())
            .prop_map(|(old, new)| MembershipConfig::joint(old, new)),
    ]
}

fn arb_log_entry() -> impl Strategy<Value = LogEntry> {
    prop_oneof![
        (1..=100u64, proptest::collection::vec(any::<u8>(), 0..=256))
            .prop_map(|(term, payload)| LogEntry::application(Term(term), payload)),
        (1..=100u64).prop_map(|term| LogEntry::noop(Term(term))),
        (1..=100u64, arb_configuration_entry())
            .prop_map(|(term, entry)| LogEntry::configuration(Term(term), entry)),
    ]
}

fn arb_snapshot_metadata() -> impl Strategy<Value = RaftSnapshotMetadata> {
    (
        1..=10u64,
        1..=100u64,
        1..=20u64,
        1..=20u64,
        1..=10u16,
        proptest::option::of(arb_membership_config()),
    )
        .prop_filter_map(
            "snapshot term must not exceed hard-state term",
            |(writer, index, snapshot_term, hard_state_term, version, membership)| {
                if snapshot_term > hard_state_term {
                    return None;
                }
                let metadata = RaftSnapshotMetadata::new(
                    SnapshotGroupId::new("property-group").expect("valid fixed id"),
                    NodeId(writer),
                    LogIndex(index),
                    Term(snapshot_term),
                    Term(hard_state_term),
                    ApplicationSnapshotMetadata::new(
                        ApplicationSnapshotKind::new("property_state").expect("valid fixed kind"),
                        ApplicationSnapshotVersion::new(version).expect("nonzero version"),
                    ),
                )
                .ok()?;
                Some(membership.map_or(metadata.clone(), |membership| {
                    metadata.with_committed_membership(membership)
                }))
            },
        )
}

fn arb_message() -> impl Strategy<Value = Message> {
    prop_oneof![
        arb_election_message(),
        arb_append_message(),
        arb_snapshot_message(),
    ]
}

fn arb_election_message() -> impl Strategy<Value = Message> {
    prop_oneof![
        (1..=100u64, 1..=10u64, 0..=100u64, 0..=100u64).prop_map(
            |(term, candidate, index, last_term)| Message::RequestVote(RequestVote {
                term: Term(term),
                candidate_id: NodeId(candidate),
                last_log_index: LogIndex(index),
                last_log_term: Term(last_term),
            })
        ),
        (1..=100u64, 1..=10u64, any::<bool>()).prop_map(|(term, voter, granted)| {
            Message::RequestVoteResponse(RequestVoteResponse {
                term: Term(term),
                voter_id: NodeId(voter),
                vote_granted: granted,
            })
        }),
        (1..=100u64, 1..=10u64, 0..=100u64, 0..=100u64).prop_map(
            |(term, candidate, index, last_term)| Message::PreVote(PreVote {
                term: Term(term),
                candidate_id: NodeId(candidate),
                last_log_index: LogIndex(index),
                last_log_term: Term(last_term),
            })
        ),
        (1..=100u64, 1..=10u64, any::<bool>()).prop_map(|(term, voter, granted)| {
            Message::PreVoteResponse(PreVoteResponse {
                term: Term(term),
                voter_id: NodeId(voter),
                vote_granted: granted,
            })
        }),
        (1..=100u64, 1..=10u64).prop_map(|(term, leader)| {
            Message::TimeoutNow(TimeoutNow {
                term: Term(term),
                leader_id: NodeId(leader),
            })
        }),
    ]
}

fn arb_append_message() -> impl Strategy<Value = Message> {
    prop_oneof![
        (
            1..=100u64,
            1..=10u64,
            0..=100u64,
            0..=100u64,
            proptest::collection::vec(arb_log_entry(), 0..=4),
            0..=100u64,
            any::<u64>(),
        )
            .prop_map(
                |(term, leader, previous, previous_term, entries, commit, sequence)| {
                    Message::AppendEntries(AppendEntries {
                        term: Term(term),
                        leader_id: NodeId(leader),
                        prev_log_index: LogIndex(previous),
                        prev_log_term: Term(previous_term),
                        entries: entries.into(),
                        leader_commit: LogIndex(commit),
                        sequence,
                    })
                }
            ),
        (
            1..=100u64,
            1..=10u64,
            any::<bool>(),
            0..=100u64,
            any::<u64>()
        )
            .prop_map(|(term, follower, success, index, sequence)| {
                Message::AppendEntriesResponse(AppendEntriesResponse {
                    term: Term(term),
                    follower_id: NodeId(follower),
                    success,
                    match_index: LogIndex(index),
                    sequence,
                })
            }),
    ]
}

fn arb_snapshot_message() -> impl Strategy<Value = Message> {
    prop_oneof![
        (
            1..=100u64,
            1..=10u64,
            any::<bool>(),
            0..=100u64,
            proptest::option::of(any::<u64>()),
            any::<u64>(),
        )
            .prop_map(|(term, follower, success, index, transfer, offset)| {
                Message::InstallSnapshotResponse(InstallSnapshotResponse {
                    term: Term(term),
                    follower_id: NodeId(follower),
                    success,
                    last_included_index: LogIndex(index),
                    transfer_id: transfer.map(SnapshotTransferId),
                    next_offset: offset,
                })
            }),
        (
            1..=100u64,
            1..=10u64,
            any::<u64>(),
            arb_snapshot_metadata(),
            any::<u64>(),
            any::<u32>(),
            any::<u64>(),
            proptest::collection::vec(any::<u8>(), 0..=1024),
            any::<bool>(),
        )
            .prop_map(
                |(term, leader, transfer, metadata, total, crc, offset, chunk, done)| {
                    Message::InstallSnapshotChunk(InstallSnapshotChunk {
                        term: Term(term),
                        leader_id: NodeId(leader),
                        transfer_id: SnapshotTransferId(transfer),
                        metadata,
                        total_payload_len: total,
                        application_payload_crc32: crc,
                        offset,
                        chunk,
                        done,
                    })
                }
            ),
    ]
}

fn append_entries_with(entries: Vec<LogEntry>) -> Message {
    Message::AppendEntries(AppendEntries {
        sequence: 3,
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries: entries.into(),
        leader_commit: LogIndex(11),
    })
}

/// The bytes `entry` adds to an encoded append frame: the size of a
/// single-entry frame minus the size of the same frame with no entries.
fn marginal_encoded_bytes(entry: &LogEntry) -> usize {
    let base = encode_message(&append_entries_with(Vec::new()))
        .expect("an empty append frame encodes")
        .len();
    let with_entry = encode_message(&append_entries_with(vec![entry.clone()]))
        .expect("a single-entry append frame encodes")
        .len();
    with_entry - base
}

proptest! {
    #![proptest_config(suite_config(256))]

    #[test]
    fn replication_bytes_upper_bounds_arbitrary_configuration_entries(
        entry in arb_configuration_entry(),
        term in 1..=100u64,
    ) {
        let log_entry = LogEntry::configuration(Term(term), entry);
        let marginal = marginal_encoded_bytes(&log_entry);
        prop_assert!(
            log_entry.replication_bytes() >= marginal,
            "budget accounting {} must upper-bound the wire encoding {} for {:?}",
            log_entry.replication_bytes(),
            marginal,
            log_entry
        );
    }

    #[test]
    fn replication_bytes_upper_bounds_application_entries(
        payload_len in 0..=2048usize,
        term in 1..=100u64,
    ) {
        let log_entry = LogEntry::application(Term(term), vec![0xC4; payload_len]);
        let marginal = marginal_encoded_bytes(&log_entry);
        prop_assert!(
            log_entry.replication_bytes() >= marginal,
            "budget accounting {} must upper-bound the wire encoding {} for a {}-byte payload",
            log_entry.replication_bytes(),
            marginal,
            payload_len
        );
    }

    #[test]
    fn every_generated_valid_message_round_trips(message in arb_message()) {
        let encoded = encode_message(&message).expect("generated valid message encodes");
        let decoded = decode_message(&encoded).expect("generated valid frame decodes");
        prop_assert_eq!(decoded, message);
    }
}
