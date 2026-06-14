//! Property suite for the append-batch size accounting the codec pins.
//! The kernel batches append entries by
//! `LogEntry::replication_bytes`, documented as an upper bound of each
//! entry's wire encoding here; the directed
//! `configuration_entry_size_accounting_is_upper_bound_of_encoding` unit
//! test pins three memberships, and these properties pin the same claim
//! across arbitrary stable and joint memberships (and application payloads).
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
    AppendEntries, ConfigurationEntry, ConfigurationId, JointMembership, LogEntry, LogIndex,
    MembershipSet, Message, NodeId, Term,
};
use rafter_codec::encode_message;

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

fn append_entries_with(entries: Vec<LogEntry>) -> Message {
    Message::AppendEntries(AppendEntries {
        sequence: 3,
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries,
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
}
