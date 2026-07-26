//! Encoding capacity, hostile-count, and replication-budget contracts.

use rafter::{
    ConfigurationEntry, ConfigurationId, JointMembership, LogEntry, MembershipSet, Message, NodeId,
    Term,
};

use super::support::{append_entries_with, stable_configuration_entry};
use crate::{
    decode_message, encode_message,
    v1::{
        append_entries_entry_capacity, membership_node_capacity, MessageTag,
        MIN_ENCODED_LOG_ENTRY_BYTES, NODE_ID_BYTES,
    },
    DecodePeerMessageError, EncodePeerMessageError, MAGIC, VERSION,
};

#[test]
fn configuration_entry_size_accounting_is_upper_bound_of_encoding() {
    let stable_large = ConfigurationEntry::stable(
        ConfigurationId(2),
        MembershipSet::new(
            (1..=21).map(NodeId).collect(),
            (22..=30).map(NodeId).collect(),
        )
        .expect("valid"),
    );
    let joint = ConfigurationEntry::joint(
        ConfigurationId(3),
        JointMembership::new(
            MembershipSet::new((1..=5).map(NodeId).collect(), vec![]).expect("valid"),
            MembershipSet::new((1..=9).map(NodeId).collect(), vec![NodeId(10)]).expect("valid"),
        ),
    );

    for log_entry in [
        stable_configuration_entry(),
        LogEntry::configuration(Term(1), stable_large),
        LogEntry::configuration(Term(1), joint),
    ] {
        let base = encode_message(&append_entries_with(vec![]))
            .expect("base encodes")
            .len();
        let with_entry = encode_message(&append_entries_with(vec![log_entry.clone()]))
            .expect("message encodes")
            .len();
        assert!(log_entry.replication_bytes() >= with_entry - base);
    }
}

#[test]
fn append_entries_reservation_never_exceeds_the_bytes_that_justify_it() {
    // The bound that matters is heap *bytes*, not element count. `Vec`
    // reserves `capacity * size_of::<LogEntry>()`, and `size_of::<LogEntry>()`
    // is 12.4x `MIN_ENCODED_LOG_ENTRY_BYTES`; asserting only the element bound
    // hides that amplification entirely, which is how a hostile frame at a
    // transport's receive limit came to reserve 12.4x its own size before a
    // single entry was validated.
    let entry_size = core::mem::size_of::<LogEntry>();
    assert!(
        entry_size > MIN_ENCODED_LOG_ENTRY_BYTES,
        "an in-memory entry is larger than its minimum encoding; if this ever \
         stops holding, the element bound alone would suffice"
    );

    for remaining in [0, 1, 8, 9, 111, 112, 256, 4_096, 65_536, 524_299, 2_097_207] {
        for entry_count in [0, 1, 3, 1_000, u32::MAX as usize] {
            let reserved_bytes = append_entries_entry_capacity(entry_count, remaining) * entry_size;
            assert!(
                reserved_bytes <= remaining,
                "decoding a frame with {remaining} bytes left and a declared count of \
                 {entry_count} reserves {reserved_bytes} bytes of heap before validating \
                 any entry"
            );
        }
    }
}

#[test]
fn append_entries_reservation_still_preallocates_a_plausible_batch() {
    // The byte bound above is satisfied by returning zero always, so pin the
    // other direction: a frame whose declared count is credible for the bytes
    // it carries is still reserved exactly, with no growth.
    assert_eq!(append_entries_entry_capacity(3, 4_096), 3);
    assert_eq!(append_entries_entry_capacity(0, 4_096), 0);
    // A hostile count is clamped to what the remaining bytes can hold in heap.
    assert_eq!(
        append_entries_entry_capacity(u32::MAX as usize, 4_096),
        4_096 / core::mem::size_of::<LogEntry>()
    );
}

#[test]
fn membership_reservation_is_bounded_by_available_node_bytes() {
    assert_eq!(
        membership_node_capacity(u16::MAX as usize, 24),
        24 / NODE_ID_BYTES
    );
    assert_eq!(membership_node_capacity(2, 24), 2);
}

#[test]
fn decode_rejects_huge_append_entries_count_with_small_payload_budget() {
    let mut frame = Vec::new();
    frame.extend_from_slice(&MAGIC);
    frame.push(VERSION);
    frame.push(MessageTag::AppendEntries.into());
    frame.extend_from_slice(&7u64.to_be_bytes());
    frame.extend_from_slice(&1u64.to_be_bytes());
    frame.extend_from_slice(&0u64.to_be_bytes());
    frame.extend_from_slice(&0u64.to_be_bytes());
    frame.extend_from_slice(&u32::MAX.to_be_bytes());
    frame.resize(frame.len() + 256, 0);

    let error = decode_message(&frame).expect_err("tiny payload cannot satisfy huge count");
    assert!(matches!(
        error,
        DecodePeerMessageError::UnexpectedEof { .. }
    ));
}

#[test]
fn encode_reports_the_membership_count_wire_maximum() {
    let membership = MembershipSet::new((1..=65_536).map(NodeId).collect(), Vec::new())
        .expect("large membership is structurally valid");
    let message = append_entries_with(vec![LogEntry::configuration(
        Term(1),
        ConfigurationEntry::stable(ConfigurationId(1), membership),
    )]);

    assert_eq!(
        encode_message(&message),
        Err(EncodePeerMessageError::FieldTooLarge {
            field: "membership_voter_count",
            len: 65_536,
            max: u16::MAX as usize,
        })
    );
}

#[test]
fn application_entry_size_accounting_is_an_upper_bound() {
    let entry = LogEntry::application(Term(1), vec![0xA5; 2048]);
    let base = encode_message(&append_entries_with(Vec::new()))
        .expect("base encodes")
        .len();
    let encoded = encode_message(&append_entries_with(vec![entry.clone()]))
        .expect("message encodes")
        .len();
    assert!(entry.replication_bytes() >= encoded - base);
}

#[test]
fn empty_append_batch_remains_a_message() {
    assert!(matches!(
        append_entries_with(Vec::new()),
        Message::AppendEntries(_)
    ));
}
