//! What a damaged control-plane file must not be allowed to do.
//!
//! The three properties, restated as the assertions this file makes: a
//! corruption must not **lower the mark**, must not **add a live identity**, and
//! must not **fence an active member**. Every case here is *syntactically valid*
//! — the parser would have accepted all of it before the seal — which is the
//! whole reason a version tag and a structural parse were not enough.

use super::*;

fn sample() -> PeerControlPlaneCheckpoint<LockGroupId> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP_ID);
    checkpoint.committed_id_high_water = Some(NodeId(5));
    checkpoint.live_committed_members = [NodeId(1), NodeId(2)].into_iter().collect();
    checkpoint.pending_fences = [NodeId(5)].into_iter().collect();
    checkpoint
}

/// Re-seals `body` so a test can corrupt a *field* rather than the checksum.
///
/// Without this every case would be caught by the checksum and none of them
/// would reach the invariant checks, which is the opposite of what these tests
/// are for.
fn resealed(body: &str) -> String {
    format!("{body}crc32 {:08x}\n", crc32(body.as_bytes()))
}

#[test]
fn a_round_trip_preserves_every_fact() {
    let checkpoint = sample();
    let decoded = decode(&encode(&checkpoint)).expect("this build reads what it writes");
    assert_eq!(decoded, checkpoint);
}

#[test]
fn a_flipped_bit_in_any_field_is_refused_by_the_checksum() {
    let original = encode(&sample());
    let corruptions = [
        // The mark, lowered — the corruption that un-retires everything above it.
        ("high_water 5", "high_water 2"),
        // The live set, widened — the corruption that un-spends an identity.
        ("live 1 2", "live 1 2 5"),
        // The fence set, emptied — the corruption that forgets an obligation.
        ("fences 5", "fences  "),
        // The group binding, moved to another group's identities.
        ("group 1", "group 9"),
        // The version tag.
        ("control-plane 2", "control-plane 3"),
    ];

    for (from, to) in corruptions {
        assert!(
            original.contains(from),
            "the fixture must contain `{from}` to corrupt it"
        );
        let damaged = original.replacen(from, to, 1);
        let refused = decode(&damaged);
        assert!(
            refused.is_err(),
            "`{from}` -> `{to}` was accepted: {refused:?}"
        );
    }
}

#[test]
fn trailing_bytes_after_the_checksum_are_refused() {
    let damaged = format!("{}live 9\n", encode(&sample()));
    let refused = decode(&damaged).expect_err("a file that grew is not a file that verified");
    assert!(
        refused.contains("trailing"),
        "the refusal should name the trailing bytes: {refused}"
    );
}

#[test]
fn a_truncated_file_is_refused() {
    let original = encode(&sample());
    for length in 1..original.len() {
        assert!(
            decode(&original[..length]).is_err(),
            "a prefix of length {length} was accepted as a whole file"
        );
    }
}

#[test]
fn a_resealed_file_for_another_group_is_refused() {
    let mut checkpoint = sample();
    checkpoint.group = LockGroupId(GROUP_ID.0 + 1);
    let refused = decode(&encode(&checkpoint)).expect_err("another group's record is not this one");
    assert!(
        refused.contains("group"),
        "the refusal should name the binding: {refused}"
    );
}

/// A resealed record whose facts contradict each other is still refused.
///
/// These are the cases a checksum cannot catch, because the bytes are internally
/// consistent — a hand-edited file, or a corruption that happened to be resealed
/// by a writer from a different build. Each one lowers a retirement record.
#[test]
fn a_resealed_contradiction_is_refused() {
    let cases = [
        // A live set with no mark: the spent test reads both together, and a
        // mark-less record spends nothing at all.
        (
            resealed("rafter-lock-control-plane 2\ngroup 1\nhigh_water -\nlive 1 2\nfences\n"),
            "no high-water mark",
        ),
        // A live member above the mark: unjudgeable by the spent test, and the
        // shape a lowered mark produces.
        (
            resealed("rafter-lock-control-plane 2\ngroup 1\nhigh_water 2\nlive 1 2 5\nfences\n"),
            "above the high-water mark",
        ),
        // A fence naming a live member: this would ask the link layer to
        // permanently fence a replica the group still needs.
        (
            resealed("rafter-lock-control-plane 2\ngroup 1\nhigh_water 5\nlive 1 2\nfences 2\n"),
            "fenced and also a live member",
        ),
    ];

    for (damaged, expected) in cases {
        let refused = decode(&damaged).expect_err("a contradictory record is not a checkpoint");
        assert!(
            refused.contains(expected),
            "expected a refusal naming `{expected}`, got `{refused}`"
        );
    }
}

/// The control: a record that verifies and is consistent is accepted whole.
///
/// Without it, a `decode` that refused everything would pass every clause above.
#[test]
fn a_sealed_consistent_record_is_accepted() {
    let text = resealed("rafter-lock-control-plane 2\ngroup 1\nhigh_water 7\nlive 1 3\nfences 7\n");
    let decoded = decode(&text).expect("a well-formed record");
    assert_eq!(decoded.committed_id_high_water, Some(NodeId(7)));
    assert_eq!(
        decoded.live_committed_members,
        [NodeId(1), NodeId(3)].into_iter().collect()
    );
    assert_eq!(decoded.pending_fences, [NodeId(7)].into_iter().collect());
}

/// An empty checkpoint round-trips, which is what a first boot writes.
#[test]
fn an_empty_checkpoint_round_trips() {
    let checkpoint = PeerControlPlaneCheckpoint::empty(GROUP_ID);
    assert_eq!(
        decode(&encode(&checkpoint)).expect("an empty record is a record"),
        checkpoint
    );
}
