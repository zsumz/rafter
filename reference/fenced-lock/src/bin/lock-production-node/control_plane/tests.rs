//! What a damaged control-plane file must not be allowed to do.
//!
//! The two properties, restated as the assertions this file makes: a corruption
//! must not **lower the mark**, and must not **change the set the mark is read
//! against** — in either direction, because widening it un-spends an identity
//! the cluster consumed and narrowing it retires a replica the cluster still
//! has. Every case here is *syntactically valid* — the parser would have
//! accepted all of it before the seal — which is the whole reason a version tag
//! and a structural parse were not enough.

use super::*;

fn sample() -> PeerControlPlaneCheckpoint<LockGroupId> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP_ID);
    checkpoint.committed_id_high_water = Some(NodeId(5));
    checkpoint.current_committed = Some(CurrentCommittedState::new(
        LogIndex(11),
        [NodeId(1), NodeId(2)].into_iter().collect(),
    ));
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
        // The live set, narrowed — the corruption that retires node 2, because
        // the floor this record produces covers every identity at or below the
        // mark that the set does not name.
        ("live 1 2", "live 1"),
        // The group binding, moved to another group's identities.
        ("group 1", "group 9"),
        // The position, rewound — the corruption that makes a current
        // membership look older than it is, so a genuine later observation
        // outranks it and everything this record names and that one does not
        // reads as a committed removal.
        ("through 11", "through 4"),
        // The version tag.
        ("control-plane 7", "control-plane 8"),
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
            resealed("rafter-lock-control-plane 7\ngroup 1\nhigh_water -\nthrough 4\nlive 1 2\ncontradicted -\n"),
            "names no high-water mark",
        ),
        // A live member above the mark: unjudgeable by the spent test, and the
        // shape a lowered mark produces.
        (
            resealed("rafter-lock-control-plane 7\ngroup 1\nhigh_water 2\nthrough 4\nlive 1 2 5\ncontradicted -\n"),
            "above the high-water mark",
        ),
        // A retirement record with no current state, which is both what a
        // truncation produces and what a migration of an older file would
        // have had to invent. A mark read against no membership spends every
        // identity at or below it, so this record starts a replica that refuses
        // its whole cluster.
        (
            resealed("rafter-lock-control-plane 7\ngroup 1\nhigh_water 5\nthrough -\nlive\ncontradicted -\n"),
            "names no committed membership to read it against",
        ),
        // The opposite separation: a current state with nothing retired behind
        // it, so the observation that produced it is lost along with what it
        // spent. `LogIndex(0)` is a real position rather than an absence, so
        // this is a separation and not a zero standing in for `-`.
        (
            resealed("rafter-lock-control-plane 7\ngroup 1\nhigh_water -\nthrough 0\nlive\ncontradicted -\n"),
            "names no high-water mark",
        ),
        (
            resealed("rafter-lock-control-plane 7\ngroup 1\nhigh_water -\nthrough 9\nlive\ncontradicted -\n"),
            "observed the committed configuration at index 9",
        ),
        // A membership with no position to date it. Refused in the decoder
        // rather than by an invariant, because the two lines are one value and
        // there is no half of it to hand on.
        (
            resealed("rafter-lock-control-plane 7\ngroup 1\nhigh_water 5\nthrough -\nlive 1 2\ncontradicted -\n"),
            "no position to date them",
        ),
        // A contradiction marker with nothing to have contradicted. A
        // contradiction is a disagreement *between* the register and something
        // else, so a record that observed nothing cannot have recorded one.
        (
            resealed("rafter-lock-control-plane 7\ngroup 1\nhigh_water -\nthrough -\nlive\ncontradicted 9\n"),
            "names no committed membership for that to have contradicted",
        ),
        // A marker beneath the observation it is about. The register stops
        // moving the moment the marker is set, so this pair describes no state
        // any driver reached — and reading it would freeze a replica at a
        // position nothing disagreed at.
        (
            resealed("rafter-lock-control-plane 7\ngroup 1\nhigh_water 5\nthrough 12\nlive 1 2\ncontradicted 4\n"),
            "a record stops moving where it is contradicted",
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

/// An older file is refused rather than migrated, whichever older version it is.
///
/// Each record is otherwise well formed and would have verified under the build
/// that wrote it. Versions 2 to 4 are refused because no honest value can be
/// invented for what they do not record — version 4's `live` was assigned by a
/// fold of either kind while only that kind's own offset advanced, so neither
/// `crossings` nor `endpoint` dates it, and both readings retire live replicas
/// in opposite directions.
///
/// **Version 5 is refused for a different reason, and it is the one worth
/// stating**, because its dropped field is the one case where the mapping *is*
/// knowable. A version-5 `fences` line named identities its own invariant check
/// required to be at or below the mark and absent from the live set, which is
/// exactly what a retirement floor covers — so ignoring it would lose nothing.
///
/// What would be lost is the reader. Accepting a line this format did not expect
/// is exactly what refusing tells a partial overwrite of a longer record from a
/// finished write. See `trailing_bytes_after_the_checksum_are_refused`, which is
/// the same rule arriving from the other side.
///
/// **Version 6 is refused for a third reason, and it is the strongest of the
/// three.** Its missing line is the contradiction marker, and a version-6 file
/// cannot support the one migration that looks obvious — "no marker, so read it
/// as uncontradicted". Before the marker existed a contradicted driver went on
/// folding later facts into its record and went on advancing its epoch, so a
/// version-6 file may have been written *past* a contradiction by a process that
/// had already stopped serving. Nothing distinguishes such a file from an honest
/// one, so reading either as clean is the forgetting version 7 exists to end.
#[test]
fn an_older_file_is_refused_rather_than_migrated() {
    let older = [
        resealed("rafter-lock-control-plane 2\ngroup 1\nhigh_water 7\nlive 1 3\nfences 7\n"),
        resealed(
            "rafter-lock-control-plane 3\ngroup 1\nhigh_water 7\nlive 1 3\nfences 7\nthrough 12\n",
        ),
        resealed(
            "rafter-lock-control-plane 4\ngroup 1\nhigh_water 7\nlive 1 3\nfences 7\ncrossings 9\nendpoint 12\n",
        ),
        resealed(
            "rafter-lock-control-plane 5\ngroup 1\nhigh_water 7\nthrough 12\nlive 1 3\nfences 7\n",
        ),
        resealed("rafter-lock-control-plane 6\ngroup 1\nhigh_water 7\nthrough 12\nlive 1 3\n"),
    ];

    for text in older {
        let refused = decode(&text).expect_err("an older format is not this format");
        assert!(
            refused.contains("rafter-lock-control-plane 7"),
            "the refusal should name the version this build reads: {refused}"
        );
    }
}

/// The control: a record that verifies and is consistent is accepted whole.
///
/// Without it, a `decode` that refused everything would pass every clause above.
#[test]
fn a_sealed_consistent_record_is_accepted() {
    let text =
        resealed("rafter-lock-control-plane 7\ngroup 1\nhigh_water 7\nthrough 12\nlive 1 3\ncontradicted -\n");
    let decoded = decode(&text).expect("a well-formed record");
    assert_eq!(decoded.committed_id_high_water, Some(NodeId(7)));
    let current = decoded
        .current_committed
        .expect("a record that retired something names where it looked");
    assert_eq!(
        current.through,
        LogIndex(12),
        "the position survives the round trip that matters"
    );
    assert_eq!(
        current.membership,
        [NodeId(1), NodeId(3)].into_iter().collect(),
        "and it arrives attached to the membership it dates"
    );
}

/// A snapshot-recovered replica's record is an ordinary record.
///
/// It observed the boundary configuration at its commit index and no
/// configuration entry at all. Under the deleted offsets that was a shape the
/// invariant had to be written carefully to permit; with one positioned current
/// state there is nothing special about it, which is the simplification the
/// deletion bought.
#[test]
fn a_snapshot_recovered_record_is_accepted() {
    let text =
        resealed("rafter-lock-control-plane 7\ngroup 1\nhigh_water 3\nthrough 10\nlive 1 2 3\ncontradicted -\n");
    let decoded = decode(&text).expect("a snapshot-recovered replica writes exactly this");
    let current = decoded.current_committed.expect("a current state");
    assert_eq!(current.through, LogIndex(10));
    assert_eq!(
        current.membership,
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect()
    );
}

/// An absent file is a first boot only for a replica that has committed
/// nothing.
///
/// The two answers differ on evidence rather than on the file, which is the
/// whole point: the file is equally absent in both. A replica whose commit index
/// is zero has committed no configuration, so it has retired no identity — an
/// empty checkpoint is what its driver would derive anyway.
/// A replica that has committed something and has no file has *lost* one, and
/// there is no second copy and nothing else on disk that records what it held.
#[test]
fn an_absent_file_is_a_first_boot_only_below_the_commit_floor() {
    let directory = std::env::temp_dir().join(format!(
        "rafter-control-plane-absent-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&directory).expect("a scratch directory");

    let fresh = load(&directory, LogIndex::ZERO).expect("a replica that has committed nothing");
    assert_eq!(
        fresh,
        PeerControlPlaneCheckpoint::empty(GROUP_ID),
        "a genuine first boot reads as the empty record its driver derives"
    );

    let refused = load(&directory, LogIndex(4)).expect_err(
        "a replica that has committed through index 4 has retired whatever its \
         configurations retired",
    );
    assert!(
        matches!(refused, CheckpointError::Missing { .. }),
        "the refusal names a deletion rather than a corruption: {refused:?}"
    );
    assert!(
        refused.to_string().contains("is missing"),
        "and says so: {refused}"
    );

    drop(std::fs::remove_dir_all(&directory));
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

/// A contradicted record round-trips, marker and all.
///
/// **The one fact in this file that no restart re-derives and no fact clears.**
/// A replica whose driver found two irreconcilable claims about one committed
/// configuration stops serving and stops publishing; the record it writes is what
/// makes the next incarnation start in the same posture rather than clean. A
/// round trip that lost the line would leave the terminal state lasting exactly
/// as long as the process that discovered it.
#[test]
fn a_contradicted_record_round_trips() {
    let mut checkpoint = sample();
    checkpoint.contradicted_at = Some(LogIndex(11));
    let decoded = decode(&encode(&checkpoint)).expect("this build reads what it writes");
    assert_eq!(decoded, checkpoint);
    assert_eq!(
        decoded.contradicted_at,
        Some(LogIndex(11)),
        "the marker is the whole point of the round trip"
    );
}

/// A marker standing above the observation it contradicts is accepted.
///
/// The clause is an inequality rather than an equality, and this is why: the
/// candidate that found the contradiction may already have folded earlier facts
/// of the same batch, so it stood ahead of the register the driver kept. Refusing
/// that would refuse records real drivers write.
#[test]
fn a_marker_above_the_observation_is_accepted() {
    let text = resealed(
        "rafter-lock-control-plane 7\ngroup 1\nhigh_water 5\nthrough 12\nlive 1 2\ncontradicted 14\n",
    );
    let decoded = decode(&text).expect("a driver that folded a later fact before refusing");
    assert_eq!(decoded.contradicted_at, Some(LogIndex(14)));
    assert_eq!(
        decoded
            .current_committed
            .expect("a marked record has an observation")
            .through,
        LogIndex(12)
    );
}
