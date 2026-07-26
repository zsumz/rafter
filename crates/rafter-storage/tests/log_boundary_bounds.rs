//! Log positions read from disk must be advanceable, in both build profiles.
//!
//! Adopted from the gen-6 reproduction of a durable-corruption defect: the RFLC
//! compaction marker and the RFLE entry index were decoded as unconstrained
//! `u64`s and handed to `LogIndex::next()`, which is `Self(self.0 + 1)`. A
//! corrupt-but-checksum-consistent artifact naming `u64::MAX` therefore panicked
//! in debug builds and, worse, wrapped to `LogIndex(0)` in release builds — a
//! store that opened `Healthy`, reported `next_index() == LogIndex(0)`, and then
//! accepted and `sync_data`-ed an entry at the zero sentinel.
//!
//! The gen-7 reproduction is adopted here too. The gen-6 fix bounded the
//! *encoder*, which bounded `FileRaftLogSegment` because it encodes and left
//! `InMemoryRaftLogSegment` — the default log for `DurableRaftNode`, which
//! encodes nothing — able to append at `u64::MAX` and wrap `next_index()` to
//! `LogIndex(0)` in release builds. The bound is now stated on the
//! `RaftLogSegment` trait and applied by both implementations.
//!
//! Every test here must hold under `cargo test` *and* `cargo test --release`.
//! The release profile is the dangerous one: a debug-only test would have seen
//! only the panic and missed the silent wrap entirely.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rafter::{LogIndex, Term};
use rafter_storage::{
    crc32, encode_raft_log_entry, BorrowedPersistedRaftLogEntry, EncodeRaftLogEntryError,
    FileRaftLogSegment, FileRaftNodeStores, InMemoryRaftLogSegment, OpenRaftLogSegmentError,
    PersistedRaftLogEntry, RaftLogSegment, RaftLogSegmentAppendError, RaftLogSegmentCompactError,
};

struct Scratch {
    path: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rafter-log-bounds-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create scratch dir");
        Self { path }
    }

    fn join(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn write_file(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .expect("create file");
    file.write_all(bytes).expect("write");
    file.sync_all().expect("sync");
}

/// A well-formed RFLC marker with an arbitrary `compacted_through` value.
fn compaction_marker(compacted_through: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RFLC");
    bytes.push(1);
    bytes.extend_from_slice(&compacted_through.to_be_bytes());
    let checksum = crc32(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

/// A well-formed RFLE noop envelope with an arbitrary `index`.
///
/// Hand-rolled on purpose: the shipped encoder now refuses the very indexes
/// these tests must forge, so the on-disk bytes cannot come from it.
/// `forged_noop_entry_matches_the_shipped_encoder` keeps this honest.
fn forged_noop_entry(index: u64, term: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RFLE");
    bytes.push(1);
    bytes.extend_from_slice(&index.to_be_bytes());
    bytes.extend_from_slice(&term.to_be_bytes());
    bytes.push(3); // noop kind tag
    let checksum = crc32(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

fn frame(entry_bytes: &[u8]) -> Vec<u8> {
    let len = u32::try_from(entry_bytes.len()).expect("test frames are small");
    let mut framed = len.to_be_bytes().to_vec();
    framed.extend_from_slice(entry_bytes);
    framed
}

/// Baseline. Without this the forgeries below could be rejected for being
/// malformed rather than for naming an unadvanceable index, and every
/// rejection test would pass vacuously.
#[test]
fn forged_noop_entry_matches_the_shipped_encoder() {
    let encoded = encode_raft_log_entry(&PersistedRaftLogEntry::noop(LogIndex(7), Term(3)))
        .expect("a legal index encodes");
    assert_eq!(
        forged_noop_entry(7, 3),
        encoded,
        "the hand-rolled RFLE forgery must be byte-identical to the real encoder"
    );
}

/// Baseline. A marker one below the bound is ordinary, valid state; if this
/// stopped opening, the rejection tests would prove nothing about `u64::MAX`
/// in particular.
#[test]
fn compaction_marker_below_the_bound_still_opens() {
    let scratch = Scratch::new("marker-below");
    let log = scratch.join("log");
    write_file(&log, &[]);
    write_file(
        &scratch.join("log.compact"),
        &compaction_marker(u64::MAX - 1),
    );

    let segment = FileRaftLogSegment::open(&log).expect("a marker below the bound opens");
    assert_eq!(segment.compacted_through(), LogIndex(u64::MAX - 1));
    assert_eq!(
        segment.next_index(),
        LogIndex(u64::MAX),
        "the retained suffix starts one past the compacted prefix"
    );
}

// ---------------------------------------------------------------------------
// The guard: exactly one value is refused, on every read path that reaches it.
// ---------------------------------------------------------------------------

/// A compaction marker of `u64::MAX` is a typed error on strict open.
///
/// `open.rs::replay_entries_strict` ends with
/// `ContiguousLogEntries::from_entries(compacted_through.next(), entries)`.
#[test]
fn compaction_marker_at_u64_max_is_a_typed_error_on_strict_open() {
    let scratch = Scratch::new("marker-max");
    let log = scratch.join("log");
    write_file(&log, &[]);
    write_file(&scratch.join("log.compact"), &compaction_marker(u64::MAX));

    let result = FileRaftLogSegment::open(&log);
    assert!(
        matches!(result, Err(OpenRaftLogSegmentError::CompactionMarker(_))),
        "a compaction marker of u64::MAX must be a typed error, got {result:?}"
    );
}

/// The same value on the repair path. Repair is permitted to discard an
/// uncommitted tail, never to invent a compacted prefix it cannot represent.
#[test]
fn compaction_marker_at_u64_max_is_a_typed_error_on_repair_open() {
    let scratch = Scratch::new("marker-max-repair");
    let log = scratch.join("log");
    write_file(&log, &[]);
    write_file(&scratch.join("log.compact"), &compaction_marker(u64::MAX));

    let result = FileRaftLogSegment::open_repairing_uncommitted_tail(&log, LogIndex(0));
    assert!(
        matches!(result, Err(OpenRaftLogSegmentError::CompactionMarker(_))),
        "a compaction marker of u64::MAX must be a typed error, got {result:?}"
    );
}

/// An RFLE entry whose `index` field is `u64::MAX` overflowed the contiguity
/// walk (`validate_contiguous` does `expected = expected.next()` after matching
/// the last entry). The marker places the retained suffix exactly at `u64::MAX`
/// so the frame is otherwise accepted as the contiguous first entry.
#[test]
fn log_entry_index_at_u64_max_is_a_typed_error_on_open() {
    let scratch = Scratch::new("entry-max");
    let log = scratch.join("log");
    write_file(&log, &frame(&forged_noop_entry(u64::MAX, 1)));
    write_file(
        &scratch.join("log.compact"),
        &compaction_marker(u64::MAX - 1),
    );

    let result = FileRaftLogSegment::open(&log);
    assert!(
        matches!(result, Err(OpenRaftLogSegmentError::Replay(_))),
        "an entry index of u64::MAX must be a typed error, got {result:?}"
    );
}

/// The same entry index without a marker. Before the fix this opened and then
/// panicked (debug) or wrapped (release) the moment `next_index()` was
/// consulted — which `RaftLogSegment::append_entries` does unconditionally.
#[test]
fn log_entry_index_at_u64_max_never_yields_a_consultable_segment() {
    let scratch = Scratch::new("next-index-max");
    let log = scratch.join("log");
    write_file(&log, &frame(&forged_noop_entry(u64::MAX, 1)));

    let result = FileRaftLogSegment::open(&log);
    assert!(
        result.is_err(),
        "an entry index of u64::MAX must never produce a segment whose next_index() \
         can be consulted, got {result:?}"
    );
}

/// The corrupt marker reaches the production bundle entry point, so the blast
/// radius was a whole replica.
#[test]
fn node_stores_bundle_open_rejects_the_corrupt_marker() {
    let scratch = Scratch::new("bundle");
    write_file(&scratch.join("log"), &[]);
    write_file(&scratch.join("log.compact"), &compaction_marker(u64::MAX));

    let result = FileRaftNodeStores::open(&scratch.path);
    assert!(
        result.is_err(),
        "the bundle must return a typed error for a corrupt marker"
    );
}

/// The defect's payload: once `next_index()` had wrapped to `LogIndex(0)`, the
/// segment accepted and durably appended an entry at the zero sentinel,
/// permanently corrupting the log's index space while reporting `Healthy`.
///
/// This asserts the whole chain is unreachable: the store does not open, and no
/// zero-sentinel entry is ever written.
#[test]
fn a_wrapped_next_index_can_no_longer_admit_an_entry_at_the_zero_sentinel() {
    let scratch = Scratch::new("wrap-append");
    let log = scratch.join("log");
    write_file(&log, &frame(&forged_noop_entry(u64::MAX, 1)));
    write_file(
        &scratch.join("log.compact"),
        &compaction_marker(u64::MAX - 1),
    );

    let result = FileRaftLogSegment::open(&log);
    assert!(
        result.is_err(),
        "the segment that produced the wrap must not open, got {result:?}"
    );

    // And independently: no segment ever reports a next index at the sentinel,
    // so the zero-index append the wrap enabled has no other way in.
    let mut fresh = InMemoryRaftLogSegment::new();
    assert_eq!(fresh.next_index(), LogIndex(1));
    assert!(
        fresh
            .append_entries(&[PersistedRaftLogEntry::noop(LogIndex(0), Term(1))])
            .is_err(),
        "an entry at the zero sentinel index must be rejected"
    );
}

// ---------------------------------------------------------------------------
// Symmetry: the format must not be able to write what it refuses to read.
// ---------------------------------------------------------------------------

/// A guard on the read path alone would leave the store able to durably record
/// an entry it could never reopen. The encoder refuses the same index.
#[test]
fn the_encoder_refuses_the_index_the_decoder_refuses() {
    let result = encode_raft_log_entry(&PersistedRaftLogEntry::noop(LogIndex(u64::MAX), Term(1)));
    assert_eq!(result, Err(EncodeRaftLogEntryError::IndexAtMaximum));
}

/// The last legally reachable append boundary. A segment may sit with
/// `next_index() == u64::MAX`; the append that would occupy it fails loudly
/// instead of writing an unreadable frame or overflowing the walk.
#[test]
fn an_append_at_the_maximum_index_fails_loudly_rather_than_wrapping() {
    let scratch = Scratch::new("append-max");
    let log = scratch.join("log");
    write_file(&log, &[]);
    write_file(
        &scratch.join("log.compact"),
        &compaction_marker(u64::MAX - 1),
    );

    let mut segment = FileRaftLogSegment::open(&log).expect("opens at the boundary");
    assert_eq!(segment.next_index(), LogIndex(u64::MAX));

    let appended =
        segment.append_entries(&[PersistedRaftLogEntry::noop(LogIndex(u64::MAX), Term(1))]);
    assert!(
        appended.is_err(),
        "an append at the maximum index must be refused, got {appended:?}"
    );
    assert_eq!(
        segment.next_index(),
        LogIndex(u64::MAX),
        "a refused append must not move the boundary"
    );
    assert_eq!(
        fs::metadata(&log).expect("stat log").len(),
        0,
        "a refused append must not reach the file"
    );
}

/// The append bound belongs to the trait, so both shipped implementations
/// answer with the same typed error.
///
/// Adopted from the gen-7 reproduction: the append guard used to live only in
/// `encode_raft_log_entry`, so `FileRaftLogSegment` was bounded because it
/// encodes and `InMemoryRaftLogSegment`, which encodes nothing, was not bounded
/// at all.
#[test]
fn an_append_at_the_maximum_index_is_refused_by_both_segments() {
    let scratch = Scratch::new("append-max-both");
    let log = scratch.join("log");
    write_file(&log, &[]);
    write_file(
        &scratch.join("log.compact"),
        &compaction_marker(u64::MAX - 1),
    );

    let mut file_segment = FileRaftLogSegment::open(&log).expect("opens at the boundary");
    assert_eq!(
        file_segment.append_entries(&[PersistedRaftLogEntry::noop(LogIndex(u64::MAX), Term(1))]),
        Err(RaftLogSegmentAppendError::IndexAtMaximum)
    );

    let mut memory_segment = segment_at_the_append_boundary();
    assert_eq!(
        memory_segment.append_entries(&[PersistedRaftLogEntry::noop(LogIndex(u64::MAX), Term(1))]),
        Err(RaftLogSegmentAppendError::IndexAtMaximum)
    );
}

/// The borrowed entry point is a separate implementation on both segments, so
/// it carries the bound separately too.
#[test]
fn a_borrowed_append_at_the_maximum_index_is_refused_by_both_segments() {
    let scratch = Scratch::new("append-max-borrowed");
    let log = scratch.join("log");
    write_file(&log, &[]);
    write_file(
        &scratch.join("log.compact"),
        &compaction_marker(u64::MAX - 1),
    );
    let entry = PersistedRaftLogEntry::noop(LogIndex(u64::MAX), Term(1));

    let mut file_segment = FileRaftLogSegment::open(&log).expect("opens at the boundary");
    assert_eq!(
        file_segment
            .append_entries_borrowed([BorrowedPersistedRaftLogEntry::from(&entry)].into_iter()),
        Err(RaftLogSegmentAppendError::IndexAtMaximum)
    );

    let mut memory_segment = segment_at_the_append_boundary();
    assert_eq!(
        memory_segment
            .append_entries_borrowed([BorrowedPersistedRaftLogEntry::from(&entry)].into_iter()),
        Err(RaftLogSegmentAppendError::IndexAtMaximum)
    );
}

/// Compaction is the other way a marker gets written. Both implementations
/// refuse the boundary the RFLC decoder would refuse to read back.
#[test]
fn compaction_through_the_maximum_index_is_refused_by_both_segments() {
    let scratch = Scratch::new("compact-max");
    let log = scratch.join("log");
    write_file(&log, &[]);

    let mut file_segment = FileRaftLogSegment::open(&log).expect("opens");
    assert_eq!(
        file_segment.compact_prefix_through(LogIndex(u64::MAX)),
        Err(RaftLogSegmentCompactError::ThroughIndexAtMaximum)
    );
    assert!(
        !scratch.join("log.compact").exists(),
        "a refused compaction must not publish a marker"
    );

    let mut memory_segment = InMemoryRaftLogSegment::new();
    assert_eq!(
        memory_segment.compact_prefix_through(LogIndex(u64::MAX)),
        Err(RaftLogSegmentCompactError::ThroughIndexAtMaximum)
    );
}

// ---------------------------------------------------------------------------
// The gen-7 reproduction, adopted: the in-memory segment on its own.
// ---------------------------------------------------------------------------

/// The boundary is legally reachable: `u64::MAX - 1` is an accepted compaction
/// boundary on both segments (the shipped
/// `the_bound_rejects_exactly_one_value_on_every_read_path` asserts exactly
/// that), and it places `next_index()` at `u64::MAX`.
fn segment_at_the_append_boundary() -> InMemoryRaftLogSegment {
    let mut segment = InMemoryRaftLogSegment::new();
    segment
        .compact_prefix_through(LogIndex(u64::MAX - 1))
        .expect("u64::MAX - 1 is an accepted compaction boundary");
    assert_eq!(segment.next_index(), LogIndex(u64::MAX));
    segment
}

/// The file segment's behaviour, restated so the asymmetry is visible in one
/// place: an append at `u64::MAX` is a typed error there.
#[test]
fn gen7_the_in_memory_segment_refuses_the_index_the_file_segment_refuses() {
    let mut segment = segment_at_the_append_boundary();

    let appended =
        segment.append_entries(&[PersistedRaftLogEntry::noop(LogIndex(u64::MAX), Term(1))]);

    assert!(
        appended.is_err(),
        "an append at the maximum index must be refused by every RaftLogSegment \
         implementation, not only the one that happens to encode: got {appended:?}"
    );
    assert_eq!(
        segment.next_index(),
        LogIndex(u64::MAX),
        "a refused append must not move the boundary"
    );
}

/// The release-profile half, which is the one the fix's commit message calls
/// the dangerous one: a wrapped `next_index()` reports `LogIndex(0)`, the
/// sentinel meaning "before the first entry" — the exact signature quoted in
/// `6cc9be23` ("the store opened Healthy, reported `next_index()` ==
/// `LogIndex(0)`").
///
/// In debug this panicked inside `validate_contiguous`, which does
/// `expected = expected.next()` after matching the last entry.
#[test]
fn gen7_the_in_memory_segment_never_reports_a_wrapped_next_index() {
    let mut segment = segment_at_the_append_boundary();
    let _ = segment.append_entries(&[PersistedRaftLogEntry::noop(LogIndex(u64::MAX), Term(1))]);

    assert!(
        segment.next_index() > segment.compacted_through(),
        "next_index {:?} must stay above compacted_through {:?}",
        segment.next_index(),
        segment.compacted_through(),
    );
    assert_ne!(
        segment.next_index(),
        LogIndex::ZERO,
        "next_index must never wrap to the zero sentinel"
    );
}

/// Baseline: one below the bound is ordinary state on the in-memory segment,
/// so the two tests above are about `u64::MAX` and nothing else.
#[test]
fn gen7_the_in_memory_segment_accepts_the_largest_advanceable_index() {
    let mut segment = InMemoryRaftLogSegment::new();
    segment
        .compact_prefix_through(LogIndex(u64::MAX - 2))
        .expect("accepted boundary");
    segment
        .append_entries(&[PersistedRaftLogEntry::noop(LogIndex(u64::MAX - 1), Term(1))])
        .expect("the largest advanceable index appends");
    assert_eq!(segment.next_index(), LogIndex(u64::MAX));
}

// ---------------------------------------------------------------------------
// Scope: what the guard does and does not cover, one test per boundary.
// ---------------------------------------------------------------------------

/// The guard rejects exactly one value, not a range. `u64::MAX - 1` is ordinary
/// state on every read path, and a guard that widened to a range would fail
/// here rather than silently shrink the usable index space.
#[test]
fn the_bound_rejects_exactly_one_value_on_every_read_path() {
    let scratch = Scratch::new("off-by-one");
    let log = scratch.join("log");
    write_file(&log, &frame(&forged_noop_entry(u64::MAX - 1, 1)));
    write_file(
        &scratch.join("log.compact"),
        &compaction_marker(u64::MAX - 2),
    );

    let segment = FileRaftLogSegment::open(&log).expect("strict open accepts the boundary-1 case");
    assert_eq!(segment.next_index(), LogIndex(u64::MAX));

    let repaired = FileRaftLogSegment::open_repairing_uncommitted_tail(&log, LogIndex(0))
        .expect("repair open accepts the boundary-1 case");
    assert_eq!(repaired.next_index(), LogIndex(u64::MAX));

    assert!(
        encode_raft_log_entry(&PersistedRaftLogEntry::noop(
            LogIndex(u64::MAX - 1),
            Term(1)
        ))
        .is_ok(),
        "the encoder must still accept the largest advanceable index"
    );

    let mut memory_segment = InMemoryRaftLogSegment::new();
    assert!(memory_segment
        .compact_prefix_through(LogIndex(u64::MAX - 1))
        .is_ok());
}

/// OUTSIDE the guard: `LogIndex::ZERO`. Zero is a valid non-advancing sentinel,
/// so the successor bound has nothing to say about it. A frame naming index 0
/// is nonetheless unwritable by this crate — `first_index` is always at least
/// one — and strict open drops it as already covered by the compacted prefix.
/// This pins that behaviour so it changes deliberately, not by accident.
#[test]
fn the_zero_sentinel_is_outside_the_bound_and_is_dropped_by_replay() {
    let scratch = Scratch::new("zero-sentinel");
    let log = scratch.join("log");
    write_file(&log, &frame(&forged_noop_entry(0, 1)));

    let segment = FileRaftLogSegment::open(&log).expect("a zero-index frame is not a decode error");
    assert_eq!(
        segment.next_index(),
        LogIndex(1),
        "the zero-index frame is filtered by the compacted-prefix rule, not admitted"
    );
    assert!(segment.replay_entries().is_empty());
}

/// A sweep over the whole neighbourhood of the bound, on both open modes.
/// Nothing may panic in either profile, and any segment that does open must
/// report a boundary that is strictly above its compacted prefix — the exact
/// property the wrap destroyed.
#[test]
fn no_marker_or_index_near_the_bound_ever_wraps_or_panics() {
    let interesting = [0, 1, 2, u64::MAX / 2, u64::MAX - 2, u64::MAX - 1, u64::MAX];
    let scratch = Scratch::new("sweep");
    let log = scratch.join("log");
    let marker_path = scratch.join("log.compact");

    let mut opened = 0_usize;
    for marker in interesting {
        for index in interesting {
            write_file(&log, &frame(&forged_noop_entry(index, 1)));
            write_file(&marker_path, &compaction_marker(marker));

            for segment in [
                FileRaftLogSegment::open(&log).ok(),
                FileRaftLogSegment::open_repairing_uncommitted_tail(&log, LogIndex(0)).ok(),
            ]
            .into_iter()
            .flatten()
            {
                opened += 1;
                assert!(
                    segment.next_index() > segment.compacted_through(),
                    "marker {marker} with entry index {index} opened with a wrapped boundary: \
                     next_index {:?} is not above compacted_through {:?}",
                    segment.next_index(),
                    segment.compacted_through(),
                );
            }
        }
    }
    assert!(opened > 0, "the sweep never reached an accepting path");
}

// ---------------------------------------------------------------------------
// The closure claim, mechanized.
// ---------------------------------------------------------------------------

/// Every log position this crate builds out of bytes it read must pass through
/// `format::advanceable_log_index`, or be listed below with the reason it does
/// not need to.
///
/// This is the closure claim "these are the only decoded log positions" as a
/// check rather than a paragraph. A new decode site that skips the guard fails
/// this test, and an exemption that stops matching real source fails it too.
/// The scan works on whitespace-normalized source so a construction split
/// across lines cannot slip past it.
#[test]
fn every_decoded_log_position_is_bounded_or_explicitly_exempt() {
    // (production file relative to the crate root, expression as written in
    // that file without its trailing punctuation, why it is exempt)
    const EXEMPT: &[(&str, &str, &str)] = &[
        (
            "src/format/v1/snapshot_metadata.rs",
            "let last_included_index = LogIndex(reader.u64()?)",
            "bounded downstream by RaftSnapshotMetadata::new, which rejects u64::MAX",
        ),
        (
            "src/format/v1/snapshot_metadata.rs",
            "index: LogIndex(reader.u64()?)",
            "committed-configuration index: compared and stored, never advanced",
        ),
        (
            "src/format/v1/hard_state.rs",
            "let commit_index = LogIndex(reader.u64()?)",
            "commit index: compared against replayed indexes, never advanced",
        ),
        (
            "src/format/v1/hard_state.rs",
            "let index = LogIndex(reader.u64()?)",
            "absent-configuration canonicality check: must equal LogIndex::ZERO",
        ),
        (
            "src/format/v1/hard_state.rs",
            "index: LogIndex(reader.u64()?)",
            "committed-configuration index: compared and stored, never advanced",
        ),
        (
            "src/raft_snapshot_store/inventory/scan.rs",
            "last_included_index: LogIndex(fields.next()?.parse().ok()?)",
            "parsed from a file name for ordering only; never advanced",
        ),
    ];

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut unguarded = Vec::new();
    let mut seen_exempt = vec![false; EXEMPT.len()];

    // Both sides go through the same normalizer, so the exemptions can be
    // written the way a maintainer would read them.
    let exempt = EXEMPT
        .iter()
        .map(|(path, source, _)| (*path, normalize_source(source)))
        .collect::<Vec<_>>();

    for file in production_sources(&root.join("src")) {
        let relative = file
            .strip_prefix(root)
            .expect("source is under the crate root")
            .to_string_lossy()
            .replace('\\', "/");
        let contents = fs::read_to_string(&file).expect("read source");
        for expression in decoded_log_index_expressions(&contents) {
            match exempt
                .iter()
                .position(|(path, source)| *path == relative && *source == expression)
            {
                Some(index) => seen_exempt[index] = true,
                None => unguarded.push(format!("{relative}: {expression}")),
            }
        }
    }

    assert!(
        unguarded.is_empty(),
        "these decoded log positions reach a LogIndex without passing through \
         advanceable_log_index. Either route them through it, or add them to EXEMPT \
         with the reason they are never advanced:\n  {}",
        unguarded.join("\n  ")
    );

    let stale = EXEMPT
        .iter()
        .zip(&seen_exempt)
        .filter(|(_, seen)| !**seen)
        .map(|((path, source, _), _)| format!("{path}: {source}"))
        .collect::<Vec<_>>();
    assert!(
        stale.is_empty(),
        "these EXEMPT entries no longer match any source line; the exemption list \
         must describe the code as it is:\n  {}",
        stale.join("\n  ")
    );
}

/// Collapses source to a wrapping-independent form.
///
/// Whitespace between two identifier characters becomes a single space; all
/// other whitespace is removed. A method chain broken across lines therefore
/// normalizes to the same text as the single-line form, which is the property
/// the scan depends on — `reader\n.u64()` must read as `reader.u64()`.
fn normalize_source(source: &str) -> String {
    fn is_identifier_char(character: char) -> bool {
        character.is_alphanumeric() || character == '_'
    }

    let mut normalized = String::with_capacity(source.len());
    let mut characters = source.chars().peekable();
    while let Some(character) = characters.next() {
        if !character.is_whitespace() {
            normalized.push(character);
            continue;
        }
        while characters.peek().is_some_and(|next| next.is_whitespace()) {
            characters.next();
        }
        let previous_is_identifier = normalized
            .chars()
            .next_back()
            .is_some_and(is_identifier_char);
        let next_is_identifier = characters.peek().copied().is_some_and(is_identifier_char);
        if previous_is_identifier && next_is_identifier {
            normalized.push(' ');
        }
    }
    normalized
}

/// Every `LogIndex(..)` in `contents` whose argument reads bytes or text this
/// crate decoded, reported as its enclosing binding or field expression.
///
/// Comments are dropped first, then the source is normalized, so the result
/// does not depend on how the source happens to be wrapped.
fn decoded_log_index_expressions(contents: &str) -> Vec<String> {
    // Marks that the argument came from bytes or text this crate read, rather
    // than from a value a caller already typed. Guarded sites do not appear at
    // all: `advanceable_log_index` builds the `LogIndex` itself.
    const FROM_EXTERNAL_BYTES: &[&str] = &["reader.", "from_be_bytes", "from_le_bytes", ".parse()"];
    const STATEMENT_BOUNDARIES: &[char] = &[';', '{', '}', ','];

    let code = contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    let normalized = normalize_source(&code);

    let mut found = Vec::new();
    let bytes = normalized.as_bytes();
    for (start, _) in normalized.match_indices("LogIndex(") {
        let open = start + "LogIndex".len();
        let mut depth = 0_usize;
        let mut end = None;
        for (offset, byte) in bytes[open..].iter().enumerate() {
            match byte {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(open + offset + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(end) = end else { continue };
        // A call wrapped across lines carries a trailing comma the single-line
        // form does not. Drop it so both spellings key the same.
        let argument = normalized[open + 1..end - 1].trim_end_matches(',');
        if !FROM_EXTERNAL_BYTES
            .iter()
            .any(|mark| argument.contains(mark))
        {
            continue;
        }
        let statement_start = normalized[..start]
            .rfind(STATEMENT_BOUNDARIES)
            .map_or(0, |boundary| boundary + 1);
        let prefix = normalized[statement_start..start].trim();
        found.push(format!("{prefix}LogIndex({argument})"));
    }
    found
}

/// Production sources only. Test modules live beside the code they cover, and
/// a fixture that names an index is not a decode site.
fn production_sources(directory: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in fs::read_dir(&current).expect("read source directory") {
            let path = entry.expect("read directory entry").path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().is_some_and(|extension| extension == "rs") {
                let name = path
                    .file_name()
                    .expect("file has a name")
                    .to_string_lossy()
                    .into_owned();
                let is_test_module = name.ends_with("_test.rs")
                    || name.ends_with("_tests.rs")
                    || name == "tests.rs"
                    || name == "test_support.rs";
                if !is_test_module {
                    files.push(path);
                }
            }
        }
    }
    files.sort();
    files
}
