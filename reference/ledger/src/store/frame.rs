//! Encoding a header and a whole transaction frame, and deciding what a tail is.
//!
//! The recognition order this module implements — a zero-filled remainder
//! first, then identity at every length, then the seal, then the version
//! wherever the field is present, and only then anything that depends on how
//! many bytes there are — is argued in the [module documentation](super)'s
//! section on which residue an opening may truncate, and pinned as a table by
//! `the_recognition_order_matches_the_sibling_lock_store`.

use rafter::LogIndex;

use crate::{adapter::codec::encode_snapshot, Ledger, LedgerConfig};

use super::{
    damage::TornTail,
    error::LedgerStoreError,
    format::{
        crc32, read_u32, read_u64, BEGIN_LEN, BEGIN_MAGIC, COMMIT_LEN, COMMIT_MAGIC, HEADER_LEN,
        JOURNAL_FORMAT_VERSION, JOURNAL_MAGIC, SEALED_FRAME_MARK, UNSEALED_FRAME_MARK,
    },
};

/// One committed frame's image and total length.
pub(super) struct Frame<'a> {
    pub(super) image: &'a [u8],
    pub(super) len: usize,
}

/// Returns the four bytes where a begin magic belongs, zero-padded when fewer
/// than four are present.
fn magic_of(bytes: &[u8]) -> [u8; 4] {
    let mut magic = [0_u8; 4];
    let present = bytes.len().min(magic.len());
    magic[..present].copy_from_slice(&bytes[..present]);
    magic
}

/// Checks that the bytes present carry this store's begin magic, as far as they
/// go.
///
/// This runs on **every** tail long enough to carry a byte of it, before the
/// mark decides anything. The ordering is the whole of the fix behind it. The
/// older shape asked the mark first and reached the magic only underneath it, so
/// a committed frame whose leading bytes were zeroed came back from
/// [`classify_unsealed`] as a corrupt begin record, that answer was folded into
/// [`TornTail::UnsealedAppend`], and `open` deleted the frame and every
/// committed frame after it. Two adjacent zero bytes were enough — one 16-bit
/// word, far under a sector — while one was correctly refused.
///
/// Byte zero is the append mark and is checked here too, because it is the
/// magic's leading byte: a tail beginning with neither mark is not a frame this
/// build ever wrote. *Which* of the two marks it is decides nothing here — that
/// question belongs to [`read_frame`], and answering it needs more than this
/// byte.
///
/// The version byte is deliberately *not* tested here. It sits behind the seal
/// test, and [`classify_unsealed`] explains why an unsealed frame's version is
/// reached through the same path as everything else it declares rather than
/// ahead of it. It is still consulted at every length that carries it.
///
/// This is the sibling lock store's `verify_identity`, statement for statement.
/// The two stores are one design in two formats, and this is the function the
/// fourth-generation hunt found them disagreeing on, one commit after they were
/// aligned on the version byte beside it.
fn verify_identity(bytes: &[u8]) -> Result<(), TornTail> {
    if bytes[0] != UNSEALED_FRAME_MARK && bytes[0] != SEALED_FRAME_MARK {
        return Err(TornTail::NotALedgerFrame {
            magic: magic_of(bytes),
        });
    }
    let present = bytes.len().min(BEGIN_MAGIC.len());
    if bytes[1..present] != BEGIN_MAGIC[1..present] {
        return Err(TornTail::NotALedgerFrame {
            magic: magic_of(bytes),
        });
    }
    Ok(())
}

/// Reads one frame from the front of `bytes`, or says why it is not committed.
///
/// Three questions, in this order, and the order is the mechanism:
///
/// 1. **Is the rest of the file zeros?** Then it is the one residue a
///    delayed-allocation crash leaves at every length, and the identity test
///    below would refuse it. [`TornTail::ZeroFilledToEnd`] says so under its own
///    name rather than borrowing the interrupted-append verdict, because it is
///    truncated on a different premise; [`TornTail::is_truncatable_residue`]
///    holds the two apart.
/// 2. **Do these bytes carry this build's begin identity?** `verify_identity`
///    asks at every length, before the mark decides anything. A zero run that
///    reaches byte one has destroyed the identity, and destroying it is not
///    something an append does.
/// 3. **Is the mark sealed?** Only now, and what it decides is narrower than it
///    used to be. An unsealed mark is one byte, and one byte is not evidence:
///    `b'R'` rots to `0x00` as easily as any other byte rots to any other value.
///    So an unsealed mark settles nothing by itself — it sends these bytes to
///    `classify_unsealed`, which asks whether they are a whole frame. A sealed
///    mark does settle it, and may, because the checksums cover the sealed
///    value: a frame whose mark reads sealed is one whose mark byte is under the
///    same checksum as every other byte of its begin record.
///
/// Step 1 above step 2 is the one place this store trades a refusal for a
/// truncation, and it is the trade named in [`TornTail::is_truncatable_residue`]
/// rather than an accident of ordering.
///
/// Every other zero run refuses, and this used to say why in a sentence with
/// two halves that are not the same set: "any run with a single non-zero byte
/// anywhere after it, **which is every run that has a committed frame behind
/// it**". The first half is the mechanism and is true. The second is a claim
/// about what the mechanism is *for*, and its counterexample is an interrupted
/// append whose leading bytes never reached the medium — a run with non-zero
/// bytes after it and nothing committed behind it anywhere, refused all the
/// same. Nothing is lost by refusing it, and something would be lost by
/// accepting it, because a committed final frame that a zeroed region hit at
/// its front and stopped short of its end leaves the same bytes. Both halves
/// are enumerated under [`TornTail::is_truncatable_residue`] now rather than
/// joined by "which is".
///
/// One run does not reach step 2 at all: a single zeroed byte at a frame's
/// front is the unsealed mark, passes the identity test with bytes one through
/// three intact, and is refused by step 3's re-read as
/// [`TornTail::UnsealedCompleteFrame`]. It refuses; it does not refuse *here*.
///
/// `bytes` is never empty — the scan stops before calling this on nothing.
pub(super) fn read_frame(bytes: &[u8]) -> Result<Frame<'_>, TornTail> {
    if bytes.iter().all(|byte| *byte == UNSEALED_FRAME_MARK) {
        return Err(TornTail::ZeroFilledToEnd {
            present: bytes.len() as u64,
        });
    }
    verify_identity(bytes)?;
    if bytes[0] == UNSEALED_FRAME_MARK {
        return Err(classify_unsealed(bytes));
    }

    read_sealed_frame(bytes)
}

/// Says what an unsealed tail is, by asking whether it is a whole frame.
///
/// This is the half of the truncation rule the mark cannot supply on its own.
/// The mark says "these bytes were not sealed"; that is compatible with an
/// append that never finished *and* with a finished append whose one mark byte
/// later rotted to zero, and those two are the same bytes. What separates them
/// is not the mark but the rest of the frame: an append that never finished left
/// a **prefix**, and a prefix is not a whole frame.
///
/// So the bytes are read again with the mark restored to its sealed value — the
/// value every checksum in the frame was computed over. Three answers, and each
/// is a different fact:
///
/// - The bytes are a whole frame that verifies. Then nothing about them is
///   incomplete, and the only thing wrong is the mark. That is
///   [`TornTail::UnsealedCompleteFrame`], which is not residue: see its
///   documentation for why this store refuses rather than choosing.
/// - The bytes declare a format version this build cannot read. Then this build
///   cannot tell a whole frame from a prefix at all, because it does not know
///   the layout, so it cannot produce the evidence truncating requires.
///   [`TornTail::UnsupportedFrameVersion`] is refused by both entry points, and
///   for the same reason as a sealed frame carrying it: the remedy for damage
///   must not delete a newer build's committed work.
/// - The bytes fail to be a whole frame in some way this build *can* read: too
///   short for a begin record, a begin record that does not verify, an image
///   that is not all there, a missing or partial commit record. That is positive
///   evidence of incompleteness, and with the unsealed mark beside it, it is
///   [`TornTail::UnsealedAppend`].
///
/// The copy is deliberate rather than clever. Recovery has already read the
/// whole journal into memory, this runs at most once per scan — the scan stops
/// at the first frame it cannot read — and a byte-substituting checksum would
/// buy nothing but a second implementation of the fold to keep honest.
fn classify_unsealed(bytes: &[u8]) -> TornTail {
    let mut sealed = bytes.to_vec();
    sealed[0] = SEALED_FRAME_MARK;
    match read_sealed_frame(&sealed) {
        Ok(frame) => TornTail::UnsealedCompleteFrame {
            len: frame.len as u64,
        },
        Err(TornTail::UnsupportedFrameVersion { version }) => {
            TornTail::UnsupportedFrameVersion { version }
        }
        Err(_) => TornTail::UnsealedAppend {
            present: bytes.len() as u64,
        },
    }
}

/// Reads one frame whose first byte is the sealed mark.
///
/// Every check below runs over bytes the frame's own checksums cover, mark
/// included, so reaching any of these answers means the bytes present are not
/// what a completed append left.
///
/// There is no magic test here, and its absence is structural rather than an
/// omission: `verify_identity` establishes the magic at every length above
/// both callers, and neither can be reached without it. Repeating it here would
/// make the ordering a coincidence of two tests agreeing instead of a property
/// of the one path.
fn read_sealed_frame(bytes: &[u8]) -> Result<Frame<'_>, TornTail> {
    // The version is read wherever the field is present, ahead of anything that
    // depends on how many bytes there are. The argument for refusing a foreign
    // version is about the field, so gating it on a full begin record would make
    // the same byte refused at one length and unreached at another — which is
    // exactly the disagreement with the sibling lock store that
    // `the_recognition_order_matches_the_sibling_lock_store` now pins.
    //
    // Folding it into the corruption below would also make a downgrade
    // indistinguishable from a torn write, and the remedy for a torn write
    // discards the frame.
    if let Some(version) = bytes.get(4) {
        if *version != JOURNAL_FORMAT_VERSION {
            return Err(TornTail::UnsupportedFrameVersion { version: *version });
        }
    }

    let Some(begin) = bytes.get(..BEGIN_LEN) else {
        return Err(TornTail::PartialBeginRecord);
    };
    if read_u32(&begin[13..17]) != crc32(&begin[..13]) {
        return Err(TornTail::BeginRecordCorrupt);
    }

    let image_len = read_u32(&begin[5..9]) as usize;
    let image_crc = read_u32(&begin[9..13]);
    let Some(image) = bytes.get(BEGIN_LEN..BEGIN_LEN + image_len) else {
        return Err(TornTail::PartialImage);
    };
    if crc32(image) != image_crc {
        return Err(TornTail::ImageCorrupt);
    }

    let commit_start = BEGIN_LEN + image_len;
    let available = bytes.len() - commit_start;
    if available == 0 {
        return Err(TornTail::MissingCommitRecord);
    }
    let Some(commit) = bytes.get(commit_start..commit_start + COMMIT_LEN) else {
        return Err(TornTail::PartialCommitRecord);
    };
    if commit[..4] != COMMIT_MAGIC
        || commit[4] != JOURNAL_FORMAT_VERSION
        || read_u32(&commit[9..13]) != crc32(&commit[..9])
        || read_u32(&commit[5..9]) != crc32(&bytes[..commit_start])
    {
        return Err(TornTail::CommitRecordCorrupt);
    }

    Ok(Frame {
        image,
        len: commit_start + COMMIT_LEN,
    })
}

/// Validates the journal header against `config`.
pub(super) fn verify_header(bytes: &[u8], config: LedgerConfig) -> Result<(), LedgerStoreError> {
    let Some(header) = bytes.get(..HEADER_LEN) else {
        return Err(LedgerStoreError::HeaderTruncated {
            length: bytes.len() as u64,
        });
    };
    if header[..4] != JOURNAL_MAGIC {
        let mut magic = [0_u8; 4];
        magic.copy_from_slice(&header[..4]);
        return Err(LedgerStoreError::NotALedgerJournal { magic });
    }
    if header[4] != JOURNAL_FORMAT_VERSION {
        return Err(LedgerStoreError::UnsupportedFormatVersion { version: header[4] });
    }
    let expected = read_u32(&header[17..21]);
    let found = crc32(&header[..17]);
    if expected != found {
        return Err(LedgerStoreError::HeaderChecksumMismatch { expected, found });
    }

    let journal_max_clients = read_u32(&header[5..9]);
    let journal_max_accounts = read_u64(&header[9..17]);
    let requested_max_accounts = config.max_accounts() as u64;
    if journal_max_clients != config.max_clients() || journal_max_accounts != requested_max_accounts
    {
        return Err(LedgerStoreError::ConfigMismatch {
            journal_max_clients,
            journal_max_accounts,
            requested_max_clients: config.max_clients(),
            requested_max_accounts,
        });
    }
    Ok(())
}

/// Encodes the journal header for `config`.
pub(super) fn encode_header(config: LedgerConfig) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER_LEN);
    bytes.extend_from_slice(&JOURNAL_MAGIC);
    bytes.push(JOURNAL_FORMAT_VERSION);
    bytes.extend_from_slice(&config.max_clients().to_be_bytes());
    bytes.extend_from_slice(&(config.max_accounts() as u64).to_be_bytes());
    let checksum = crc32(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

/// Encodes one whole transaction frame.
pub(super) fn encode_frame(
    ledger: &Ledger,
    applied_index: LogIndex,
) -> Result<Vec<u8>, LedgerStoreError> {
    let image =
        encode_snapshot(applied_index.0, &ledger.snapshot()).map_err(LedgerStoreError::Image)?;
    let image_len = u32::try_from(image.len()).map_err(|_| LedgerStoreError::ImageTooLarge {
        length: image.len() as u64,
    })?;

    let mut frame = Vec::with_capacity(BEGIN_LEN + image.len() + COMMIT_LEN);
    frame.extend_from_slice(&BEGIN_MAGIC);
    frame.push(JOURNAL_FORMAT_VERSION);
    frame.extend_from_slice(&image_len.to_be_bytes());
    frame.extend_from_slice(&crc32(&image).to_be_bytes());
    let begin_checksum = crc32(&frame);
    frame.extend_from_slice(&begin_checksum.to_be_bytes());
    frame.extend_from_slice(&image);

    let frame_checksum = crc32(&frame);
    let mut commit = Vec::with_capacity(COMMIT_LEN);
    commit.extend_from_slice(&COMMIT_MAGIC);
    commit.push(JOURNAL_FORMAT_VERSION);
    commit.extend_from_slice(&frame_checksum.to_be_bytes());
    let commit_checksum = crc32(&commit);
    commit.extend_from_slice(&commit_checksum.to_be_bytes());

    frame.extend_from_slice(&commit);
    Ok(frame)
}
