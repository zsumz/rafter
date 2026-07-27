//! Encoding one whole slot image, and deciding what a slot's bytes are.
//!
//! The recognition order this module implements — the creation mark, then
//! identity at every length, then the seal, then the version wherever the field
//! is present, and only then anything that depends on how many bytes there are
//! — is argued in the `# Which unreadable slots recovery may skip` section of
//! the [module documentation](super), and pinned as a table by
//! `the_recognition_order_matches_the_sibling_ledger_store`.

use rafter::LogIndex;

use crate::{
    adapter::{decode_snapshot, encode_snapshot},
    LockConfig, LockService,
};

use super::{
    damage::SlotDamage,
    error::LockStoreError,
    format::{
        as_u64, crc32, read_u32, read_u64, SlotIndex, CREATION_MARK, HEADER_APPLIED_INDEX_OFFSET,
        HEADER_CHECKSUM_OFFSET, SEALED_MARK, SLOT_FORMAT_VERSION, SLOT_HEADER_LEN, SLOT_MAGIC,
        SLOT_TRAILER_LEN, UNSEALED_MARK,
    },
};

// Imported for the intra-doc links the prose below carries.
#[allow(unused_imports)]
use super::LockStore;

/// A slot whose header, payload, and trailing checksum all verified.
pub(super) struct SealedImage<'a> {
    pub(super) generation: u64,
    pub(super) applied_index: LogIndex,
    max_clients: u32,
    max_resources: u32,
    payload: &'a [u8],
}

/// A sealed slot's payload, restored through the model's validating path.
pub(super) struct DecodedImage {
    pub(super) service: LockService,
    pub(super) applied_index: LogIndex,
}

/// Returns the four bytes where a slot's magic belongs, zero-padded when the
/// slot is shorter than that.
fn magic_of(bytes: &[u8]) -> [u8; 4] {
    let mut magic = [0_u8; 4];
    let present = bytes.len().min(magic.len());
    magic[..present].copy_from_slice(&bytes[..present]);
    magic
}

/// Checks that the bytes present carry this store's magic, as far as they go.
///
/// This runs on **every** slot long enough to carry a byte of it, before
/// anything classifies the slot by its length. That ordering is the point: the
/// older shape put the magic test behind a full-header slice, so a short slot
/// was attributed to this build rather than shown to belong to it, and twenty
/// bytes of a foreign format were read as this build's own residue.
///
/// Byte zero is the publication mark and is checked here too, because it is the
/// magic's leading byte: a slot that begins with neither mark is not a slot
/// this build ever wrote. Which of the two marks it is decides nothing here —
/// that question belongs to [`verify_slot`], and answering it needs more than
/// this byte.
///
/// The version byte is deliberately *not* tested here. It sits behind the seal
/// test now, and `classify_unsealed` explains why an unsealed slot's version
/// is reached through the same path as everything else it declares rather than
/// ahead of it. It is still consulted at every length that carries it.
fn verify_identity(bytes: &[u8]) -> Result<(), SlotDamage> {
    if bytes[0] != UNSEALED_MARK && bytes[0] != SEALED_MARK {
        return Err(SlotDamage::NotALockImage {
            magic: magic_of(bytes),
        });
    }
    let present = bytes.len().min(SLOT_MAGIC.len());
    if bytes[1..present] != SLOT_MAGIC[1..present] {
        return Err(SlotDamage::NotALockImage {
            magic: magic_of(bytes),
        });
    }
    Ok(())
}

/// Verifies one slot's bytes, returning its sealed image or the damage found.
///
/// `Ok(None)` means the slot carries its creation mark and nothing has ever
/// been sealed into it, which is not damage. A slot of zero bytes is not that
/// case: creation writes the mark, so an empty file is something else's doing.
///
/// The mark decides less here than it used to, and the narrowing is the whole
/// of the fix behind it. An unsealed mark is one byte, and `b'R'` rots to `0x00`
/// as readily as any other byte rots to any other value, so an unsealed mark no
/// longer settles anything on its own: it sends the slot to
/// `classify_unsealed`, which asks whether these bytes are a whole image. A
/// sealed mark does settle it, and may, because both checksums are computed over
/// the sealed form — a slot whose mark reads sealed is a slot whose mark byte is
/// covered by the same checksum as every other byte of its header.
pub(super) fn verify_slot(bytes: &[u8]) -> Result<Option<SealedImage<'_>>, SlotDamage> {
    if bytes.is_empty() {
        return Err(SlotDamage::SlotEmptied);
    }
    if bytes == CREATION_MARK {
        return Ok(None);
    }
    // Magic first, at every length, then the seal, and only then anything that
    // depends on how many bytes are present.
    verify_identity(bytes)?;
    if bytes[0] == UNSEALED_MARK {
        return Err(classify_unsealed(bytes));
    }
    verify_sealed_slot(bytes).map(Some)
}

/// Says what an unsealed slot is, by asking whether it is a whole image.
///
/// This is the half of the skip rule the mark cannot supply by itself. The mark
/// says "these bytes were not sealed", which is true of a publication that never
/// finished *and* of a finished publication whose one mark byte later rotted to
/// zero — and those two are the same bytes. What separates them is not the mark
/// but the rest of the slot: a publication that never finished left a
/// **prefix**, and a prefix is not a whole image.
///
/// So the bytes are read again with the mark restored to the value both
/// checksums were computed over. Three answers, three different facts:
///
/// - The bytes are a whole image that verifies. Nothing about them is
///   incomplete, and the only thing wrong is the mark. That is
///   [`SlotDamage::UnsealedCompleteImage`], which is not residue; its
///   documentation gives the argument for refusing rather than choosing.
/// - The bytes declare a format version this build cannot read. Then this build
///   does not know the layout and cannot tell a whole image from a prefix at
///   all, so it cannot produce the evidence skipping requires.
///   [`SlotDamage::UnsupportedFormatVersion`] is a refusal, and it is one that
///   even [`LockStore::open_and_repair`] will not clear: a downgrade meeting a
///   newer build's committed image must not be answered by discarding it.
/// - The bytes fail to be a whole image in some way this build *can* read: a
///   header cut short, a header checksum over bytes that are all present, a
///   payload that is not all there, no trailer, a torn trailer, a trailer that
///   seals nothing, bytes beyond the seal. That is positive evidence of
///   incompleteness, and with the unsealed mark beside it, it is
///   [`SlotDamage::UnsealedPublication`].
///
/// The copy is deliberate rather than clever. Opening has already read the slot
/// into memory, this runs at most twice per open, and a byte-substituting
/// checksum would buy nothing but a second implementation of the fold to keep
/// honest.
fn classify_unsealed(bytes: &[u8]) -> SlotDamage {
    let mut sealed = bytes.to_vec();
    sealed[0] = SEALED_MARK;
    match verify_sealed_slot(&sealed) {
        Ok(image) => SlotDamage::UnsealedCompleteImage {
            len: as_u64(bytes.len()),
            generation: image.generation,
        },
        Err(SlotDamage::UnsupportedFormatVersion { version }) => {
            SlotDamage::UnsupportedFormatVersion { version }
        }
        Err(_) => SlotDamage::UnsealedPublication {
            present: as_u64(bytes.len()),
        },
    }
}

/// Verifies one slot whose first byte is the sealed mark.
///
/// Every check below runs over bytes both of the slot's checksums cover, mark
/// included, so reaching any of these answers means the bytes present are not
/// what a completed publication sealed.
pub(super) fn verify_sealed_slot(bytes: &[u8]) -> Result<SealedImage<'_>, SlotDamage> {
    // The version is read wherever the field is present, ahead of anything that
    // depends on how many bytes there are. The argument for refusing a foreign
    // version is about the field, so gating it on a full header would make the
    // same bytes refused at one length and adopted at another.
    if let Some(version) = bytes.get(4) {
        if *version != SLOT_FORMAT_VERSION {
            return Err(SlotDamage::UnsupportedFormatVersion { version: *version });
        }
    }

    let Some(header) = bytes.get(..SLOT_HEADER_LEN) else {
        return Err(SlotDamage::HeaderIncomplete {
            present: as_u64(bytes.len()),
        });
    };
    let declared_header_crc = read_u32(&header[HEADER_CHECKSUM_OFFSET..SLOT_HEADER_LEN]);
    let computed_header_crc = crc32(&header[..HEADER_CHECKSUM_OFFSET]);
    if declared_header_crc != computed_header_crc {
        return Err(SlotDamage::HeaderChecksumMismatch {
            declared: declared_header_crc,
            computed: computed_header_crc,
        });
    }

    // Only now is `payload_len` trustworthy enough to locate the trailer.
    let payload_len = read_u32(&header[29..33]) as usize;
    let Some(payload) = bytes.get(SLOT_HEADER_LEN..SLOT_HEADER_LEN + payload_len) else {
        return Err(SlotDamage::PayloadIncomplete {
            declared: as_u64(payload_len),
            present: as_u64(bytes.len() - SLOT_HEADER_LEN),
        });
    };

    let trailer_start = SLOT_HEADER_LEN + payload_len;
    let available = bytes.len() - trailer_start;
    if available == 0 {
        return Err(SlotDamage::MissingCommitChecksum);
    }
    let Some(trailer) = bytes.get(trailer_start..trailer_start + SLOT_TRAILER_LEN) else {
        return Err(SlotDamage::PartialCommitChecksum {
            present: as_u64(available),
        });
    };
    let declared_commit_crc = read_u32(trailer);
    let computed_commit_crc = crc32(&bytes[..trailer_start]);
    if declared_commit_crc != computed_commit_crc {
        return Err(SlotDamage::CommitChecksumMismatch {
            declared: declared_commit_crc,
            computed: computed_commit_crc,
        });
    }
    let sealed_len = trailer_start + SLOT_TRAILER_LEN;
    if bytes.len() > sealed_len {
        return Err(SlotDamage::TrailingBytes {
            extra: as_u64(bytes.len() - sealed_len),
        });
    }

    Ok(SealedImage {
        generation: read_u64(&header[5..13]),
        applied_index: LogIndex(read_u64(
            &header[HEADER_APPLIED_INDEX_OFFSET..HEADER_APPLIED_INDEX_OFFSET + 8],
        )),
        max_clients: read_u32(&header[21..25]),
        max_resources: read_u32(&header[25..29]),
        payload,
    })
}

/// Restores a sealed slot's payload through the model's own validating path.
pub(super) fn decode_image(
    slot: SlotIndex,
    sealed: &SealedImage<'_>,
    config: LockConfig,
) -> Result<DecodedImage, LockStoreError> {
    if sealed.max_clients != config.max_clients() || sealed.max_resources != config.max_resources()
    {
        return Err(LockStoreError::ConfigMismatch {
            slot,
            image_max_clients: sealed.max_clients,
            image_max_resources: sealed.max_resources,
            requested_max_clients: config.max_clients(),
            requested_max_resources: config.max_resources(),
        });
    }

    let (payload_index, snapshot) =
        decode_snapshot(sealed.payload).map_err(|source| LockStoreError::Image { slot, source })?;
    let payload_index = LogIndex(payload_index);
    if payload_index != sealed.applied_index {
        return Err(LockStoreError::AppliedIndexDisagreement {
            slot,
            header_index: sealed.applied_index,
            payload_index,
        });
    }

    // The model decides whether these parts describe a legal service. A slot
    // whose checksums verify still cannot produce a state that breaks the
    // expiry invariant or the held-token/high-water-mark equality.
    let service = LockService::from_snapshot(config, snapshot)
        .map_err(|source| LockStoreError::Snapshot { slot, source })?;
    Ok(DecodedImage {
        service,
        applied_index: sealed.applied_index,
    })
}

/// Encodes one whole slot image: header, payload, and the trailer that seals
/// them.
pub(super) fn encode_image(
    config: LockConfig,
    service: &LockService,
    applied_index: LogIndex,
    generation: u64,
) -> Result<Vec<u8>, LockStoreError> {
    let payload = encode_snapshot(applied_index.0, &service.snapshot()).map_err(|source| {
        LockStoreError::Image {
            slot: SlotIndex::Zero,
            source,
        }
    })?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| LockStoreError::ImageTooLarge {
        length: as_u64(payload.len()),
    })?;

    let mut image = Vec::with_capacity(SLOT_HEADER_LEN + payload.len() + SLOT_TRAILER_LEN);
    image.extend_from_slice(&SLOT_MAGIC);
    image.push(SLOT_FORMAT_VERSION);
    image.extend_from_slice(&generation.to_be_bytes());
    image.extend_from_slice(&applied_index.0.to_be_bytes());
    image.extend_from_slice(&config.max_clients().to_be_bytes());
    image.extend_from_slice(&config.max_resources().to_be_bytes());
    image.extend_from_slice(&payload_len.to_be_bytes());
    let header_checksum = crc32(&image);
    image.extend_from_slice(&header_checksum.to_be_bytes());

    image.extend_from_slice(&payload);
    let commit_checksum = crc32(&image);
    image.extend_from_slice(&commit_checksum.to_be_bytes());
    Ok(image)
}
