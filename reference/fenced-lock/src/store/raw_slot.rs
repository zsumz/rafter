//! Direct access to a slot file's bytes, for crash tests only.
//!
//! Everything here reaches past [`LockStore`] and reads or rewrites the artifact
//! the store owns. That is not a capability a durable application ever needs: an
//! application publishes through [`LockStore::commit`] and
//! [`LockStore::install`] and reads through [`LockStore::open`], and nothing
//! else in this crate calls into this module.
//!
//! It is a named public module rather than a hidden item because the honest
//! statement is that these functions *are* reachable, and hiding them from the
//! rendered documentation would not change that. A `#[doc(hidden)]` function
//! sitting beside the store's own API reads at the call site exactly like API; a
//! call that has to name `raw_slot` says what it is doing every time it appears,
//! and greps for one word. The crate's dependency boundary forbids gating this
//! behind a feature or an internal hook — a consumer manifest must resolve like
//! an external user's — so the guard is the name, this paragraph, and review.
//!
//! Nothing here validates anything. The store's own checks are what a forged
//! artifact is aimed at, so a caller is responsible for the bytes being the ones
//! it means to present.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

use rafter::LogIndex;

use super::{
    error::LockStoreError,
    format::{
        crc32, SlotIndex, HEADER_APPLIED_INDEX_OFFSET, HEADER_CHECKSUM_OFFSET, SLOT_HEADER_LEN,
        SLOT_TRAILER_LEN,
    },
    slot_file::slot_path,
};

// Imported for the intra-doc links the prose above carries.
#[allow(unused_imports)]
use super::LockStore;

/// Reads one slot's raw bytes.
///
/// Crash tests corrupt sealed images to prove the checksums are load
/// bearing, which needs the exact bytes the store wrote.
///
/// # Errors
///
/// Returns an error when the slot cannot be read.
pub fn read(directory: &Path, slot: SlotIndex) -> Result<Vec<u8>, LockStoreError> {
    let path = slot_path(directory, slot);
    let mut bytes = Vec::new();
    File::open(&path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|source| LockStoreError::Io {
            operation: "read a lock store slot",
            path,
            source,
        })?;
    Ok(bytes)
}

/// Overwrites one slot's raw bytes.
///
/// This is the corruption half of [`read`].
///
/// # Errors
///
/// Returns an error when the slot cannot be written.
pub fn write(directory: &Path, slot: SlotIndex, bytes: &[u8]) -> Result<(), LockStoreError> {
    let path = slot_path(directory, slot);
    OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .and_then(|mut file| file.write_all(bytes).and_then(|()| file.sync_all()))
        .map_err(|source| LockStoreError::Io {
            operation: "rewrite a lock store slot",
            path,
            source,
        })
}

/// Recomputes both checksums over a modified slot image.
///
/// Several header fields — the generation, the declared bounds, the applied
/// index — are protected by a checksum precisely so recovery can trust
/// them, which means flipping one of them cannot reach the check that reads
/// it: the checksum catches the flip first. Resealing is how a test
/// presents a *well-formed* image that nonetheless says something a correct
/// store would never write, so that the checks behind the checksums are
/// reachable at all.
///
/// # Panics
///
/// Panics when `image` is too short to hold a header and a trailer, which
/// is not an image any store wrote.
#[must_use]
pub fn reseal(mut image: Vec<u8>) -> Vec<u8> {
    assert!(
        image.len() >= SLOT_HEADER_LEN + SLOT_TRAILER_LEN,
        "a resealable image is at least a header and a trailer"
    );
    let header_checksum = crc32(&image[..HEADER_CHECKSUM_OFFSET]);
    image[HEADER_CHECKSUM_OFFSET..SLOT_HEADER_LEN].copy_from_slice(&header_checksum.to_be_bytes());
    let sealed = image.len() - SLOT_TRAILER_LEN;
    let commit_checksum = crc32(&image[..sealed]);
    image[sealed..].copy_from_slice(&commit_checksum.to_be_bytes());
    image
}

/// Overwrites the applied Raft index a slot header declares, leaving the
/// payload's own copy alone.
///
/// The two copies exist so recovery can order and report without decoding,
/// and they are cross-checked because a disagreement means the artifact is
/// not what it says it is. A correct store cannot produce that disagreement
/// — both copies come from one argument — so this is the only way to reach
/// the check.
///
/// # Panics
///
/// Panics when `image` is too short to hold a header and a trailer.
#[must_use]
pub fn overwrite_header_applied_index(mut image: Vec<u8>, applied_index: LogIndex) -> Vec<u8> {
    assert!(
        image.len() >= SLOT_HEADER_LEN + SLOT_TRAILER_LEN,
        "a rewritable image is at least a header and a trailer"
    );
    image[HEADER_APPLIED_INDEX_OFFSET..HEADER_APPLIED_INDEX_OFFSET + 8]
        .copy_from_slice(&applied_index.0.to_be_bytes());
    reseal(image)
}
