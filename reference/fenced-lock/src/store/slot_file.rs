//! The two files the store owns, and nothing that interprets them.
//!
//! Establishing the pair, creating one with its mark, reading one back, and
//! making the directory entry durable. The rule that decides when creating is
//! allowed at all — narrower than "one slot is missing" — is on
//! [`establish_slot_files`].

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use super::{
    damage::SlotState,
    error::LockStoreError,
    format::{SlotIndex, CREATION_MARK},
    image::verify_slot,
};

/// Brings both slot files into existence, or refuses, and says whether this
/// open created them.
///
/// Creating a store is not the same act as opening one, and the difference is
/// invisible from inside a directory that has lost a file. So creation is
/// allowed in exactly two shapes, and both are provable:
///
/// - **Neither slot exists.** There is no store here, and making one loses
///   nothing.
/// - **Slot zero exists, carries only its creation mark, and slot one does
///   not exist.** A creation was interrupted between its two files, and
///   finishing it loses nothing.
///
/// The second shape is narrower than "one slot is missing and the other has
/// never been published to", and the difference is the whole point. Creation
/// writes slot zero first and publications write slot zero first, so slot zero
/// carrying only its creation mark proves no publication ever committed
/// *anywhere*. The mirror statement is false: slot one carrying only its
/// creation mark proves only that generation two never landed, and slot zero —
/// the one that is missing — is exactly where generation one would have been.
/// Accepting that shape would recreate the store's first committed generation
/// as an empty file and report a clean opening.
///
/// Anything else is [`LockStoreError::MissingSlot`]. A slot that should exist
/// and does not is unreadable rather than absent, and re-creating it would open
/// the store one generation back with nothing reported.
pub(super) fn establish_slot_files(directory: &Path) -> Result<bool, LockStoreError> {
    let present = [
        slot_path(directory, SlotIndex::Zero).exists(),
        slot_path(directory, SlotIndex::One).exists(),
    ];

    match present {
        [true, true] => return Ok(false),
        [false, false] => {}
        // Slot zero survives, so the interrupted-creation reading is available
        // if its bytes support it.
        [true, false] => {
            let bytes = read_slot(directory, SlotIndex::Zero)?;
            if !matches!(verify_slot(&bytes), Ok(None)) {
                return Err(LockStoreError::MissingSlot {
                    slot: SlotIndex::One,
                    other: slot_state(&bytes),
                });
            }
        }
        // Slot zero is the one that is gone, and it is the slot the first
        // publication writes. Whatever slot one holds, it cannot speak for it.
        [false, true] => {
            let bytes = read_slot(directory, SlotIndex::One)?;
            return Err(LockStoreError::MissingSlot {
                slot: SlotIndex::Zero,
                other: slot_state(&bytes),
            });
        }
    }

    // Slot zero is created first, so the only state an interrupted creation can
    // leave is the one accepted above. Both entries are made durable with one
    // directory sync, and every later publication is a pure content rewrite, so
    // no publication ever has a directory-entry crash window.
    for slot in [SlotIndex::Zero, SlotIndex::One] {
        if !present[slot.position()] {
            create_slot(directory, slot)?;
        }
    }
    sync_directory(directory)?;
    Ok(true)
}

/// Creates one slot file carrying its creation mark.
///
/// The mark is what separates a created slot from an emptied one. A file of
/// zero bytes is not a state this store leaves behind at any point in its life,
/// so meeting one is damage rather than the ordinary state of a store that has
/// never committed.
fn create_slot(directory: &Path, slot: SlotIndex) -> Result<(), LockStoreError> {
    let path = slot_path(directory, slot);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(|source| LockStoreError::Io {
            operation: "create a lock store slot",
            path: path.clone(),
            source,
        })?;
    file.write_all(&CREATION_MARK)
        .and_then(|()| file.sync_data())
        .map_err(|source| LockStoreError::Io {
            operation: "mark a created lock store slot",
            path,
            source,
        })
}

pub(super) fn slot_state(bytes: &[u8]) -> SlotState {
    match verify_slot(bytes) {
        Ok(None) => SlotState::Empty,
        Ok(Some(sealed)) => SlotState::Intact {
            generation: sealed.generation,
            applied_index: sealed.applied_index,
        },
        Err(damage) => SlotState::Damaged(damage),
    }
}

pub(super) fn read_slot(directory: &Path, slot: SlotIndex) -> Result<Vec<u8>, LockStoreError> {
    let path = slot_path(directory, slot);
    fs::read(&path).map_err(|source| LockStoreError::Io {
        operation: "read a lock store slot",
        path,
        source,
    })
}

/// Makes a directory's entries durable.
pub(super) fn sync_directory(directory: &Path) -> Result<(), LockStoreError> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|source| LockStoreError::Io {
            operation: "sync the lock store directory",
            path: directory.to_path_buf(),
            source,
        })
}

pub(super) fn slot_path(directory: &Path, slot: SlotIndex) -> PathBuf {
    directory.join(slot.file_name())
}
