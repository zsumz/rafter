//! Direct access to the journal's bytes, for crash tests only.
//!
//! Everything here reaches past [`LedgerStore`] and reads or rewrites the file
//! the store owns. That is not a capability a durable application ever needs: an
//! application commits through [`LedgerStore::commit`] and
//! [`LedgerStore::replace`] and reads through [`LedgerStore::open`], and nothing
//! else in this crate calls into this module.
//!
//! It is a named public module rather than a hidden item because the honest
//! statement is that these functions *are* reachable, and keeping them out of
//! the rendered documentation would not change that. A `#[doc(hidden)]`
//! function sitting beside the store's own API reads at the call site exactly
//! like API; a call that has to name `raw_journal` says what it is doing every
//! time it appears, and greps for one word. The crate's dependency boundary
//! forbids gating this behind a feature or an internal hook — a consumer
//! manifest must resolve like an external user's — so the guard is the name,
//! this paragraph, and review.
//!
//! Nothing here validates anything. The store's own checks are what a corrupted
//! journal is aimed at, so a caller is responsible for the bytes being the ones
//! it means to present.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

use super::{error::LedgerStoreError, format::JOURNAL_FILE_NAME};

// Imported for the intra-doc links the prose above carries.
#[allow(unused_imports)]
use super::LedgerStore;

/// Reads the journal's raw bytes.
///
/// Crash tests corrupt committed frames to prove the checksums are load
/// bearing, which needs the exact bytes the store wrote.
///
/// # Errors
///
/// Returns an error when the journal cannot be read.
pub fn read(directory: &Path) -> Result<Vec<u8>, LedgerStoreError> {
    let path = directory.join(JOURNAL_FILE_NAME);
    let mut bytes = Vec::new();
    File::open(&path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|source| LedgerStoreError::Io {
            operation: "read the ledger journal",
            path,
            source,
        })?;
    Ok(bytes)
}

/// Overwrites the journal's raw bytes.
///
/// This is the corruption half of [`read`].
///
/// # Errors
///
/// Returns an error when the journal cannot be written.
pub fn write(directory: &Path, bytes: &[u8]) -> Result<(), LedgerStoreError> {
    let path = directory.join(JOURNAL_FILE_NAME);
    OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .and_then(|mut file| file.write_all(bytes).and_then(|()| file.sync_all()))
        .map_err(|source| LedgerStoreError::Io {
            operation: "rewrite the ledger journal",
            path,
            source,
        })
}
