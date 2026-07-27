//! The journal file itself, and the one name this store stages beside it.
//!
//! Creating the journal through a rename, shortening it to its committed
//! length, sweeping an abandoned staging file, and making the directory entry
//! durable. Nothing here interprets a frame.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use crate::LedgerConfig;

use super::{
    error::LedgerStoreError,
    format::{JOURNAL_FILE_NAME, STAGED_FILE_NAME},
    frame::encode_header,
};

// Imported for the intra-doc links the prose below carries.
#[allow(unused_imports)]
use super::LedgerStore;

/// Creates the journal by staging its header and renaming it into place.
///
/// Renaming rather than creating-then-writing is what closes the crash window
/// between the two. A journal that existed but held no header could never be
/// completed by a later open — the file exists, so creation would never run
/// again — and the directory would be bricked by a crash inside a
/// two-statement function. With a rename, an interrupted creation leaves only a
/// staging file, the next open sweeps it, and creation runs properly.
pub(super) fn create_journal(
    directory: &Path,
    config: LedgerConfig,
) -> Result<(), LedgerStoreError> {
    let staged_path = staged_path(directory);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&staged_path)
        .map_err(|source| LedgerStoreError::Io {
            operation: "stage the ledger journal header",
            path: staged_path.clone(),
            source,
        })?;
    file.write_all(&encode_header(config))
        .and_then(|()| file.sync_data())
        .map_err(|source| LedgerStoreError::Io {
            operation: "write the ledger journal header",
            path: staged_path.clone(),
            source,
        })?;
    drop(file);

    let journal_path = directory.join(JOURNAL_FILE_NAME);
    fs::rename(&staged_path, &journal_path).map_err(|source| LedgerStoreError::Io {
        operation: "publish the staged ledger journal",
        path: journal_path,
        source,
    })?;
    sync_directory(directory)
}

/// Discards an uncommitted tail and makes the shortened file durable.
pub(super) fn truncate_journal(
    journal_path: &Path,
    committed_len: usize,
) -> Result<(), LedgerStoreError> {
    let file = OpenOptions::new()
        .write(true)
        .open(journal_path)
        .map_err(|source| LedgerStoreError::Io {
            operation: "open the ledger journal for truncation",
            path: journal_path.to_path_buf(),
            source,
        })?;
    file.set_len(committed_len as u64)
        .and_then(|()| file.sync_all())
        .map_err(|source| LedgerStoreError::Io {
            operation: "truncate the ledger journal's uncommitted tail",
            path: journal_path.to_path_buf(),
            source,
        })
}

/// Makes a directory's entries durable.
pub(super) fn sync_directory(directory: &Path) -> Result<(), LedgerStoreError> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|source| LedgerStoreError::Io {
            operation: "sync the ledger store directory",
            path: directory.to_path_buf(),
            source,
        })
}

/// Staging path for a rewrite or a creation.
pub(super) fn staged_path(directory: &Path) -> PathBuf {
    directory.join(STAGED_FILE_NAME)
}

/// Removes the one file this store stages, returning how large it was.
///
/// The rule is exactly one name — [`STAGED_FILE_NAME`] — and not a prefix
/// match. An earlier shape of this sweep removed anything beside the journal
/// whose name began with the journal's name and a dot, on the reasoning that a
/// staging file is always somebody else's abandoned work and the widest rule
/// leaks the least. That reasoning had the direction of its own proof backwards
/// in the same way the tail classifier did: it proved every staging file this
/// store writes matches the prefix, and then used matching the prefix as proof
/// that a file was one. It is not. The process tells an operator to run a
/// repair, the obvious first move is to copy the journal aside, and the obvious
/// name for the copy begins with the journal's name and a dot. Opening the
/// store deleted the backup the store's own instructions invited.
///
/// So the sweep removes only a name this store could have written itself, and
/// nothing else in the directory is touched. Leaking is the smaller failure:
/// residue this store did not create is somebody's evidence.
///
/// Removing this file is safe when there is no other writer, and this store
/// does not establish that — it inherits it, or it does not get it at all.
///
/// Where it holds: `ledger-node` takes `rafter-storage`'s operating-system lock
/// over its Raft store directory before it opens this journal and holds it for
/// the process's life, so a second process is refused before it reaches this
/// directory. Under that composition a staging file present at this point was
/// abandoned by an incarnation that is gone, and deleting it loses nothing.
///
/// Where it does not: the lock is taken by a different crate, over a different
/// directory, by a binary this store does not require. Anything that embeds
/// [`LedgerStore`] directly — including every test in this crate — gets none of
/// it, and two live stores over one directory would sweep each other's staging
/// files with nothing raised. That is the embedder's obligation, stated as an
/// obligation rather than as a fact about this function.
pub(super) fn sweep_staged_file(directory: &Path) -> Result<Option<u64>, LedgerStoreError> {
    let path = staged_path(directory);
    let length = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(LedgerStoreError::Io {
                operation: "inspect an abandoned staging file",
                path,
                source,
            })
        }
    };
    match fs::remove_file(&path) {
        Ok(()) => Ok(Some(length)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(LedgerStoreError::Io {
            operation: "remove an abandoned staging file",
            path,
            source,
        }),
    }
}
