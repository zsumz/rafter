//! Obsolete-generation removal happens only after one authority validates.

use super::{format::manifest_path, Authority, Failure};
use crate::durable_fs::sync_parent_directory;
use std::{ffi::OsStr, fs, path::Path};

pub(crate) fn cleanup(
    directory: &Path,
    authority: &Authority,
    inject: bool,
) -> Result<(), Failure> {
    #[cfg(not(test))]
    let _ = inject;
    let mut removed = false;
    for entry in fs::read_dir(directory).map_err(|source| Failure {
        operation: "list obsolete WAL generations",
        source,
    })? {
        let entry = entry.map_err(|source| Failure {
            operation: "inspect obsolete WAL generation",
            source,
        })?;
        if !entry
            .file_type()
            .map_err(|source| Failure {
                operation: "inspect obsolete WAL generation",
                source,
            })?
            .is_file()
        {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !is_obsolete_managed_name(&name, authority) {
            continue;
        }
        fs::remove_file(entry.path()).map_err(|source| Failure {
            operation: "delete obsolete WAL generation",
            source,
        })?;
        removed = true;
        #[cfg(test)]
        if inject {
            crate::storage_failpoint_test::check(
                crate::storage_failpoint_test::DurabilityPoint::WalCleanupAfterDelete,
            )
            .map_err(|source| Failure {
                operation: "delete obsolete WAL generation",
                source,
            })?;
        }
    }
    if removed {
        sync_parent_directory(&manifest_path(directory)).map_err(|source| Failure {
            operation: "sync obsolete WAL generation deletion",
            source,
        })?;
    }
    Ok(())
}

fn is_obsolete_managed_name(name: &str, authority: &Authority) -> bool {
    match authority {
        Authority::Legacy if name == "hard-state" => false,
        Authority::Generation(_) if name == "hard-state" => true,
        _ if name.starts_with(".raft-wal-current-") && has_extension(name, "tmp") => true,
        _ if name.starts_with("raft-wal-checkpoint-") && has_extension(name, "rfwc") => {
            !selected_generation_name(name, authority, "raft-wal-checkpoint-", ".rfwc")
        }
        _ if name.starts_with("raft-wal-segment-") && has_extension(name, "rfwb") => {
            !selected_generation_name(name, authority, "raft-wal-segment-", ".rfwb")
        }
        _ => false,
    }
}

fn has_extension(name: &str, extension: &str) -> bool {
    Path::new(name).extension() == Some(OsStr::new(extension))
}

fn selected_generation_name(name: &str, authority: &Authority, prefix: &str, suffix: &str) -> bool {
    let Authority::Generation(generation) = authority else {
        return false;
    };
    name == format!("{prefix}{generation:020}{suffix}")
}
