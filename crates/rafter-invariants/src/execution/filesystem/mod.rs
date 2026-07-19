//! Descriptor-confined filesystem execution with bounded traversal and durable publication.

mod cleanup;
mod file_io;
mod handles;
mod model;
mod paths;
mod publication;
mod sync;
mod traversal;

#[cfg(test)]
mod tests;

pub(crate) use cleanup::remove_file;
pub(crate) use file_io::{create_new_file, hold_file, read_file};
pub(crate) use model::{
    ChildDirectory, EntryKind, FileIdentity, HeldDirectory, HeldFile, OperationDeadline,
    TreeLimits, TREE_LIMITS,
};
pub(crate) use publication::path_exists;

#[cfg(all(test, any(target_os = "android", target_os = "linux")))]
pub(super) use sync::{complete_directory_sync, complete_filesystem_sync};
