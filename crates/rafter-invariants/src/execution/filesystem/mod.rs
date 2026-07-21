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

pub(crate) use file_io::{hold_file, read_file_bounded};
pub(crate) use model::{
    ChildDirectory, EntryKind, FileIdentity, HeldDirectory, HeldFile, OperationDeadline,
    TreeLimits, TREE_LIMITS,
};
#[cfg(all(test, any(target_os = "android", target_os = "linux")))]
use sync::{complete_directory_sync, complete_filesystem_sync};
