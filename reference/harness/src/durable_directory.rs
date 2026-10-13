//! Directory creation for reference consumers that promise durable state.

use std::{fs, io, path::Path};

/// Creates `directory`, synchronizing each new entry before creating its children.
///
/// The nearest existing ancestor must have durably provisioned ancestors of its
/// own. Its parent is synchronized first: an earlier failed attempt may have
/// created that one directory but failed to persist its entry. This ordering
/// makes retry safe after this helper's own creation or synchronization errors.
/// Existing directory trees created by other code still need that code's
/// durability guarantee; this cannot discover arbitrary unsynchronized ancestors.
///
/// No files are removed, and this does not acquire ownership of the directory.
/// Existing ancestors must not be removed or redirected while stores open.
///
/// # Errors
///
/// Returns an I/O error if a component is not a directory, creation fails, or a
/// parent cannot be synchronized. On error, no child is created beneath the
/// entry whose synchronization failed. Directory synchronization must be
/// supported by the host filesystem.
pub fn create_durable_directory(directory: &Path) -> io::Result<()> {
    provision(directory, |parent| fs::File::open(parent)?.sync_all())
}

fn provision(directory: &Path, mut sync: impl FnMut(&Path) -> io::Result<()>) -> io::Result<()> {
    let directory = if directory.is_absolute() {
        directory.to_path_buf()
    } else {
        std::env::current_dir()?.join(directory)
    };
    let mut missing = Vec::new();
    let mut ancestor = directory;
    loop {
        match fs::metadata(&ancestor) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => return Err(io::ErrorKind::NotADirectory.into()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let parent = ancestor.parent().ok_or(error)?.to_path_buf();
                missing.push(ancestor);
                ancestor = parent;
            }
            Err(error) => return Err(error),
        }
    }
    sync(ancestor.parent().unwrap_or(&ancestor))?;
    for directory in missing.into_iter().rev() {
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if !fs::metadata(&directory)?.is_dir() {
                    return Err(io::ErrorKind::NotADirectory.into());
                }
            }
            Err(error) => return Err(error),
        }
        sync(
            directory
                .parent()
                .expect("a missing directory had a parent"),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
