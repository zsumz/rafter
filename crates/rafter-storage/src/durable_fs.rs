use std::{
    collections::BTreeSet,
    fs::File,
    io,
    path::{Path, PathBuf},
};

pub(crate) fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = parent_directory(path);
    sync_directory(&parent)
}

fn parent_directory(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn sync_directory(path: &Path) -> io::Result<()> {
    let directory = File::open(path)?;
    directory.sync_all()
}

#[derive(Debug, Default)]
pub(crate) struct ParentDirectorySyncBatch {
    parents: BTreeSet<PathBuf>,
}

impl ParentDirectorySyncBatch {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record_parent_of(&mut self, path: &Path) {
        self.parents.insert(parent_directory(path));
    }

    pub(crate) fn flush(&mut self) -> io::Result<()> {
        for parent in &self.parents {
            sync_directory(parent)?;
        }
        self.parents.clear();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn pending_count(&self) -> usize {
        self.parents.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        sync::atomic::{AtomicU64, Ordering},
    };

    static TEST_FILE_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn sync_parent_directory_accepts_existing_parent() {
        let path = std::env::temp_dir().join(format!(
            "rafter-storage-parent-sync-{}-{}",
            std::process::id(),
            TEST_FILE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .expect("test file creates");
        file.write_all(b"durable").expect("test file writes");
        file.sync_data().expect("test file syncs");

        sync_parent_directory(&path).expect("parent directory syncs");

        fs::remove_file(path).expect("test file removes");
    }

    #[test]
    fn relative_file_parent_defaults_to_current_directory() {
        assert_eq!(
            parent_directory(Path::new("hard-state")),
            PathBuf::from(".")
        );
    }

    #[test]
    fn parent_directory_sync_batch_deduplicates_common_parents() {
        let mut batch = ParentDirectorySyncBatch::new();

        batch.record_parent_of(Path::new("/tmp/group-1/log"));
        batch.record_parent_of(Path::new("/tmp/group-1/snapshots"));
        batch.record_parent_of(Path::new("/tmp/group-2/log"));

        assert_eq!(batch.pending_count(), 2);
    }
}
