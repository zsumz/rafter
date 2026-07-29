use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A uniquely named directory removed when its owner drops.
#[derive(Debug)]
pub struct ScratchSpace {
    inner: Arc<ScratchDirectory>,
}

#[derive(Debug)]
struct ScratchDirectory {
    path: PathBuf,
}

impl ScratchSpace {
    /// Creates an empty directory under the system temporary directory.
    ///
    /// # Errors
    ///
    /// Returns an error when a prior directory cannot be removed or the empty
    /// directory cannot be created.
    pub fn create(namespace: &str, label: &str) -> io::Result<Self> {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("{namespace}.{}.{label}.{id}", std::process::id()));
        match fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::create_dir_all(&path)?;
        Ok(Self {
            inner: Arc::new(ScratchDirectory { path }),
        })
    }

    /// Returns the directory path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub(super) fn lease(&self) -> ScratchLease {
        ScratchLease {
            _inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.path));
    }
}

/// Keeps a scratch directory alive for another owner's lifetime.
#[derive(Debug)]
pub(super) struct ScratchLease {
    _inner: Arc<ScratchDirectory>,
}
