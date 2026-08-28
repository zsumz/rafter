//! Descriptor-confined content-addressed verifier artifact publication.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    error::Error,
    ffi::OsStr,
    fmt, fs,
    path::Path,
    sync::atomic::AtomicU64,
    time::Instant,
};

use sha2::{Digest, Sha256};

use crate::{
    evidence::{limits::MAX_ARTIFACT_BYTES, ArtifactRef},
    execution::filesystem::{HeldDirectory, HeldFile, OperationDeadline},
};

mod inventory;

use inventory::{
    harden_read_only, invocation_name, publication_root, require_exact_inventory,
    require_read_only, validate_archive_budget,
};

static INVOCATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const DEFAULT_PUBLICATION_ROOT: &str = "target/rafter-invariants/verifier-evidence";
const PUBLICATION_ROOT_ENV: &str = "RAFTER_INVARIANT_VERIFIER_EVIDENCE_DIR";
const MANIFEST_KIND: &str = "verifier-artifact-manifest";

pub(super) struct ReplayArtifactPublisher {
    lock: PublicationLock,
    root: HeldDirectory,
    path: std::path::PathBuf,
    published: RefCell<BTreeMap<String, PublishedArtifact>>,
    deadline: OperationDeadline,
}

struct PublishedArtifact {
    reference: ArtifactRef,
    file: HeldFile,
}

pub(in crate::verification) struct ReplayArtifactGuard {
    lock: PublicationLock,
    root: HeldDirectory,
    published: BTreeMap<String, PublishedArtifact>,
    deadline: OperationDeadline,
}

struct PublicationLock {
    file: fs::File,
}

impl PublicationLock {
    fn verify_descriptor(&self) -> Result<(), Box<dyn Error>> {
        if !self.file.metadata()?.is_file() {
            return Err("verifier publication lock is not a regular file".into());
        }
        Ok(())
    }
}

impl Drop for PublicationLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        let _ = rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock);
    }
}

impl fmt::Debug for ReplayArtifactGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplayArtifactGuard")
            .field("artifacts", &self.published.len())
            .finish_non_exhaustive()
    }
}

impl ReplayArtifactPublisher {
    pub(super) fn create(
        profile: &str,
        source_ref: &str,
        publication_deadline: Instant,
    ) -> Result<Self, Box<dyn Error>> {
        let deadline = OperationDeadline::at(
            publication_deadline,
            "publish verifier replay artifact inventory",
        );
        deadline.check()?;
        let identity = format!("{:x}", Sha256::digest(format!("{profile}\0{source_ref}")));
        let parent_path = publication_root()?;
        let parent = HeldDirectory::create_all(&parent_path)?;
        deadline.check()?;
        let lock_path = parent
            .external_path()
            .join(format!(".run-{}.lock", &identity[..16]));
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        #[cfg(unix)]
        rustix::fs::flock(
            &lock_file,
            rustix::fs::FlockOperation::NonBlockingLockExclusive,
        )
        .map_err(|error| {
            format!(
                "another verifier artifact publisher already owns {}: {error}",
                lock_path.display()
            )
        })?;
        let lock = PublicationLock { file: lock_file };
        parent.verify_path_binding()?;
        deadline.check()?;
        let run_name = invocation_name(&identity)?;
        let root = parent.create_new_dir(OsStr::new(&run_name))?;
        let path = parent_path.join(run_name);
        deadline.check()?;
        Ok(Self {
            lock,
            root,
            path,
            published: RefCell::new(BTreeMap::new()),
            deadline,
        })
    }

    pub(super) fn capture(&self, kind: &str, bytes: &[u8]) -> Result<ArtifactRef, Box<dyn Error>> {
        self.deadline.check()?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_ARTIFACT_BYTES {
            return Err(format!(
                "{kind} verifier artifact must contain 1..={MAX_ARTIFACT_BYTES} bytes, found {}",
                bytes.len()
            )
            .into());
        }
        let mut published = self.published.borrow_mut();
        if kind != MANIFEST_KIND
            && published
                .values()
                .any(|artifact| artifact.reference.kind == MANIFEST_KIND)
        {
            return Err("verifier artifact inventory is already manifested".into());
        }
        let digest = format!("{:x}", Sha256::digest(bytes));
        let name = format!("{kind}-{digest}");
        if let Some(existing) = published.get(&name) {
            existing.revalidate(self.deadline)?;
            return Ok(existing.reference.clone());
        }
        let published_bytes = published.values().try_fold(0_usize, |total, artifact| {
            let artifact_bytes = usize::try_from(artifact.reference.size_bytes)
                .map_err(|_| "verifier artifact size exceeds usize")?;
            total
                .checked_add(artifact_bytes)
                .ok_or("verifier artifact byte count overflow")
        })?;
        validate_archive_budget(published.len(), published_bytes, bytes.len())?;
        self.root.write_atomic(Path::new(&name), bytes)?;
        self.deadline.check()?;
        let file = self.root.hold_file(Path::new(&name))?;
        let persisted = file.read_bounded(self.deadline, MAX_ARTIFACT_BYTES)?;
        if persisted != bytes {
            return Err(format!("verifier artifact {name} changed during publication").into());
        }
        let reference = ArtifactRef {
            kind: kind.to_owned(),
            path: self.path.join(&name).to_string_lossy().into_owned(),
            sha256: digest,
            size_bytes: persisted.len() as u64,
        };
        published.insert(
            name,
            PublishedArtifact {
                reference: reference.clone(),
                file,
            },
        );
        Ok(reference)
    }

    pub(super) fn publish_manifest(&self) -> Result<ArtifactRef, Box<dyn Error>> {
        self.deadline.check()?;
        let bytes = {
            let published = self.published.borrow();
            if published
                .values()
                .any(|artifact| artifact.reference.kind == MANIFEST_KIND)
            {
                return Err("verifier artifact manifest was published more than once".into());
            }
            let mut bytes = Vec::new();
            for (name, artifact) in published.iter() {
                use std::io::Write as _;
                writeln!(bytes, "{}  {name}", artifact.reference.sha256)?;
            }
            bytes
        };
        self.capture(MANIFEST_KIND, &bytes)
    }

    pub(super) fn seal(self) -> Result<ReplayArtifactGuard, Box<dyn Error>> {
        self.deadline.check()?;
        if self
            .published
            .borrow()
            .values()
            .filter(|artifact| artifact.reference.kind == MANIFEST_KIND)
            .count()
            != 1
        {
            return Err("verifier artifact inventory has no unique manifest".into());
        }
        for artifact in self.published.borrow().values() {
            harden_read_only(&artifact.file.external_path(), self.deadline)?;
        }
        require_exact_inventory(&self.root, &self.published.borrow(), self.deadline)?;
        harden_read_only(&self.root.external_path(), self.deadline)?;
        let guard = ReplayArtifactGuard {
            lock: self.lock,
            root: self.root,
            published: self.published.into_inner(),
            deadline: self.deadline,
        };
        guard.revalidate()?;
        Ok(guard)
    }
}

impl ReplayArtifactGuard {
    pub(in crate::verification) fn references(&self) -> BTreeSet<ArtifactRef> {
        self.published
            .values()
            .map(|artifact| artifact.reference.clone())
            .collect()
    }

    pub(in crate::verification) fn revalidate(&self) -> Result<(), Box<dyn Error>> {
        self.deadline.check()?;
        self.lock.verify_descriptor()?;
        self.root.verify_path_binding()?;
        require_read_only(&self.root.external_path())?;
        require_exact_inventory(&self.root, &self.published, self.deadline)?;
        for artifact in self.published.values() {
            artifact.revalidate(self.deadline)?;
        }
        self.deadline.check()?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "publisher_tests.rs"]
mod tests;
