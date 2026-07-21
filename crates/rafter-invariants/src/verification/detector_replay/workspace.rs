//! Fresh descriptor-held writable state for authenticated replay execution.

use std::{
    error::Error,
    ffi::OsString,
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use crate::execution::filesystem::{ChildDirectory, HeldDirectory, OperationDeadline, TREE_LIMITS};

pub(super) struct ReplayWorkspace {
    _lock: fs::File,
    root: HeldDirectory,
    target: HeldDirectory,
    cargo_home: HeldDirectory,
    temporary: HeldDirectory,
    next_temporary: AtomicU64,
}

pub(super) struct ReplayBindings {
    pub(super) target: ChildDirectory,
    pub(super) cargo_home: ChildDirectory,
    pub(super) temporary: ChildDirectory,
}

pub(super) struct FixtureTemporary {
    directory: HeldDirectory,
}

impl ReplayWorkspace {
    pub(super) fn create(
        profile: &str,
        source_ref: &str,
        deadline: Instant,
    ) -> Result<Self, Box<dyn Error>> {
        validate_component(profile, "profile")?;
        let source = source_ref.get(..16).unwrap_or(source_ref);
        validate_component(source, "source reference")?;
        let parent_path = Path::new("target/rafter-invariants/verifier-replay");
        let parent = HeldDirectory::create_all(parent_path)?;
        let lock_path = parent
            .external_path()
            .join(format!(".{profile}-{source}.lock"));
        let lock = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        #[cfg(unix)]
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive).map_err(
            |error| {
                format!(
                    "another detector replay already owns {}: {error}",
                    lock_path.display()
                )
            },
        )?;
        parent.verify_path_binding()?;
        let path = parent_path.join(format!("{profile}-{source}"));
        let root = HeldDirectory::replace_tree(
            &path,
            TREE_LIMITS,
            OperationDeadline::at(deadline, "detector replay workspace cleanup"),
        )?;
        let target = root.create_dir_all(Path::new("target"))?;
        let cargo_home = root.create_dir_all(Path::new("cargo-home"))?;
        let temporary = root.create_dir_all(Path::new("tmp"))?;
        let workspace = Self {
            _lock: lock,
            root,
            target,
            cargo_home,
            temporary,
            next_temporary: AtomicU64::new(0),
        };
        workspace.verify()?;
        Ok(workspace)
    }

    pub(super) fn bind_for_child(&self) -> Result<ReplayBindings, Box<dyn Error>> {
        self.verify()?;
        Ok(ReplayBindings {
            target: self.target.bind_for_child()?,
            cargo_home: self.cargo_home.bind_for_child()?,
            temporary: self.temporary.bind_for_child()?,
        })
    }

    pub(super) fn target(&self) -> &HeldDirectory {
        &self.target
    }

    pub(super) fn create_fixture_temporary(&self) -> Result<FixtureTemporary, Box<dyn Error>> {
        self.verify()?;
        let sequence = self
            .next_temporary
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| {
                sequence.checked_add(1)
            })
            .map_err(|_| "detector replay fixture temporary sequence exhausted")?;
        let name = OsString::from(format!("fixture-{sequence:04}"));
        let directory = self.temporary.create_new_dir(&name)?;
        Ok(FixtureTemporary { directory })
    }

    pub(super) fn verify(&self) -> Result<(), Box<dyn Error>> {
        self.root.verify_path_binding()?;
        self.target.verify_path_binding()?;
        self.cargo_home.verify_path_binding()?;
        self.temporary.verify_path_binding()?;
        self.require_unconfigured_cargo_home()
    }

    fn require_unconfigured_cargo_home(&self) -> Result<(), Box<dyn Error>> {
        for path in ["config", "config.toml", "credentials", "credentials.toml"] {
            if self.cargo_home.path_exists(Path::new(path))? {
                return Err(
                    format!("private Cargo home acquired forbidden configuration {path}").into(),
                );
            }
        }
        Ok(())
    }
}

impl FixtureTemporary {
    pub(super) fn bind_for_child(&self) -> Result<ChildDirectory, Box<dyn Error>> {
        self.directory.verify_path_binding()?;
        self.directory.bind_for_child()
    }

    pub(super) fn verify(&self) -> Result<(), Box<dyn Error>> {
        self.directory.verify_path_binding()
    }
}

fn validate_component(value: &str, label: &str) -> Result<(), Box<dyn Error>> {
    if value.is_empty()
        || value.len() > 64
        || value
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')))
    {
        return Err(format!("detector replay {label} is not a safe path component").into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "workspace/tests.rs"]
mod tests;
