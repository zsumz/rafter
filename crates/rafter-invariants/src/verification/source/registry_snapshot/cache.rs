//! No-follow acquisition of lock-matching archives from the ambient Cargo cache.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};

use super::lock::LockedPackage;

mod authenticate;

use authenticate::authenticate_candidate;

const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct AuthenticatedArchive {
    pub(super) bytes: Vec<u8>,
    pub(super) digest: [u8; 32],
}

#[derive(Debug)]
pub(super) struct ArchiveInventory {
    candidates: BTreeMap<String, Vec<PathBuf>>,
}

impl ArchiveInventory {
    #[cfg(test)]
    pub(super) fn fixture(candidates: BTreeMap<String, Vec<PathBuf>>) -> Self {
        Self { candidates }
    }

    pub(super) fn discover(deadline: Instant, maximum_entries: u64) -> Result<Self, String> {
        let home = cargo_home()?;
        Self::discover_from(&home, deadline, maximum_entries)
    }

    fn discover_from(home: &Path, deadline: Instant, maximum_entries: u64) -> Result<Self, String> {
        let mut budget = DiscoveryBudget::new(deadline, maximum_entries)?;
        let cache = home.join("registry/cache");
        require_directory(&cache, "Cargo registry cache")?;
        let mut candidates = BTreeMap::<String, Vec<PathBuf>>::new();
        for registry in read_directory(&cache)? {
            budget.visit()?;
            let registry =
                registry.map_err(|error| format!("read registry cache entry: {error}"))?;
            require_directory(&registry.path(), "Cargo registry cache namespace")?;
            for entry in read_directory(&registry.path())? {
                budget.visit()?;
                let entry =
                    entry.map_err(|error| format!("read registry archive entry: {error}"))?;
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| "Cargo registry archive name is not UTF-8".to_owned())?;
                if Path::new(&name).extension() != Some(std::ffi::OsStr::new("crate")) {
                    continue;
                }
                candidates.entry(name).or_default().push(entry.path());
            }
        }
        budget.check()?;
        Ok(Self { candidates })
    }

    #[cfg(test)]
    pub(super) fn discover_fixture(home: &Path) -> Result<Self, String> {
        Self::discover_from(
            home,
            Instant::now() + std::time::Duration::from_secs(30),
            4096,
        )
    }

    #[cfg(test)]
    pub(super) fn discover_fixture_bounded(
        home: &Path,
        deadline: Instant,
        maximum_entries: u64,
    ) -> Result<Self, String> {
        Self::discover_from(home, deadline, maximum_entries)
    }

    pub(super) fn acquire(
        &self,
        package: &LockedPackage,
        deadline: Instant,
    ) -> Result<AuthenticatedArchive, String> {
        require_time(deadline)?;
        let name = package.archive_name();
        let mut candidates = self.candidates.get(&name).cloned().unwrap_or_default();
        candidates.sort();
        candidates.dedup();
        if candidates.is_empty() {
            return Err(format!(
                "registry archive {name} has {} cache candidates; at least one is required",
                candidates.len()
            ));
        }
        let mut authenticated = None;
        for path in &candidates {
            let candidate = authenticate_candidate(path, package, deadline)?;
            if authenticated
                .as_ref()
                .is_some_and(|first: &AuthenticatedArchive| {
                    first.digest != candidate.digest || first.bytes != candidate.bytes
                })
            {
                return Err(format!(
                    "registry archive {name} has conflicting authenticated cache candidates"
                ));
            }
            authenticated.get_or_insert(candidate);
        }
        authenticated
            .ok_or_else(|| format!("registry archive {name} has no authenticated cache candidate"))
    }
}

struct DiscoveryBudget {
    deadline: Instant,
    maximum_entries: u64,
    entries: u64,
}

impl DiscoveryBudget {
    fn new(deadline: Instant, maximum_entries: u64) -> Result<Self, String> {
        if maximum_entries == 0 {
            return Err(
                "Cargo registry cache discovery requires a positive entry limit".to_owned(),
            );
        }
        let budget = Self {
            deadline,
            maximum_entries,
            entries: 0,
        };
        budget.check()?;
        Ok(budget)
    }

    fn visit(&mut self) -> Result<(), String> {
        self.check()?;
        self.entries = self
            .entries
            .checked_add(1)
            .ok_or_else(|| "Cargo registry cache entry count overflow".to_owned())?;
        if self.entries > self.maximum_entries {
            return Err(format!(
                "Cargo registry cache exceeds its discovery entry limit of {}",
                self.maximum_entries
            ));
        }
        Ok(())
    }

    fn check(&self) -> Result<(), String> {
        require_time(self.deadline)
    }
}

fn require_time(deadline: Instant) -> Result<(), String> {
    if Instant::now() >= deadline {
        Err("Cargo registry cache discovery deadline expired".to_owned())
    } else {
        Ok(())
    }
}

fn cargo_home() -> Result<PathBuf, String> {
    let value = env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")))
        .ok_or_else(|| "registry replay requires CARGO_HOME or HOME".to_owned())?;
    let path = if value.is_absolute() {
        value
    } else {
        env::current_dir()
            .map_err(|error| format!("resolve relative CARGO_HOME: {error}"))?
            .join(value)
    };
    fs::canonicalize(&path)
        .map_err(|error| format!("canonicalize Cargo home {}: {error}", path.display()))
}

fn require_directory(path: &Path, label: &str) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect {label} {}: {error}", path.display()))?;
    if !metadata.file_type().is_dir() {
        return Err(format!(
            "{label} is not a direct directory: {}",
            path.display()
        ));
    }
    Ok(())
}

fn read_directory(path: &Path) -> Result<fs::ReadDir, String> {
    fs::read_dir(path).map_err(|error| format!("read directory {}: {error}", path.display()))
}
