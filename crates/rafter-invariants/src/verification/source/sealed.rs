//! Immutable verifier-owned trees with identity, digest, inventory, and mode checks.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::execution::filesystem::OperationDeadline;

#[derive(Clone, Debug)]
pub(super) struct FilePlan {
    pub(super) digest: [u8; 32],
    #[cfg(unix)]
    pub(super) executable: bool,
}

#[derive(Debug)]
struct SealedFile {
    identity: FileIdentity,
    digest: [u8; 32],
    size_bytes: u64,
    #[cfg(unix)]
    executable: bool,
}

#[derive(Debug)]
pub(super) struct SealedTree {
    context: &'static str,
    root: PathBuf,
    root_identity: FileIdentity,
    directories: BTreeSet<PathBuf>,
    files: BTreeMap<PathBuf, SealedFile>,
    maximum_nodes: u64,
}

impl SealedTree {
    pub(super) fn capture(
        context: &'static str,
        root: &Path,
        plans: BTreeMap<PathBuf, FilePlan>,
    ) -> Result<Self, String> {
        Self::capture_bounded(
            context,
            root,
            plans,
            OperationDeadline::none("capture verifier-owned sealed tree"),
            u64::MAX,
        )
    }

    pub(super) fn capture_bounded(
        context: &'static str,
        root: &Path,
        plans: BTreeMap<PathBuf, FilePlan>,
        deadline: OperationDeadline,
        maximum_nodes: u64,
    ) -> Result<Self, String> {
        check_deadline(deadline)?;
        let root_identity = FileIdentity::capture(root, true, context)?;
        let directories = expected_directories(plans.keys());
        let expected_nodes = planned_node_count_from(&directories, plans.len())?;
        if expected_nodes > maximum_nodes {
            return Err(format!(
                "{context} planned tree exceeds its node limit of {maximum_nodes}"
            ));
        }
        let mut files = BTreeMap::new();
        for (relative, plan) in plans {
            check_deadline(deadline)?;
            let path = root.join(&relative);
            let identity = FileIdentity::capture(&path, false, context)?;
            let size_bytes = fs::symlink_metadata(&path)
                .map_err(|error| format!("inspect {context} file {}: {error}", path.display()))?
                .len();
            files.insert(
                relative,
                SealedFile {
                    identity,
                    digest: plan.digest,
                    size_bytes,
                    #[cfg(unix)]
                    executable: plan.executable,
                },
            );
        }
        let tree = Self {
            context,
            root: root.to_owned(),
            root_identity,
            directories,
            files,
            maximum_nodes: expected_nodes,
        };
        tree.revalidate_bounded(deadline)?;
        Ok(tree)
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    pub(super) fn paths(&self) -> HashSet<PathBuf> {
        self.files.keys().cloned().collect()
    }

    pub(super) fn revalidate(&self) -> Result<(), String> {
        self.revalidate_bounded(OperationDeadline::none(
            "revalidate verifier-owned sealed tree",
        ))
    }

    pub(super) fn revalidate_bounded(&self, deadline: OperationDeadline) -> Result<(), String> {
        check_deadline(deadline)?;
        if FileIdentity::capture(&self.root, true, self.context)? != self.root_identity {
            return Err(format!("{} root identity changed", self.context));
        }
        let (directories, files) =
            inventory(&self.root, self.context, deadline, self.maximum_nodes)?;
        if directories != self.directories || files != self.files.keys().cloned().collect() {
            return Err(format!("{} path inventory changed", self.context));
        }
        verify_directory_permissions(&self.root, &directories, self.context, deadline)?;
        for (relative, expected) in &self.files {
            check_deadline(deadline)?;
            revalidate_file(&self.root.join(relative), expected, self.context, deadline)?;
        }
        check_deadline(deadline)?;
        Ok(())
    }
}

fn revalidate_file(
    path: &Path,
    expected: &SealedFile,
    context: &str,
    deadline: OperationDeadline,
) -> Result<(), String> {
    if FileIdentity::capture(path, false, context)? != expected.identity {
        return Err(format!(
            "{context} file identity changed: {}",
            path.display()
        ));
    }
    let mut file = fs::File::open(path)
        .map_err(|error| format!("read {context} file {}: {error}", path.display()))?;
    let limit = expected
        .size_bytes
        .checked_add(1)
        .ok_or_else(|| format!("{context} file size overflow: {}", path.display()))?;
    let mut read_bytes = 0_u64;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    while read_bytes < limit {
        check_deadline(deadline)?;
        let remaining = usize::try_from(limit - read_bytes).map_err(|_| {
            format!(
                "{context} file size does not fit memory: {}",
                path.display()
            )
        })?;
        let chunk = remaining.min(buffer.len());
        let read = file
            .read(&mut buffer[..chunk])
            .map_err(|error| format!("read {context} file {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        read_bytes = read_bytes
            .checked_add(u64::try_from(read).map_err(|_| "sealed file read length overflow")?)
            .ok_or("sealed file read length overflow")?;
        digest.update(&buffer[..read]);
    }
    check_deadline(deadline)?;
    let digest: [u8; 32] = digest.finalize().into();
    if read_bytes != expected.size_bytes {
        return Err(format!("{context} file size changed: {}", path.display()));
    }
    if digest != expected.digest {
        return Err(format!("{context} file bytes changed: {}", path.display()));
    }
    #[cfg(unix)]
    verify_unix_mode(path, expected.executable, context)?;
    Ok(())
}

fn expected_directories<'a>(paths: impl Iterator<Item = &'a PathBuf>) -> BTreeSet<PathBuf> {
    let mut directories = BTreeSet::from([PathBuf::new()]);
    for path in paths {
        let mut parent = path.parent();
        while let Some(directory) = parent.filter(|directory| !directory.as_os_str().is_empty()) {
            directories.insert(directory.to_owned());
            parent = directory.parent();
        }
    }
    directories
}

pub(super) fn planned_node_count(plans: &BTreeMap<PathBuf, FilePlan>) -> Result<u64, String> {
    let directories = expected_directories(plans.keys());
    planned_node_count_from(&directories, plans.len())
}

fn planned_node_count_from(directories: &BTreeSet<PathBuf>, files: usize) -> Result<u64, String> {
    let directories = directories
        .len()
        .checked_sub(1)
        .ok_or_else(|| "sealed tree directory inventory omitted its root".to_owned())?;
    checked_node_count(directories, files)
}

pub(super) fn checked_node_count(directories: usize, files: usize) -> Result<u64, String> {
    let directories =
        u64::try_from(directories).map_err(|_| "sealed tree node count overflow".to_owned())?;
    let files = u64::try_from(files).map_err(|_| "sealed tree node count overflow".to_owned())?;
    directories
        .checked_add(files)
        .ok_or_else(|| "sealed tree node count overflow".to_owned())
}

fn inventory(
    root: &Path,
    context: &str,
    deadline: OperationDeadline,
    maximum_nodes: u64,
) -> Result<(BTreeSet<PathBuf>, BTreeSet<PathBuf>), String> {
    let mut directories = BTreeSet::from([PathBuf::new()]);
    let mut files = BTreeSet::new();
    let mut pending = vec![root.to_owned()];
    let mut nodes = 0_u64;
    while let Some(directory) = pending.pop() {
        check_deadline(deadline)?;
        let entries = fs::read_dir(&directory).map_err(|error| {
            format!("read {context} directory {}: {error}", directory.display())
        })?;
        for entry in entries {
            check_deadline(deadline)?;
            let entry = entry.map_err(|error| format!("read {context} entry: {error}"))?;
            nodes = nodes
                .checked_add(1)
                .ok_or_else(|| format!("{context} node count overflow"))?;
            if nodes > maximum_nodes {
                return Err(format!(
                    "{context} tree exceeds its node limit of {maximum_nodes}"
                ));
            }
            let metadata = fs::symlink_metadata(entry.path()).map_err(|error| {
                format!(
                    "inspect {context} entry {}: {error}",
                    entry.path().display()
                )
            })?;
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| format!("{context} entry escaped its root"))?
                .to_owned();
            if metadata.file_type().is_dir() {
                directories.insert(relative);
                pending.push(entry.path());
            } else if metadata.file_type().is_file() {
                files.insert(relative);
            } else {
                return Err(format!(
                    "{context} contains a non-regular entry: {}",
                    entry.path().display()
                ));
            }
        }
    }
    check_deadline(deadline)?;
    Ok((directories, files))
}

#[cfg(unix)]
fn verify_directory_permissions(
    root: &Path,
    directories: &BTreeSet<PathBuf>,
    context: &str,
    deadline: OperationDeadline,
) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    for relative in directories {
        check_deadline(deadline)?;
        let path = root.join(relative);
        let mode = fs::symlink_metadata(&path)
            .map_err(|error| format!("inspect {context} permissions {}: {error}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o222 != 0 {
            return Err(format!(
                "{context} directory became writable: {}",
                path.display()
            ));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_directory_permissions(
    _root: &Path,
    _directories: &BTreeSet<PathBuf>,
    _context: &str,
    _deadline: OperationDeadline,
) -> Result<(), String> {
    Ok(())
}

fn check_deadline(deadline: OperationDeadline) -> Result<(), String> {
    deadline.check().map_err(|error| error.to_string())
}

#[cfg(unix)]
fn verify_unix_mode(path: &Path, executable: bool, context: &str) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect {context} permissions {}: {error}", path.display()))?
        .permissions()
        .mode()
        & 0o777;
    let expected = if executable { 0o500 } else { 0o400 };
    if mode != expected {
        return Err(format!("{context} permissions changed: {}", path.display()));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(windows)]
    volume: u32,
    #[cfg(windows)]
    index: u64,
}

impl FileIdentity {
    #[cfg(unix)]
    fn capture(path: &Path, directory: bool, context: &str) -> Result<Self, String> {
        use std::os::unix::fs::MetadataExt;

        let metadata = checked_metadata(path, directory, context)?;
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    #[cfg(windows)]
    fn capture(path: &Path, directory: bool, context: &str) -> Result<Self, String> {
        use cap_std::fs::MetadataExt;

        checked_metadata(path, directory, context)?;
        let metadata = if directory {
            cap_std::fs::Dir::open_ambient_dir(path, cap_std::ambient_authority())
                .and_then(|directory| directory.dir_metadata())
        } else {
            cap_std::fs::File::open_ambient(path, cap_std::ambient_authority())
                .and_then(|file| file.metadata())
        }
        .map_err(|error| format!("inspect {context} identity {}: {error}", path.display()))?;
        Ok(Self {
            volume: metadata.volume_serial_number().ok_or_else(|| {
                format!(
                    "{context} volume identity is unavailable: {}",
                    path.display()
                )
            })?,
            index: metadata.file_index().ok_or_else(|| {
                format!("{context} file identity is unavailable: {}", path.display())
            })?,
        })
    }

    #[cfg(not(any(unix, windows)))]
    fn capture(path: &Path, _directory: bool, context: &str) -> Result<Self, String> {
        Err(format!(
            "{context} file identity is unsupported on this platform: {}",
            path.display()
        ))
    }
}

#[cfg(any(unix, windows))]
fn checked_metadata(
    path: &Path,
    directory: bool,
    context: &str,
) -> Result<std::fs::Metadata, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("inspect {context} identity {}: {error}", path.display()))?;
    let matches_kind = if directory {
        metadata.file_type().is_dir()
    } else {
        metadata.file_type().is_file()
    };
    if !matches_kind {
        return Err(format!(
            "{context} path changed file kind or became an alias: {}",
            path.display()
        ));
    }
    Ok(metadata)
}
