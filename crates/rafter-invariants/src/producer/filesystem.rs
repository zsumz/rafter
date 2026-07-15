use std::{
    cmp::Reverse,
    collections::BTreeMap,
    error::Error,
    ffi::{OsStr, OsString},
    fmt,
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use cap_std::{
    ambient_authority,
    fs::{Dir, File, Metadata, OpenOptions},
};

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};

static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);

pub(super) const TREE_LIMITS: TreeLimits = TreeLimits {
    directory_entries: 16 * 1024,
    files: 64 * 1024,
    directories: 16 * 1024,
    nodes: 96 * 1024,
    depth: 64,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct TreeLimits {
    pub(super) directory_entries: usize,
    pub(super) files: usize,
    pub(super) directories: usize,
    pub(super) nodes: usize,
    pub(super) depth: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct OperationDeadline {
    deadline: Option<Instant>,
    operation: &'static str,
}

impl OperationDeadline {
    #[cfg(test)]
    pub(super) const fn none(operation: &'static str) -> Self {
        Self {
            deadline: None,
            operation,
        }
    }

    pub(super) const fn at(deadline: Instant, operation: &'static str) -> Self {
        Self {
            deadline: Some(deadline),
            operation,
        }
    }

    pub(super) fn check(self) -> Result<(), Box<dyn Error>> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(Box::new(FilesystemDeadlineError(self.operation)));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct FilesystemDeadlineError(&'static str);

impl fmt::Display for FilesystemDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "producer filesystem deadline expired during {}",
            self.0
        )
    }
}

impl Error for FilesystemDeadlineError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EntryKind {
    Directory,
    File,
    Symlink,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FileIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(windows)]
    volume: Option<u32>,
    #[cfg(windows)]
    index: Option<u64>,
    #[cfg(not(any(unix, windows)))]
    file_type: cap_std::fs::FileType,
    #[cfg(not(any(unix, windows)))]
    len: u64,
    #[cfg(not(any(unix, windows)))]
    modified: Option<std::time::SystemTime>,
}

impl FileIdentity {
    pub(super) fn from_metadata(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        {
            use cap_std::fs::MetadataExt;
            Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            }
        }
        #[cfg(windows)]
        {
            use cap_std::fs::MetadataExt;
            Self {
                volume: metadata.volume_serial_number(),
                index: metadata.file_index(),
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            Self {
                file_type: metadata.file_type(),
                len: metadata.len(),
                modified: metadata.modified().ok(),
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct HeldDirectory {
    root: Dir,
    dir: Dir,
    root_path: PathBuf,
    relative: PathBuf,
    identity: FileIdentity,
}

#[derive(Debug)]
pub(super) struct HeldFile {
    root: Dir,
    file: File,
    root_path: PathBuf,
    relative: PathBuf,
    identity: FileIdentity,
}

#[derive(Debug)]
pub(super) struct ChildDirectory {
    #[cfg(unix)]
    descriptor: OwnedFd,
    path: PathBuf,
}

impl ChildDirectory {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(unix)]
    pub(super) fn descriptor(&self) -> BorrowedFd<'_> {
        self.descriptor.as_fd()
    }
}

impl HeldFile {
    pub(super) fn try_clone_std(&self) -> Result<std::fs::File, Box<dyn Error>> {
        Ok(self.file.try_clone()?.into_std())
    }

    pub(super) fn external_path(&self) -> PathBuf {
        if self.relative.as_os_str().is_empty() {
            self.root_path.clone()
        } else {
            self.root_path.join(&self.relative)
        }
    }

    pub(super) fn verify_path_binding(&self) -> Result<(), Box<dyn Error>> {
        let workspace = HeldDirectory {
            root: self.root.try_clone()?,
            dir: self.root.try_clone()?,
            root_path: self.root_path.clone(),
            relative: PathBuf::new(),
            identity: FileIdentity::from_metadata(&self.root.dir_metadata()?),
        };
        if workspace.file_identity(&self.relative)? != self.identity {
            return Err(format!(
                "producer file changed after it was opened: {}",
                self.relative.display()
            )
            .into());
        }
        if FileIdentity::from_metadata(&self.file.metadata()?) != self.identity {
            return Err(format!(
                "producer file handle changed unexpectedly: {}",
                self.relative.display()
            )
            .into());
        }
        Ok(())
    }
}

impl HeldDirectory {
    pub(super) fn workspace() -> Result<Self, Box<dyn Error>> {
        let root = Dir::open_ambient_dir(".", ambient_authority())?;
        let dir = root.try_clone()?;
        let identity = FileIdentity::from_metadata(&dir.dir_metadata()?);
        Ok(Self {
            root,
            dir,
            root_path: std::env::current_dir()?,
            relative: PathBuf::new(),
            identity,
        })
    }

    pub(super) fn open(path: &Path) -> Result<Self, Box<dyn Error>> {
        let workspace = Self::workspace()?;
        let relative = workspace_relative(&workspace, path)?;
        workspace.open_beneath(&relative)
    }

    pub(super) fn create_all(path: &Path) -> Result<Self, Box<dyn Error>> {
        let workspace = Self::workspace()?;
        let relative = workspace_relative(&workspace, path)?;
        workspace.create_all_beneath(&relative)
    }

    pub(super) fn replace_tree(
        path: &Path,
        limits: TreeLimits,
        deadline: OperationDeadline,
    ) -> Result<Self, Box<dyn Error>> {
        Self::replace_tree_with(path, limits, deadline, || {})
    }

    #[cfg(test)]
    pub(super) fn replace_tree_with_hook<F>(
        path: &Path,
        limits: TreeLimits,
        deadline: OperationDeadline,
        hook: F,
    ) -> Result<Self, Box<dyn Error>>
    where
        F: FnOnce(),
    {
        Self::replace_tree_with(path, limits, deadline, hook)
    }

    fn replace_tree_with<F>(
        path: &Path,
        limits: TreeLimits,
        deadline: OperationDeadline,
        hook: F,
    ) -> Result<Self, Box<dyn Error>>
    where
        F: FnOnce(),
    {
        deadline.check()?;
        let workspace = Self::workspace()?;
        let path = workspace_relative(&workspace, path)?;
        let (parent_path, name) = split_parent(&path)?;
        let parent = workspace.create_all_beneath(&parent_path)?;
        deadline.check()?;
        match parent.dir.symlink_metadata(&name) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                parent.dir.remove_file_or_symlink(&name)?;
            }
            Ok(metadata) if metadata.is_dir() => {
                let existing = parent.open_child(&name)?;
                hook();
                existing.remove_contents(limits, deadline)?;
                existing.verify_path_binding()?;
                existing.dir.remove_open_dir()?;
            }
            Ok(_) => {
                return Err(format!(
                    "producer scratch path is not a directory: {}",
                    path.display()
                )
                .into())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        deadline.check()?;
        parent.dir.create_dir(&name)?;
        let created = parent.open_child(&name)?;
        created.verify_path_binding()?;
        Ok(created)
    }

    pub(super) fn path(&self) -> &Path {
        &self.relative
    }

    pub(super) fn external_path(&self) -> PathBuf {
        if self.relative.as_os_str().is_empty() {
            self.root_path.clone()
        } else {
            self.root_path.join(&self.relative)
        }
    }

    pub(super) fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    pub(super) fn file_identity(&self, path: &Path) -> Result<FileIdentity, Box<dyn Error>> {
        Ok(FileIdentity::from_metadata(
            &self.open_file(path)?.metadata()?,
        ))
    }

    pub(super) fn directory_identity(&self, path: &Path) -> Result<FileIdentity, Box<dyn Error>> {
        Ok(self.open_dir(path)?.identity)
    }

    pub(super) fn create_dir_all(&self, path: &Path) -> Result<Self, Box<dyn Error>> {
        self.create_all_beneath(path)
    }

    pub(super) fn open_dir(&self, path: &Path) -> Result<Self, Box<dyn Error>> {
        self.open_beneath(path)
    }

    pub(super) fn verify_path_binding(&self) -> Result<(), Box<dyn Error>> {
        let reopened = open_components(&self.root, &self.relative)?;
        let observed = FileIdentity::from_metadata(&reopened.dir_metadata()?);
        if observed != self.identity {
            return Err(format!(
                "producer directory changed after it was opened: {}",
                self.relative.display()
            )
            .into());
        }
        Ok(())
    }

    pub(super) fn bind_for_child(&self) -> Result<ChildDirectory, Box<dyn Error>> {
        self.verify_path_binding()?;
        #[cfg(unix)]
        {
            let descriptor = rustix::io::dup(&self.dir)?;
            #[cfg(target_os = "linux")]
            let path = PathBuf::from(format!("/proc/self/fd/{}", descriptor.as_raw_fd()));
            #[cfg(not(target_os = "linux"))]
            let path = PathBuf::from(format!("/dev/fd/{}", descriptor.as_raw_fd()));
            Ok(ChildDirectory { descriptor, path })
        }
        #[cfg(not(unix))]
        {
            Ok(ChildDirectory {
                path: self.external_path(),
            })
        }
    }

    pub(super) fn entry_kind(&self, name: &OsStr) -> Result<Option<EntryKind>, Box<dyn Error>> {
        validate_name(name)?;
        match self.dir.symlink_metadata(name) {
            Ok(metadata) => Ok(Some(classify(&metadata)?)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn entries(
        &self,
        deadline: OperationDeadline,
    ) -> Result<Vec<(OsString, EntryKind)>, Box<dyn Error>> {
        let mut entries = BTreeMap::new();
        for entry in self.dir.entries()? {
            deadline.check()?;
            let entry = entry?;
            let name = entry.file_name();
            validate_name(&name)?;
            let kind = classify_type(entry.file_type()?)?;
            entries.insert(name, kind);
        }
        deadline.check()?;
        Ok(entries.into_iter().collect())
    }

    pub(super) fn open_file(&self, path: &Path) -> Result<File, Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, false)?;
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        let file = parent.dir.open_with(&name, &options)?;
        if !file.metadata()?.is_file() {
            return Err(format!("producer path is not a regular file: {}", path.display()).into());
        }
        Ok(file)
    }

    pub(super) fn hold_file(&self, path: &Path) -> Result<HeldFile, Box<dyn Error>> {
        let relative = join_relative(&self.relative, path)?;
        let file = self.open_file(path)?;
        let identity = FileIdentity::from_metadata(&file.metadata()?);
        Ok(HeldFile {
            root: self.root.try_clone()?,
            file,
            root_path: self.root_path.clone(),
            relative,
            identity,
        })
    }

    pub(super) fn create_new_file(&self, path: &Path) -> Result<std::fs::File, Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, true)?;
        let mut options = OpenOptions::new();
        options
            .create_new(true)
            .write(true)
            .follow(FollowSymlinks::No);
        Ok(parent.dir.open_with(&name, &options)?.into_std())
    }

    pub(super) fn read(&self, path: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut file = self.open_file(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    pub(super) fn read_with_deadline(
        &self,
        path: &Path,
        deadline: OperationDeadline,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut file = self.open_file(path)?;
        let mut bytes = Vec::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            deadline.check()?;
            let read = file.read(&mut buffer)?;
            deadline.check()?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        Ok(bytes)
    }

    pub(super) fn read_to_string_with_deadline(
        &self,
        path: &Path,
        deadline: OperationDeadline,
    ) -> Result<String, Box<dyn Error>> {
        Ok(String::from_utf8(self.read_with_deadline(path, deadline)?)?)
    }

    pub(super) fn files_below(
        &self,
        limits: TreeLimits,
        deadline: OperationDeadline,
    ) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        let mut budget = TraversalBudget::new(limits);
        let mut files = Vec::new();
        collect_files(self, Path::new(""), 0, &mut budget, deadline, &mut files)?;
        deadline.check()?;
        files.sort();
        deadline.check()?;
        Ok(files)
    }

    pub(super) fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, true)?;
        let temporary = temporary_name(&name);
        let mut options = OpenOptions::new();
        options
            .create_new(true)
            .write(true)
            .follow(FollowSymlinks::No);
        let mut file = parent.dir.open_with(&temporary, &options)?;
        let publish = (|| -> Result<(), Box<dyn Error>> {
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            parent.dir.rename(&temporary, &parent.dir, &name)?;
            parent.dir.try_clone()?.into_std_file().sync_all()?;
            Ok(())
        })();
        let _ = parent.dir.remove_file_or_symlink(&temporary);
        publish
    }

    pub(super) fn remove_file_if_exists(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, false)?;
        match parent.dir.remove_file_or_symlink(&name) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn remove_file_if_identity(
        &self,
        path: &Path,
        expected: &FileIdentity,
    ) -> Result<(), Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, false)?;
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        let file = parent.dir.open_with(&name, &options)?;
        let observed = FileIdentity::from_metadata(&file.metadata()?);
        if &observed != expected {
            return Err(format!("producer file changed before removal: {}", path.display()).into());
        }
        drop(file);
        parent.dir.remove_file(&name)?;
        Ok(())
    }

    pub(super) fn remove_dir_if_identity(
        &self,
        path: &Path,
        expected: &FileIdentity,
    ) -> Result<(), Box<dyn Error>> {
        let directory = self.open_dir(path)?;
        if directory.identity() != expected {
            return Err(format!(
                "producer directory changed before removal: {}",
                path.display()
            )
            .into());
        }
        directory.verify_path_binding()?;
        directory.dir.remove_open_dir()?;
        Ok(())
    }

    pub(super) fn remove_symlink(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, false)?;
        let metadata = parent.dir.symlink_metadata(&name)?;
        if !metadata.file_type().is_symlink() {
            return Err(format!("producer cleanup expected symlink: {}", path.display()).into());
        }
        parent.dir.remove_file_or_symlink(&name)?;
        Ok(())
    }

    pub(super) fn rename(&self, from: &Path, to: &Path) -> Result<(), Box<dyn Error>> {
        let (from_parent, from_name) = self.parent_and_name(from, false)?;
        let (to_parent, to_name) = self.parent_and_name(to, true)?;
        from_parent
            .dir
            .rename(&from_name, &to_parent.dir, &to_name)?;
        Ok(())
    }

    pub(super) fn remove_contents(
        &self,
        limits: TreeLimits,
        deadline: OperationDeadline,
    ) -> Result<(), Box<dyn Error>> {
        let mut budget = TraversalBudget::new(limits);
        let mut nodes = Vec::new();
        scan_cleanup_tree(self, Path::new(""), 0, &mut budget, deadline, &mut nodes)?;
        let mut ordered =
            BTreeMap::<Reverse<usize>, BTreeMap<Reverse<PathBuf>, CleanupNode>>::new();
        for node in nodes {
            deadline.check()?;
            ordered
                .entry(Reverse(node.depth))
                .or_default()
                .insert(Reverse(node.relative.clone()), node);
        }
        for by_path in ordered.into_values() {
            deadline.check()?;
            for node in by_path.into_values() {
                deadline.check()?;
                match node.kind {
                    EntryKind::Directory => self.remove_dir_if_identity(
                        &node.relative,
                        node.identity
                            .as_ref()
                            .ok_or("producer directory omitted cleanup identity")?,
                    )?,
                    EntryKind::File => self.remove_file_if_identity(
                        &node.relative,
                        node.identity
                            .as_ref()
                            .ok_or("producer file omitted cleanup identity")?,
                    )?,
                    EntryKind::Symlink => self.remove_symlink(&node.relative)?,
                }
            }
        }
        Ok(())
    }

    pub(super) fn remove_self(self) -> Result<(), Box<dyn Error>> {
        self.verify_path_binding()?;
        self.dir.remove_open_dir()?;
        Ok(())
    }

    fn open_beneath(&self, path: &Path) -> Result<Self, Box<dyn Error>> {
        let relative = join_relative(&self.relative, path)?;
        let dir = open_components(&self.dir, path)?;
        let identity = FileIdentity::from_metadata(&dir.dir_metadata()?);
        Ok(Self {
            root: self.root.try_clone()?,
            dir,
            root_path: self.root_path.clone(),
            relative,
            identity,
        })
    }

    fn create_all_beneath(&self, path: &Path) -> Result<Self, Box<dyn Error>> {
        let mut dir = self.dir.try_clone()?;
        let mut relative = self.relative.clone();
        for name in normal_components(path)? {
            match dir.open_dir_nofollow(&name) {
                Ok(next) => dir = next,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    match dir.create_dir(&name) {
                        Ok(()) => {}
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                        Err(error) => return Err(error.into()),
                    }
                    dir = dir.open_dir_nofollow(&name)?;
                }
                Err(error) => return Err(error.into()),
            }
            relative.push(name);
        }
        let identity = FileIdentity::from_metadata(&dir.dir_metadata()?);
        Ok(Self {
            root: self.root.try_clone()?,
            dir,
            root_path: self.root_path.clone(),
            relative,
            identity,
        })
    }

    fn open_child(&self, name: &OsStr) -> Result<Self, Box<dyn Error>> {
        validate_name(name)?;
        let dir = self.dir.open_dir_nofollow(name)?;
        let identity = FileIdentity::from_metadata(&dir.dir_metadata()?);
        Ok(Self {
            root: self.root.try_clone()?,
            dir,
            root_path: self.root_path.clone(),
            relative: self.relative.join(name),
            identity,
        })
    }

    fn parent_and_name(
        &self,
        path: &Path,
        create_parent: bool,
    ) -> Result<(Self, OsString), Box<dyn Error>> {
        let (parent, name) = split_parent(path)?;
        let parent = if create_parent {
            self.create_all_beneath(&parent)?
        } else {
            self.open_beneath(&parent)?
        };
        Ok((parent, name))
    }
}

pub(super) fn create_new_file(path: &Path) -> Result<std::fs::File, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    workspace.create_new_file(&relative)
}

pub(super) fn hold_file(path: &Path) -> Result<HeldFile, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    workspace.hold_file(&relative)
}

pub(super) fn read_file(path: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    workspace.read(&relative)
}

pub(super) fn remove_file(path: &Path) -> Result<(), Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    workspace.remove_file_if_exists(&relative)
}

pub(super) fn path_exists(path: &Path) -> Result<bool, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    let (parent, name) = workspace.parent_and_name(&relative, false)?;
    Ok(parent.entry_kind(&name)?.is_some())
}

fn workspace_relative(workspace: &HeldDirectory, path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    if path.is_absolute() {
        return path
            .strip_prefix(&workspace.root_path)
            .map(Path::to_path_buf)
            .map_err(|_| {
                format!("producer path is outside the workspace: {}", path.display()).into()
            });
    }
    Ok(path.to_path_buf())
}

#[derive(Debug)]
struct CleanupNode {
    relative: PathBuf,
    kind: EntryKind,
    identity: Option<FileIdentity>,
    depth: usize,
}

fn scan_cleanup_tree(
    directory: &HeldDirectory,
    relative: &Path,
    depth: usize,
    budget: &mut TraversalBudget,
    deadline: OperationDeadline,
    nodes: &mut Vec<CleanupNode>,
) -> Result<(), Box<dyn Error>> {
    budget.enter_directory(directory.path(), depth)?;
    let entries = directory.entries(deadline)?;
    if entries.len() > budget.limits.directory_entries {
        return Err(format!(
            "producer directory {} exceeds the entry limit of {}",
            directory.path().display(),
            budget.limits.directory_entries
        )
        .into());
    }
    for (name, kind) in entries {
        deadline.check()?;
        let node_relative = relative.join(&name);
        let node_path = directory.path().join(&name);
        let child_depth = depth
            .checked_add(1)
            .ok_or("producer filesystem depth overflow")?;
        budget.visit(&node_path, child_depth, kind)?;
        let identity = match kind {
            EntryKind::Directory => {
                let child = directory.open_child(&name)?;
                child.verify_path_binding()?;
                scan_cleanup_tree(&child, &node_relative, child_depth, budget, deadline, nodes)?;
                Some(child.identity().clone())
            }
            EntryKind::File => Some(directory.file_identity(Path::new(&name))?),
            EntryKind::Symlink => None,
        };
        nodes.push(CleanupNode {
            relative: node_relative,
            kind,
            identity,
            depth: child_depth,
        });
    }
    deadline.check()?;
    Ok(())
}

fn collect_files(
    directory: &HeldDirectory,
    relative: &Path,
    depth: usize,
    budget: &mut TraversalBudget,
    deadline: OperationDeadline,
    files: &mut Vec<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    budget.enter_directory(directory.path(), depth)?;
    let entries = directory.entries(deadline)?;
    if entries.len() > budget.limits.directory_entries {
        return Err(format!(
            "producer directory {} exceeds the entry limit of {}",
            directory.path().display(),
            budget.limits.directory_entries
        )
        .into());
    }
    for (name, kind) in entries {
        deadline.check()?;
        let node_relative = relative.join(&name);
        let node_path = directory.path().join(&name);
        let child_depth = depth
            .checked_add(1)
            .ok_or("producer filesystem depth overflow")?;
        budget.visit(&node_path, child_depth, kind)?;
        match kind {
            EntryKind::Directory => {
                let child = directory.open_child(&name)?;
                collect_files(&child, &node_relative, child_depth, budget, deadline, files)?;
            }
            EntryKind::File => files.push(node_relative),
            EntryKind::Symlink => {
                return Err(
                    format!("producer traversal rejects symlink {}", node_path.display()).into(),
                )
            }
        }
    }
    Ok(())
}

struct TraversalBudget {
    limits: TreeLimits,
    directories: usize,
    files: usize,
    nodes: usize,
}

impl TraversalBudget {
    const fn new(limits: TreeLimits) -> Self {
        Self {
            limits,
            directories: 0,
            files: 0,
            nodes: 0,
        }
    }

    fn enter_directory(&mut self, path: &Path, depth: usize) -> Result<(), Box<dyn Error>> {
        self.check_depth(path, depth)?;
        if self.directories >= self.limits.directories {
            return Err(format!(
                "producer traversal exceeds the global directory limit of {}",
                self.limits.directories
            )
            .into());
        }
        self.directories += 1;
        Ok(())
    }

    fn visit(&mut self, path: &Path, depth: usize, kind: EntryKind) -> Result<(), Box<dyn Error>> {
        self.check_depth(path, depth)?;
        if self.nodes >= self.limits.nodes {
            return Err(format!(
                "producer traversal exceeds the global node limit of {}",
                self.limits.nodes
            )
            .into());
        }
        self.nodes += 1;
        if kind == EntryKind::File {
            if self.files >= self.limits.files {
                return Err(format!(
                    "producer traversal exceeds the file limit of {}",
                    self.limits.files
                )
                .into());
            }
            self.files += 1;
        }
        Ok(())
    }

    fn check_depth(&self, path: &Path, depth: usize) -> Result<(), Box<dyn Error>> {
        if depth > self.limits.depth {
            return Err(format!(
                "producer path {} exceeds the traversal depth limit of {}",
                path.display(),
                self.limits.depth
            )
            .into());
        }
        Ok(())
    }
}

fn open_components(start: &Dir, path: &Path) -> Result<Dir, Box<dyn Error>> {
    let mut dir = start.try_clone()?;
    for name in normal_components(path)? {
        dir = dir.open_dir_nofollow(name)?;
    }
    Ok(dir)
}

fn normal_components(path: &Path) -> Result<Vec<OsString>, Box<dyn Error>> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => components.push(name.to_os_string()),
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                return Err(format!(
                    "producer filesystem path must be repository-relative: {}",
                    path.display()
                )
                .into())
            }
        }
    }
    Ok(components)
}

fn join_relative(base: &Path, path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let mut joined = base.to_path_buf();
    for component in normal_components(path)? {
        joined.push(component);
    }
    Ok(joined)
}

fn split_parent(path: &Path) -> Result<(PathBuf, OsString), Box<dyn Error>> {
    let components = normal_components(path)?;
    let (name, parent) = components
        .split_last()
        .ok_or_else(|| format!("producer filesystem path has no leaf: {}", path.display()))?;
    Ok((parent.iter().collect(), name.clone()))
}

fn validate_name(name: &OsStr) -> Result<(), Box<dyn Error>> {
    let path = Path::new(name);
    if path.components().count() != 1
        || !matches!(path.components().next(), Some(Component::Normal(_)))
    {
        return Err(format!("producer filesystem rejected entry name {}", path.display()).into());
    }
    Ok(())
}

fn classify(metadata: &Metadata) -> Result<EntryKind, Box<dyn Error>> {
    classify_type(metadata.file_type())
}

fn classify_type(file_type: cap_std::fs::FileType) -> Result<EntryKind, Box<dyn Error>> {
    if file_type.is_symlink() {
        Ok(EntryKind::Symlink)
    } else if file_type.is_dir() {
        Ok(EntryKind::Directory)
    } else if file_type.is_file() {
        Ok(EntryKind::File)
    } else {
        Err("producer filesystem rejects special filesystem nodes".into())
    }
}

fn temporary_name(name: &OsStr) -> OsString {
    let sequence = NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed);
    let mut temporary = OsString::from(".");
    temporary.push(name);
    temporary.push(format!(".tmp-{}-{sequence}", std::process::id()));
    temporary
}
