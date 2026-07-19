//! Identity-bound allocation, child inheritance, bounded reads, and replay retention.

use std::{
    error::Error,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(unix)]
use std::os::fd::BorrowedFd;

use crate::execution::filesystem::{HeldDirectory, HeldFile, OperationDeadline};

static TELEMETRY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const TELEMETRY_DIRECTORY: &str = "target/rafter-invariants/telemetry";

#[derive(Debug)]
pub(super) struct ProcessArtifacts {
    directory: HeldDirectory,
    stdout: HeldFile,
    stderr: HeldFile,
    resource: HeldFile,
    process_group: HeldFile,
    reservation: HeldFile,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProcessArtifactPaths {
    pub(crate) stdout: std::path::PathBuf,
    pub(crate) stderr: std::path::PathBuf,
    pub(crate) resource: std::path::PathBuf,
    pub(crate) process_group: std::path::PathBuf,
    pub(crate) reservation: std::path::PathBuf,
}

#[cfg(test)]
impl ProcessArtifactPaths {
    pub(crate) fn all(&self) -> [&Path; 5] {
        [
            &self.stdout,
            &self.stderr,
            &self.resource,
            &self.process_group,
            &self.reservation,
        ]
    }
}

impl ProcessArtifacts {
    pub(super) fn allocate() -> Result<Self, Box<dyn Error>> {
        allocate_process_artifacts_at(
            Path::new(TELEMETRY_DIRECTORY),
            std::process::id(),
            &TELEMETRY_SEQUENCE,
        )
    }

    pub(super) fn stdout_file(&self) -> Result<std::fs::File, Box<dyn Error>> {
        self.stdout.try_clone_std()
    }

    pub(super) fn stderr_file(&self) -> Result<std::fs::File, Box<dyn Error>> {
        self.stderr.try_clone_std()
    }

    #[cfg(unix)]
    pub(super) fn child_descriptors(&self) -> [BorrowedFd<'_>; 2] {
        [self.resource.descriptor(), self.process_group.descriptor()]
    }

    #[cfg(unix)]
    pub(super) fn resource_descriptor(&self) -> BorrowedFd<'_> {
        self.resource.descriptor()
    }

    #[cfg(unix)]
    pub(super) fn process_group_descriptor(&self) -> BorrowedFd<'_> {
        self.process_group.descriptor()
    }

    pub(super) fn stdout_path(&self) -> std::path::PathBuf {
        self.stdout.external_path()
    }

    pub(super) fn stderr_path(&self) -> std::path::PathBuf {
        self.stderr.external_path()
    }

    pub(super) fn resource_path(&self) -> std::path::PathBuf {
        self.resource.external_path()
    }

    #[cfg(test)]
    pub(super) fn process_group_path(&self) -> std::path::PathBuf {
        self.process_group.external_path()
    }

    #[cfg(test)]
    pub(super) fn reservation_path(&self) -> std::path::PathBuf {
        self.reservation.external_path()
    }

    pub(super) fn read_stdout(
        &self,
        deadline: OperationDeadline,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        self.stdout.read_bounded(deadline, maximum_bytes)
    }

    pub(super) fn read_stderr(
        &self,
        deadline: OperationDeadline,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        self.stderr.read_bounded(deadline, maximum_bytes)
    }

    pub(super) fn read_resource(
        &self,
        deadline: OperationDeadline,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        self.resource.read_bounded(deadline, maximum_bytes)
    }

    pub(super) fn read_process_group(
        &self,
        deadline: OperationDeadline,
        maximum_bytes: u64,
    ) -> Result<String, Box<dyn Error>> {
        self.process_group
            .read_to_string_bounded(deadline, maximum_bytes)
    }

    pub(super) fn verify_path_bindings(&self) -> Result<(), Box<dyn Error>> {
        self.directory.verify_path_binding()?;
        for file in [
            &self.stdout,
            &self.stderr,
            &self.resource,
            &self.process_group,
            &self.reservation,
        ] {
            file.verify_path_binding()?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn path_snapshot(&self) -> ProcessArtifactPaths {
        ProcessArtifactPaths {
            stdout: self.stdout.external_path(),
            stderr: self.stderr.external_path(),
            resource: self.resource.external_path(),
            process_group: self.process_group.external_path(),
            reservation: self.reservation.external_path(),
        }
    }
}

pub(super) fn allocate_process_artifacts_at(
    directory: &Path,
    process_id: u32,
    sequence: &AtomicU64,
) -> Result<ProcessArtifacts, Box<dyn Error>> {
    let directory = HeldDirectory::create_all(directory)?;
    loop {
        let sequence = sequence.fetch_add(1, Ordering::Relaxed);
        let prefix = format!("{process_id}-{sequence}");
        let reservation_name = format!("{prefix}.reserve");
        let reservation = match directory.create_new_held_file(Path::new(&reservation_name)) {
            Ok(file) => file,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists) =>
            {
                continue
            }
            Err(error) => return Err(error),
        };
        let names = [
            format!("{prefix}.stdout"),
            format!("{prefix}.stderr"),
            format!("{prefix}.time"),
            format!("{prefix}.pgid"),
        ];
        if names.iter().try_fold(false, |collision, name| {
            Ok::<_, Box<dyn Error>>(collision || directory.path_exists(Path::new(name))?)
        })? {
            // A stale receipt owns this prefix. The empty reservation is not evidence.
            reservation.remove_if_bound()?;
            continue;
        }
        let stdout = directory.create_new_held_file(Path::new(&names[0]))?;
        let stderr = directory.create_new_held_file(Path::new(&names[1]))?;
        let resource = directory.create_new_held_file(Path::new(&names[2]))?;
        let process_group = directory.create_new_held_file(Path::new(&names[3]))?;
        return Ok(ProcessArtifacts {
            directory,
            stdout,
            stderr,
            resource,
            process_group,
            reservation,
        });
    }
}
