//! Authenticated file bindings and retained semantic bytes.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use crate::{
    evidence::ArtifactRef,
    verification::{filesystem::VerificationFile, AggregateError},
};

type ProcessLog = Arc<[crate::evidence::format::process::LabeledProcess]>;
type CachedProcessLog = Result<ProcessLog, String>;
type ProcessLogCache = BTreeMap<ArtifactRef, CachedProcessLog>;

pub(super) struct AuthenticatedFile {
    pub(super) declaration: DeclaredFile,
    pub(super) file: VerificationFile,
}

pub(super) struct AuthenticatedRead {
    pub(super) file: AuthenticatedFile,
    pub(super) bytes: Option<Arc<[u8]>>,
}

#[derive(Clone)]
pub(super) struct DeclaredFile {
    pub(super) path: String,
    pub(super) size_bytes: u64,
    pub(super) sha256: String,
    pub(super) label: &'static str,
}

pub(crate) struct AuthenticatedArtifacts {
    pub(super) bytes_by_artifact: BTreeMap<ArtifactRef, Arc<[u8]>>,
    pub(super) files: Vec<AuthenticatedFile>,
    combined_processes: Mutex<ProcessLogCache>,
}

impl fmt::Debug for AuthenticatedArtifacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedArtifacts")
            .field("retained_artifacts", &self.bytes_by_artifact.len())
            .field("held_files", &self.files.len())
            .field("combined_processes", &"cached on demand")
            .finish()
    }
}

impl AuthenticatedArtifacts {
    pub(super) fn new(
        bytes_by_artifact: BTreeMap<ArtifactRef, Arc<[u8]>>,
        files: Vec<AuthenticatedFile>,
    ) -> Self {
        Self {
            bytes_by_artifact,
            files,
            combined_processes: Mutex::new(BTreeMap::new()),
        }
    }

    /// Builds a snapshot directly from bytes, for scenarios that exercise
    /// binding classification rather than artifact authentication itself.
    #[cfg(test)]
    pub(crate) fn for_test(bytes_by_artifact: BTreeMap<ArtifactRef, Arc<[u8]>>) -> Self {
        Self::new(bytes_by_artifact, Vec::new())
    }

    pub(crate) fn bytes(&self, artifact: &ArtifactRef) -> Result<&[u8], AggregateError> {
        self.bytes_by_artifact
            .get(artifact)
            .map(AsRef::as_ref)
            .ok_or_else(|| {
                AggregateError::new(format!(
                    "artifact was not retained for semantic verification: {}",
                    artifact.path
                ))
            })
    }

    pub(crate) fn text(&self, artifact: &ArtifactRef) -> Result<&str, AggregateError> {
        std::str::from_utf8(self.bytes(artifact)?).map_err(|error| {
            AggregateError::new(format!(
                "artifact is not valid UTF-8 {}: {error}",
                artifact.path
            ))
        })
    }

    pub(crate) fn combined_processes(
        &self,
        artifact: &ArtifactRef,
    ) -> Result<ProcessLog, AggregateError> {
        let mut cached = self.combined_processes.lock().map_err(|_| {
            AggregateError::new("authenticated process-log cache is poisoned".to_owned())
        })?;
        if let Some(processes) = cached.get(artifact) {
            return clone_cached_processes(processes, artifact);
        }
        let parsed = self
            .text(artifact)
            .and_then(|source| {
                crate::evidence::format::process::parse_combined_processes(source)
                    .map_err(|error| AggregateError::new(error.to_string()))
            })
            .map(Arc::from)
            .map_err(|error| error.to_string());
        cached.insert(artifact.clone(), parsed.clone());
        clone_cached_processes(&parsed, artifact)
    }

    pub(crate) fn combined_v4(&self, artifact: &ArtifactRef) -> Result<ProcessLog, AggregateError> {
        let processes = self.combined_processes(artifact)?;
        if let Some(process) = processes.iter().find(|process| {
            process.schema_version
                != crate::evidence::format::process::COMBINED_PROCESS_SCHEMA_VERSION
        }) {
            return Err(AggregateError::new(format!(
                "process log {} uses schema {}, expected {}",
                artifact.path,
                process.schema_version,
                crate::evidence::format::process::COMBINED_PROCESS_SCHEMA_VERSION
            )));
        }
        Ok(processes)
    }

    pub(crate) fn revalidate_paths(&self) -> Result<(), AggregateError> {
        for authenticated in &self.files {
            super::file::read_declared(&authenticated.file, &authenticated.declaration, false)?;
            authenticated.file.verify_path_binding().map_err(|error| {
                AggregateError::new(format!(
                    "revalidate {} {}: {error}",
                    authenticated.declaration.label, authenticated.declaration.path
                ))
            })?;
        }
        Ok(())
    }
}

fn clone_cached_processes(
    processes: &CachedProcessLog,
    artifact: &ArtifactRef,
) -> Result<ProcessLog, AggregateError> {
    processes.clone().map_err(|error| {
        AggregateError::new(format!("parse process log {}: {error}", artifact.path))
    })
}
