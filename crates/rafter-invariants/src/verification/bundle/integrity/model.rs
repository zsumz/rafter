//! Authenticated file bindings and retained semantic bytes.

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    evidence::ArtifactRef,
    verification::{filesystem::VerificationFile, AggregateError},
};

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
}

impl AuthenticatedArtifacts {
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
