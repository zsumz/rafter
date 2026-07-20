//! Bounded descriptor reads, hashing, and physical alias rejection.

use std::{
    io::{Read, Seek, SeekFrom},
    path::Path,
    sync::Arc,
};

use sha2::{Digest, Sha256};

use crate::{
    evidence::{ArtifactRef, PlanInput},
    verification::{
        filesystem::{VerificationFile, VerificationRoot},
        AggregateError,
    },
};

use super::model::{AuthenticatedFile, AuthenticatedRead, DeclaredFile};

#[cfg(test)]
pub(super) fn authenticate_artifact(
    artifact: &ArtifactRef,
    repository: &Path,
) -> Result<Arc<[u8]>, AggregateError> {
    super::preflight::validate_artifact_ref(artifact)?;
    let directory = VerificationRoot::open(repository)
        .map_err(|error| AggregateError::new(format!("open artifact root: {error}")))?;
    let authenticated = authenticate_artifact_at(artifact, &directory, true)?;
    authenticated
        .bytes
        .ok_or_else(|| AggregateError::new("artifact snapshot was not retained".to_owned()))
}

pub(super) fn authenticate_plan_input(
    input: &PlanInput,
    repository: &VerificationRoot,
) -> Result<AuthenticatedFile, AggregateError> {
    let declaration = DeclaredFile {
        path: input.path.clone(),
        size_bytes: input.size_bytes,
        sha256: input.sha256.clone(),
        label: "execution-plan input",
    };
    authenticate_declared_file(declaration, repository, false).map(|read| read.file)
}

pub(super) fn authenticate_artifact_at(
    artifact: &ArtifactRef,
    repository: &VerificationRoot,
    retain: bool,
) -> Result<AuthenticatedRead, AggregateError> {
    let declaration = DeclaredFile {
        path: artifact.path.clone(),
        size_bytes: artifact.size_bytes,
        sha256: artifact.sha256.clone(),
        label: "artifact",
    };
    authenticate_declared_file(declaration, repository, retain)
}

fn authenticate_declared_file(
    declaration: DeclaredFile,
    repository: &VerificationRoot,
    retain: bool,
) -> Result<AuthenticatedRead, AggregateError> {
    let file = repository
        .hold_file(Path::new(&declaration.path))
        .map_err(|error| {
            AggregateError::new(format!(
                "open {} {}: {error}",
                declaration.label, declaration.path
            ))
        })?;
    let bytes = read_declared(&file, &declaration, retain)?;
    file.verify_path_binding().map_err(|error| {
        AggregateError::new(format!(
            "bind {} {}: {error}",
            declaration.label, declaration.path
        ))
    })?;
    Ok(AuthenticatedRead {
        file: AuthenticatedFile { declaration, file },
        bytes,
    })
}

pub(super) fn read_declared(
    held: &VerificationFile,
    declaration: &DeclaredFile,
    retain: bool,
) -> Result<Option<Arc<[u8]>>, AggregateError> {
    let mut file = held.try_clone_std().map_err(|error| {
        AggregateError::new(format!(
            "clone {} {}: {error}",
            declaration.label, declaration.path
        ))
    })?;
    let metadata = file.metadata().map_err(|error| {
        AggregateError::new(format!(
            "inspect {} {}: {error}",
            declaration.label, declaration.path
        ))
    })?;
    if !metadata.is_file() || metadata.len() != declaration.size_bytes {
        return Err(AggregateError::new(format!(
            "{} size or file type mismatch: {}",
            declaration.label, declaration.path
        )));
    }
    file.seek(SeekFrom::Start(0)).map_err(|error| {
        AggregateError::new(format!(
            "seek {} {}: {error}",
            declaration.label, declaration.path
        ))
    })?;

    let mut retained = Vec::new();
    if retain {
        let capacity = usize::try_from(declaration.size_bytes).map_err(|error| {
            AggregateError::new(format!(
                "represent {} size {}: {error}",
                declaration.label, declaration.path
            ))
        })?;
        retained.try_reserve_exact(capacity).map_err(|error| {
            AggregateError::new(format!(
                "reserve {} {}: {error}",
                declaration.label, declaration.path
            ))
        })?;
    }

    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            AggregateError::new(format!(
                "read {} {}: {error}",
                declaration.label, declaration.path
            ))
        })?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|error| {
                AggregateError::new(format!("convert authenticated read length: {error}"))
            })?)
            .ok_or_else(|| {
                AggregateError::new("authenticated read length overflowed".to_owned())
            })?;
        if total > declaration.size_bytes {
            return Err(AggregateError::new(format!(
                "{} grew while being read: {}",
                declaration.label, declaration.path
            )));
        }
        digest.update(&buffer[..read]);
        if retain {
            retained.extend_from_slice(&buffer[..read]);
        }
    }
    let observed_digest = format!("{:x}", digest.finalize());
    if total != declaration.size_bytes || observed_digest != declaration.sha256 {
        return Err(AggregateError::new(format!(
            "{} integrity mismatch: {}",
            declaration.label, declaration.path
        )));
    }
    Ok(retain.then(|| Arc::<[u8]>::from(retained)))
}

pub(super) fn reject_file_alias(
    authenticated: &[AuthenticatedFile],
    candidate: &AuthenticatedFile,
) -> Result<(), AggregateError> {
    if let Some(existing) = authenticated.iter().find(|existing| {
        existing.file.identity() == candidate.file.identity()
            && (existing.declaration.path != candidate.declaration.path
                || existing.declaration.label != candidate.declaration.label)
    }) {
        return Err(AggregateError::new(format!(
            "{} {} aliases {} {}",
            candidate.declaration.label,
            candidate.declaration.path,
            existing.declaration.label,
            existing.declaration.path
        )));
    }
    Ok(())
}
