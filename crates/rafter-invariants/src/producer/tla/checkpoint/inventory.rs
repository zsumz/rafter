//! Checkpoint candidate validation, bounded inventory, hashing, and metadata I/O.

use std::{error::Error, io::Read, path::Path, time::Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{evidence::ArtifactRef, execution::filesystem::HeldDirectory};

use super::{
    super::artifact,
    cache::checkpoint_runs,
    finalization::{ensure_deadline, CheckpointDeadlineError},
    model::{HASH_BUFFER_BYTES, MAX_CHECKPOINT_METADATA_BYTES},
    traversal::{
        entry_kind, path_entry_exists, scan_checkpoint_tree, CheckpointNodeKind, TraversalBudget,
        TraversalLimits, TRAVERSAL_LIMITS,
    },
    CheckpointContract, CheckpointFile, CheckpointInventory,
};

pub(super) fn validate_candidate(
    contract_path: &Path,
    inventory_path: &Path,
    state_dir: &Path,
    expected: &CheckpointContract,
    deadline: Instant,
) -> Result<Result<Option<String>, String>, Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint candidate validation")?;
    let observed_contract: CheckpointContract =
        match read_candidate_json(contract_path, "contract", deadline)? {
            Ok(contract) => contract,
            Err(error) => return Ok(Err(error)),
        };
    ensure_deadline(deadline, "checkpoint contract validation")?;
    if &observed_contract != expected {
        return Ok(Err(
            "restored checkpoint contract is stale or incompatible".to_owned()
        ));
    }
    let observed_inventory: CheckpointInventory =
        match read_candidate_json(inventory_path, "inventory", deadline)? {
            Ok(inventory) => inventory,
            Err(error) => return Ok(Err(error)),
        };
    ensure_deadline(deadline, "checkpoint inventory validation")?;
    let contract_sha256 = match expected.sha256() {
        Ok(sha256) => sha256,
        Err(error) => return Ok(Err(format!("digest checkpoint contract: {error}"))),
    };
    let expected_inventory = match inventory(state_dir, &contract_sha256, deadline) {
        Ok(inventory) => inventory,
        Err(error)
            if error.downcast_ref::<CheckpointDeadlineError>().is_some()
                || error.downcast_ref::<std::io::Error>().is_some() =>
        {
            return Err(error)
        }
        Err(error) => return Ok(Err(format!("inventory restored checkpoint: {error}"))),
    };
    if observed_inventory != expected_inventory {
        return Ok(Err(
            "restored checkpoint inventory is malformed or stale".to_owned()
        ));
    }
    Ok(Ok(observed_inventory.latest_checkpoint))
}

pub(super) fn inventory(
    state_dir: &Path,
    contract_sha256: &str,
    deadline: Instant,
) -> Result<CheckpointInventory, Box<dyn Error>> {
    inventory_with_limits(state_dir, contract_sha256, deadline, TRAVERSAL_LIMITS)
}

pub(super) fn inventory_with_limits(
    state_dir: &Path,
    contract_sha256: &str,
    deadline: Instant,
    limits: TraversalLimits,
) -> Result<CheckpointInventory, Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint inventory")?;
    let mut files = Vec::new();
    if path_entry_exists(state_dir)? {
        let mut budget = TraversalBudget::new(limits);
        let tree = scan_checkpoint_tree(
            state_dir,
            deadline,
            "checkpoint inventory",
            &mut budget,
            false,
        )?;
        for (run, markers) in checkpoint_runs(state_dir, &tree)? {
            if !markers.complete() {
                return Err(
                    format!("checkpoint run directory is incomplete: {}", run.display()).into(),
                );
            }
        }
        for node in tree
            .nodes
            .iter()
            .filter(|node| node.kind == CheckpointNodeKind::File)
        {
            ensure_deadline(deadline, "checkpoint hashing")?;
            let (sha256, size_bytes) = hash_file(&node.path, deadline)?;
            let relative = node
                .path
                .strip_prefix(state_dir)?
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            files.push(CheckpointFile {
                path: relative,
                sha256,
                size_bytes,
            });
        }
    }
    ensure_deadline(deadline, "checkpoint inventory sorting")?;
    files.sort();
    ensure_deadline(deadline, "checkpoint inventory sorting")?;
    let latest_checkpoint = files
        .iter()
        .filter_map(|file| file.path.split_once('/').map(|(directory, _)| directory))
        .max()
        .map(str::to_owned);
    Ok(CheckpointInventory {
        schema_version: 1,
        contract_sha256: contract_sha256.to_owned(),
        latest_checkpoint,
        files,
    })
}

fn hash_file(path: &Path, deadline: Instant) -> Result<(String, u64), Box<dyn Error>> {
    let file = HeldDirectory::workspace()?.open_file(path)?;
    hash_reader(file, || {
        ensure_deadline(deadline, "checkpoint file hashing")
    })
}

pub(super) fn hash_reader<R, F>(
    mut reader: R,
    mut check_deadline: F,
) -> Result<(String, u64), Box<dyn Error>>
where
    R: Read,
    F: FnMut() -> Result<(), Box<dyn Error>>,
{
    let mut digest = Sha256::new();
    let mut size_bytes = 0_u64;
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        check_deadline()?;
        let read = reader.read(&mut buffer)?;
        check_deadline()?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        size_bytes = size_bytes
            .checked_add(u64::try_from(read)?)
            .ok_or("checkpoint file size overflow")?;
    }
    Ok((format!("{:x}", digest.finalize()), size_bytes))
}

pub(super) fn preserve_if_regular(
    source: &Path,
    output_dir: &Path,
    namespace: &Path,
    kind: &str,
    artifacts: &mut Vec<ArtifactRef>,
    deadline: Instant,
) -> Result<(), Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint diagnostic capture")?;
    if entry_kind(source)? != Some(CheckpointNodeKind::File) {
        return Ok(());
    }
    let metadata = HeldDirectory::workspace()?.open_file(source)?.metadata()?;
    if metadata.len() > MAX_CHECKPOINT_METADATA_BYTES {
        return Ok(());
    }
    let bytes = match read_file_with_deadline(source, deadline, "checkpoint diagnostic read") {
        Ok(bytes) => bytes,
        Err(error)
            if error.downcast_ref::<CheckpointDeadlineError>().is_some()
                || error.downcast_ref::<std::io::Error>().is_some() =>
        {
            return Err(error)
        }
        Err(_) => return Ok(()),
    };
    artifacts.push(artifact::write(
        output_dir,
        &namespace.join(kind),
        kind,
        &bytes,
    )?);
    ensure_deadline(deadline, "checkpoint diagnostic capture")?;
    Ok(())
}

pub(super) fn read_candidate_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
    label: &str,
    deadline: Instant,
) -> Result<Result<T, String>, Box<dyn Error>> {
    if entry_kind(path)? != Some(CheckpointNodeKind::File) {
        return Ok(Err(format!("checkpoint {label} is not a regular file")));
    }
    let metadata = HeldDirectory::workspace()?.open_file(path)?.metadata()?;
    if metadata.len() > MAX_CHECKPOINT_METADATA_BYTES {
        return Ok(Err(format!(
            "checkpoint {label} exceeds the metadata size limit"
        )));
    }
    let bytes = match read_file_with_deadline(path, deadline, "checkpoint metadata read") {
        Ok(bytes) => bytes,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(Err(format!("read checkpoint {label}: {error}")))
        }
        Err(error)
            if error.downcast_ref::<CheckpointDeadlineError>().is_some()
                || error.downcast_ref::<std::io::Error>().is_some() =>
        {
            return Err(error)
        }
        Err(error) => return Ok(Err(format!("read checkpoint {label}: {error}"))),
    };
    Ok(
        serde_json::from_slice(&bytes)
            .map_err(|error| format!("parse checkpoint {label}: {error}")),
    )
}

pub(super) fn read_file_with_deadline(
    path: &Path,
    deadline: Instant,
    operation: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut file = HeldDirectory::workspace()?.open_file(path)?;
    let metadata = file.metadata()?;
    if metadata.len() > MAX_CHECKPOINT_METADATA_BYTES {
        return Err(format!(
            "{} exceeds the checkpoint metadata size limit",
            path.display()
        )
        .into());
    }
    let mut bytes = Vec::new();
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        ensure_deadline(deadline, operation)?;
        let read = file.read(&mut buffer)?;
        ensure_deadline(deadline, operation)?;
        if read == 0 {
            break;
        }
        let next_len = bytes
            .len()
            .checked_add(read)
            .ok_or("checkpoint metadata size overflow")?;
        if u64::try_from(next_len)? > MAX_CHECKPOINT_METADATA_BYTES {
            return Err("checkpoint metadata exceeds the size limit".into());
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok(bytes)
}

pub(super) fn write_json_artifact<T: Serialize>(
    output_dir: &Path,
    namespace: &Path,
    kind: &str,
    value: &T,
) -> Result<ArtifactRef, Box<dyn Error>> {
    artifact::write(
        output_dir,
        &namespace.join(format!("{kind}.json")),
        kind,
        &serde_json::to_vec_pretty(value)?,
    )
}

pub(super) fn write_json_atomic<T: Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), Box<dyn Error>> {
    HeldDirectory::workspace()?.write_atomic(path, &serde_json::to_vec_pretty(value)?)
}
