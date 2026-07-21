//! Checkpoint candidate inspection and recovery preparation.

use std::{collections::BTreeMap, error::Error, path::Path, time::Instant};

use crate::{evidence::ArtifactRef, execution::filesystem::HeldDirectory};

use super::{
    cache::{initialize_cache_root, write_cache_valid_marker},
    finalization::ensure_deadline,
    inventory::{preserve_if_regular, validate_candidate, write_json_artifact, write_json_atomic},
    model::{expected_contract, CandidateRecovery, CheckpointLayout, Preparation},
    traversal::{
        directory_has_entries, entry_kind, path_entry_exists, sanitize_cache_root,
        CheckpointNodeKind,
    },
    CheckpointContract, RecoveryReport, RecoveryStatus, RECOVERED_CONTRACT_KIND,
    RECOVERED_INVENTORY_KIND, RECOVERY_REPORT_KIND,
};

pub(in crate::producer) fn prepare(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    source_artifacts: &[ArtifactRef],
    output_dir: &Path,
    deadline: Instant,
) -> Result<Preparation, Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint preparation")?;
    let contract = expected_contract(profile, configuration, source_artifacts)?;
    let contract_sha256 = contract.sha256()?;
    let layout = CheckpointLayout::new(profile, source_ref);
    let root_is_symlink = initialize_cache_root(&layout.root, deadline)?;
    let mut recovery =
        inspect_candidate(&layout, root_is_symlink, &contract, output_dir, deadline)?;
    let report = recovery_report(&recovery, contract_sha256.clone());
    recovery.artifacts.push(write_json_artifact(
        output_dir,
        &layout.namespace,
        RECOVERY_REPORT_KIND,
        &report,
    )?);
    ensure_deadline(deadline, "checkpoint recovery report capture")?;

    let state_handle = if recovery.error.is_some() {
        sanitize_cache_root(&layout.root, deadline)?;
        ensure_deadline(deadline, "checkpoint cache sanitization")?;
        write_cache_valid_marker(&layout.root, "clean", &contract_sha256)?;
        None
    } else {
        let state_handle = HeldDirectory::create_all(&layout.state_dir)?;
        write_json_atomic(&layout.contract_path, &contract)?;
        Some(state_handle)
    };
    let recover_handle = recovery
        .recover_from
        .as_deref()
        .map(HeldDirectory::open)
        .transpose()?;
    ensure_deadline(deadline, "checkpoint preparation finalization")?;

    Ok(Preparation {
        state_dir: layout.state_dir,
        state_handle,
        recover_handle,
        report,
        error: recovery.error,
        artifacts: recovery.artifacts,
        contract,
        root: layout.root,
        namespace: layout.namespace,
    })
}

fn inspect_candidate(
    layout: &CheckpointLayout,
    root_is_symlink: bool,
    contract: &CheckpointContract,
    output_dir: &Path,
    deadline: Instant,
) -> Result<CandidateRecovery, Box<dyn Error>> {
    let contract_present = !root_is_symlink && path_entry_exists(&layout.contract_path)?;
    let inventory_present = !root_is_symlink && path_entry_exists(&layout.inventory_path)?;
    let states_is_symlink =
        !root_is_symlink && entry_kind(&layout.state_dir)? == Some(CheckpointNodeKind::Symlink);
    let states_present = states_is_symlink
        || (!root_is_symlink && directory_has_entries(&layout.state_dir, deadline)?);
    let candidate_present =
        root_is_symlink || contract_present || inventory_present || states_present;
    let mut artifacts = Vec::new();
    if candidate_present && !root_is_symlink {
        preserve_if_regular(
            &layout.contract_path,
            output_dir,
            &layout.namespace,
            RECOVERED_CONTRACT_KIND,
            &mut artifacts,
            deadline,
        )?;
        preserve_if_regular(
            &layout.inventory_path,
            output_dir,
            &layout.namespace,
            RECOVERED_INVENTORY_KIND,
            &mut artifacts,
            deadline,
        )?;
    }

    let compatibility = if root_is_symlink {
        Err("restored checkpoint cache root is a symlink".to_owned())
    } else if states_is_symlink {
        Err("restored checkpoint states directory is a symlink".to_owned())
    } else if candidate_present {
        validate_candidate(
            &layout.contract_path,
            &layout.inventory_path,
            &layout.state_dir,
            contract,
            deadline,
        )?
    } else {
        Ok(None)
    };
    let (recover_from, error) = match compatibility {
        Ok(Some(checkpoint)) => (Some(layout.state_dir.join(checkpoint)), None),
        Ok(None) => (None, None),
        Err(error) => (None, Some(error)),
    };
    Ok(CandidateRecovery {
        candidate_present,
        recover_from,
        error,
        artifacts,
    })
}

fn recovery_report(recovery: &CandidateRecovery, contract_sha256: String) -> RecoveryReport {
    let status = if recovery.error.is_some() {
        RecoveryStatus::Incompatible
    } else if recovery.recover_from.is_some() {
        RecoveryStatus::Compatible
    } else {
        RecoveryStatus::Fresh
    };
    RecoveryReport {
        schema_version: 1,
        status,
        contract_sha256,
        candidate_present: recovery.candidate_present,
        recovery_attempted: recovery.recover_from.is_some(),
        recovered_checkpoint: recovery
            .recover_from
            .as_ref()
            .and_then(|path| path.file_name())
            .map(|name| name.to_string_lossy().into_owned()),
        error: recovery.error.clone(),
    }
}
