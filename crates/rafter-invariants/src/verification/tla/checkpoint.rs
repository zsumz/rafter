//! Independent acceptance of TLA+ checkpoint recovery metadata.

use std::{collections::BTreeMap, path::Path};

use sha2::{Digest, Sha256};

use crate::evidence::format::tla::checkpoint::{
    CheckpointContract, CheckpointInventory, RecoveryReport, RecoveryStatus, CONTRACT_KIND,
    INVENTORY_KIND, RECOVERED_CONTRACT_KIND, RECOVERED_INVENTORY_KIND, RECOVERY_REPORT_KIND,
};
use crate::{
    evidence::{ArtifactRef, CheckCompletion, CheckReceipt, ResultBundle},
    verification::AggregateError,
};

use super::artifact::{has_kind, read_json_kind};
use crate::verification::AuthenticatedArtifacts;

const INPUT_KINDS: [&str; 10] = [
    "tla-tool",
    "tla-spec",
    "tla-trace-spec",
    "tla-detector-spec",
    "tla-runner",
    "tla-tool-asset-id",
    "tla-tool-checksums",
    "tla-config",
    "tla-trace-config",
    "tla-detector-config",
];

fn expected_contract(
    profile: &str,
    configuration: &BTreeMap<String, String>,
    artifacts: &[ArtifactRef],
) -> Result<CheckpointContract, AggregateError> {
    let mut input_sha256 = BTreeMap::new();
    for kind in INPUT_KINDS {
        let mut matching = artifacts.iter().filter(|artifact| artifact.kind == kind);
        let artifact = matching.next().ok_or_else(|| {
            AggregateError::new(format!(
                "checkpoint contract requires exactly one {kind} artifact"
            ))
        })?;
        if matching.next().is_some() {
            return Err(AggregateError::new(format!(
                "checkpoint contract requires exactly one {kind} artifact"
            )));
        }
        input_sha256.insert(kind.to_owned(), artifact.sha256.clone());
    }
    let config = configuration
        .get("config")
        .ok_or_else(|| AggregateError::new("checkpoint contract omitted config".to_owned()))?;
    let runner_contract = serde_json::to_vec(configuration).map_err(|error| {
        AggregateError::new(format!("serialize TLA checkpoint runner contract: {error}"))
    })?;
    Ok(CheckpointContract {
        schema_version: 1,
        profile: profile.to_owned(),
        config: config.clone(),
        runner_contract_sha256: format!("{:x}", Sha256::digest(runner_contract)),
        input_sha256,
    })
}

pub(super) fn verify_checkpoint_authenticated(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    main_has_violation: bool,
    authenticated: &AuthenticatedArtifacts,
) -> Result<Option<RecoveryReport>, AggregateError> {
    let checkpoint_enabled = bundle.execution.plan.contract.runners["tla"]
        .configuration
        .contains_key("checkpoint_minutes");
    if !checkpoint_enabled {
        for kind in [
            CONTRACT_KIND,
            INVENTORY_KIND,
            RECOVERED_CONTRACT_KIND,
            RECOVERED_INVENTORY_KIND,
            RECOVERY_REPORT_KIND,
        ] {
            if has_kind(check, kind)? {
                return Err(AggregateError::new(format!(
                    "non-checkpointed TLA receipt contains {kind}"
                )));
            }
        }
        return Ok(None);
    }

    let report: RecoveryReport = read_json_kind(check, RECOVERY_REPORT_KIND, authenticated)?;
    let contract = expected_contract(
        &bundle.profile,
        &bundle.execution.plan.contract.runners["tla"].configuration,
        &check.artifacts,
    )
    .map_err(|error| AggregateError::new(format!("derive TLA checkpoint contract: {error}")))?;
    let contract_sha256 = contract
        .sha256()
        .map_err(|error| AggregateError::new(format!("digest TLA checkpoint contract: {error}")))?;
    if report.schema_version != 1 || report.contract_sha256 != contract_sha256 {
        return Err(AggregateError::new(
            "TLA checkpoint recovery report does not match the exact execution contract".to_owned(),
        ));
    }
    let report_shape_valid = match report.status {
        RecoveryStatus::Fresh => {
            !report.recovery_attempted
                && report.recovered_checkpoint.is_none()
                && report.error.is_none()
        }
        RecoveryStatus::Compatible => {
            report.candidate_present
                && report.recovery_attempted
                && report.recovered_checkpoint.is_some()
                && report.error.is_none()
        }
        RecoveryStatus::Incompatible => {
            report.candidate_present
                && !report.recovery_attempted
                && report.recovered_checkpoint.is_none()
                && report.error.as_ref().is_some_and(|error| !error.is_empty())
        }
    };
    if !report_shape_valid {
        return Err(AggregateError::new(
            "TLA checkpoint recovery report has inconsistent status fields".to_owned(),
        ));
    }

    verify_final_metadata(
        check,
        report.status,
        &contract,
        &contract_sha256,
        main_has_violation,
        authenticated,
    )?;

    if report.candidate_present && report.status != RecoveryStatus::Incompatible {
        let recovered_contract: CheckpointContract =
            read_json_kind(check, RECOVERED_CONTRACT_KIND, authenticated)?;
        let recovered_inventory: CheckpointInventory =
            read_json_kind(check, RECOVERED_INVENTORY_KIND, authenticated)?;
        if recovered_contract != contract {
            return Err(AggregateError::new(
                "restored TLA checkpoint metadata does not match the execution contract".to_owned(),
            ));
        }
        validate_inventory(&recovered_inventory, &contract_sha256)?;
        if report.status == RecoveryStatus::Compatible
            && recovered_inventory.latest_checkpoint != report.recovered_checkpoint
        {
            return Err(AggregateError::new(
                "TLA recovery report selected a checkpoint outside the restored inventory"
                    .to_owned(),
            ));
        }
    }
    Ok(Some(report))
}

#[cfg(test)]
pub(super) fn verify_checkpoint(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    root: &Path,
    main_has_violation: bool,
) -> Result<Option<RecoveryReport>, AggregateError> {
    let authenticated = crate::verification::snapshot_available_artifacts(bundle, root)?;
    verify_checkpoint_authenticated(bundle, check, main_has_violation, &authenticated)
}

fn verify_final_metadata(
    check: &CheckReceipt,
    recovery_status: RecoveryStatus,
    contract: &CheckpointContract,
    contract_sha256: &str,
    main_has_violation: bool,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    let final_contract_present = has_kind(check, CONTRACT_KIND)?;
    let final_inventory_present = has_kind(check, INVENTORY_KIND)?;
    if final_contract_present != final_inventory_present {
        return Err(AggregateError::new(
            "TLA final checkpoint contract and inventory must be retained together".to_owned(),
        ));
    }
    if recovery_status == RecoveryStatus::Incompatible {
        if final_contract_present {
            return Err(AggregateError::new(
                "incompatible TLA recovery must not overwrite final checkpoint metadata".to_owned(),
            ));
        }
        return Ok(());
    }
    if main_has_violation
        && matches!(
            check.completion,
            CheckCompletion::Counterexample | CheckCompletion::HarnessError
        )
    {
        if final_contract_present {
            return Err(AggregateError::new(
                "violating TLA execution must abandon final checkpoint publication".to_owned(),
            ));
        }
        return Ok(());
    }
    let final_contract: CheckpointContract = read_json_kind(check, CONTRACT_KIND, authenticated)?;
    let final_inventory: CheckpointInventory =
        read_json_kind(check, INVENTORY_KIND, authenticated)?;
    if final_contract != *contract {
        return Err(AggregateError::new(
            "TLA final checkpoint metadata does not match the execution contract".to_owned(),
        ));
    }
    validate_inventory(&final_inventory, contract_sha256)
}

#[cfg(test)]
#[path = "tests/checkpoint.rs"]
mod tests;

pub(crate) fn validate_inventory(
    inventory: &CheckpointInventory,
    contract_sha256: &str,
) -> Result<(), AggregateError> {
    let paths = inventory
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let top_level = inventory
        .files
        .iter()
        .filter_map(|file| file.path.split_once('/').map(|(directory, _)| directory))
        .collect::<std::collections::BTreeSet<_>>();
    let expected_latest = top_level
        .iter()
        .next()
        .map(|directory| (*directory).to_owned());
    let has_committed_checkpoint = inventory.files.iter().any(|file| {
        file.path
            .rsplit('/')
            .next()
            .is_some_and(|name| has_tlc_extension(name, "chkpt"))
    });
    let has_temporary_checkpoint = inventory.files.iter().any(|file| {
        file.path
            .rsplit('/')
            .next()
            .is_some_and(|name| has_tlc_extension(name, "tmp"))
    });
    let valid_files = inventory.files.iter().all(|file| {
        file.sha256.len() == 64
            && file
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && file.path.split_once('/').is_some()
            && !file
                .path
                .split('/')
                .any(|component| component.is_empty() || component == "." || component == "..")
    });
    if inventory.schema_version != 1
        || inventory.contract_sha256 != contract_sha256
        || paths.len() != inventory.files.len()
        || !valid_files
        || top_level.len() > 1
        || inventory.latest_checkpoint != expected_latest
        || (!inventory.files.is_empty() && !has_committed_checkpoint)
        || has_temporary_checkpoint
    {
        return Err(AggregateError::new(
            "TLA checkpoint inventory is malformed or not contract-bound".to_owned(),
        ));
    }
    Ok(())
}

fn has_tlc_extension(name: &str, expected: &str) -> bool {
    let path = Path::new(name);
    path.extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
        || path
            .file_stem()
            .and_then(|stem| Path::new(stem).extension())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
}
