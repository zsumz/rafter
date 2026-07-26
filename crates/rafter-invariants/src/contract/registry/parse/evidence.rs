//! Strict evidence-row parsing and layer-specific shape validation.

use std::collections::BTreeMap;

use crate::contract::{
    registry::{PersistenceEvidenceKind, RegistryEvidence, RegistryParseError},
    TestIdentity,
};

mod atomic;

use super::path::parse_repository_path;
use super::simulator::parse_simulator_identity;
use atomic::validate_atomic_group;

pub(super) fn parse_evidence_record(
    index: usize,
    record: &BTreeMap<String, String>,
) -> Result<RegistryEvidence, RegistryParseError> {
    let required = |field: &str| {
        record.get(field).cloned().ok_or_else(|| {
            RegistryParseError(format!(
                "evidence record {} is missing required field {field}",
                index + 1
            ))
        })
    };
    let layer = required("layer")?;
    let test = if layer == "tests" {
        Some(TestIdentity {
            package: required("package")?,
            target_kind: required("target_kind")?,
            target: required("target")?,
            test_name: required("test_name")?,
        })
    } else {
        None
    };
    let simulator = if layer == "simulator" {
        Some(parse_simulator_identity(index, record, &required)?)
    } else {
        None
    };
    let invariant_id = required("id")?;
    let clause_ids = required("clauses")?
        .split(',')
        .map(str::trim)
        .filter(|clause| !clause.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if clause_ids.is_empty() {
        return Err(RegistryParseError(format!(
            "evidence record {} has no clause bindings",
            index + 1
        )));
    }
    let strength = required("strength")?;
    validate_evidence_shape(index, record, &layer, &strength)?;
    validate_direct_test_binding(index, &clause_ids, &layer, &strength)?;
    let atomic_group = record.get("atomic_group").cloned();
    validate_atomic_group(
        index,
        record,
        &invariant_id,
        &clause_ids,
        &layer,
        &strength,
        atomic_group.as_deref(),
    )?;
    let path = required("path")?;
    let symbol = required("symbol")?;
    let persistence_evidence = record
        .get("persistence_evidence")
        .map(|value| match value.as_str() {
            "crash_reopen" => Ok(PersistenceEvidenceKind::CrashReopen),
            "failure_injection" => Ok(PersistenceEvidenceKind::FailureInjection),
            _ => Err(RegistryParseError(format!(
                "evidence record {} has unsupported persistence_evidence {value}",
                index + 1
            ))),
        })
        .transpose()?;
    if let Some(identity) = &test {
        validate_test_identity(index, identity, &symbol, "tests")?;
    }
    let negative_fixture_detector_path = record
        .get("negative_fixture_detector_path")
        .map(|path| parse_repository_path(index, "negative_fixture_detector_path", path))
        .transpose()?;
    Ok(RegistryEvidence {
        id: invariant_id,
        clauses: clause_ids,
        layer,
        strength,
        path,
        symbol,
        persistence_evidence,
        atomic_group,
        negative_fixture: record.get("negative_fixture").cloned(),
        negative_fixture_path: record.get("negative_fixture_path").cloned(),
        negative_fixture_detector: record.get("negative_fixture_detector").cloned(),
        negative_fixture_detector_path,
        negative_fixture_detector_bridge: record.get("negative_fixture_detector_bridge").cloned(),
        negative_fixture_uncovered: record.get("negative_fixture_uncovered").cloned(),
        negative_fixture_exemption: record.get("negative_fixture_exemption").cloned(),
        test,
        simulator,
    })
}

pub(super) fn validate_test_identity(
    index: usize,
    identity: &TestIdentity,
    symbol: &str,
    context: &str,
) -> Result<(), RegistryParseError> {
    if !matches!(identity.target_kind.as_str(), "lib" | "test" | "bin") {
        return Err(RegistryParseError(format!(
            "{context} evidence record {} has unsupported Cargo target kind {}",
            index + 1,
            identity.target_kind
        )));
    }
    if identity.package.trim().is_empty()
        || identity.target.trim().is_empty()
        || identity.test_name.split("::").any(str::is_empty)
    {
        return Err(RegistryParseError(format!(
            "{context} evidence record {} has a malformed test identity",
            index + 1
        )));
    }
    if identity.test_name.rsplit("::").next() != Some(symbol) {
        return Err(RegistryParseError(format!(
            "{context} evidence record {} symbol must equal the exact test-name leaf",
            index + 1
        )));
    }
    Ok(())
}

fn validate_direct_test_binding(
    index: usize,
    clause_ids: &[String],
    layer: &str,
    strength: &str,
) -> Result<(), RegistryParseError> {
    if layer == "tests" && strength == "direct" && clause_ids.len() != 1 {
        return Err(RegistryParseError(format!(
            "direct tests evidence record {} must bind exactly one clause, found {}",
            index + 1,
            clause_ids.len()
        )));
    }
    Ok(())
}

fn validate_evidence_shape(
    index: usize,
    record: &BTreeMap<String, String>,
    layer: &str,
    strength: &str,
) -> Result<(), RegistryParseError> {
    if !matches!(layer, "tests" | "simulator" | "tla" | "maelstrom") {
        return Err(RegistryParseError(format!(
            "evidence record {} has unsupported layer {layer}",
            index + 1
        )));
    }
    if !matches!(strength, "direct" | "e2e") {
        return Err(RegistryParseError(format!(
            "evidence record {} has unsupported strength {strength}",
            index + 1
        )));
    }

    let test_fields = ["package", "target_kind", "target", "test_name"];
    let simulator_fields = [
        "simulator_check",
        "minimum_protocol_states",
        "minimum_verifier_states",
        "minimum_runs_per_check",
        "minimum_steps",
        "required_observation",
        "minimum_observation",
        "negative_fixture_package",
        "negative_fixture_target_kind",
        "negative_fixture_target",
        "negative_fixture_test_name",
        "required_liveness_invariant",
        "required_liveness_clauses",
        "required_liveness_feature",
        "required_liveness_scenario",
        "liveness_fault_requirement",
        "liveness_stable_leader_retained",
        "liveness_stable_leader_rounds_minimum",
        "liveness_stable_leader_rounds_exact",
        "liveness_stable_leader_rounds_relation",
        "liveness_proposal_outcome",
        "liveness_authority_loss_required",
        "liveness_fault_cycle_required",
        "liveness_fairness_policy",
        "liveness_tick_bound_rounds",
        "liveness_delivery_bound_rounds",
        "liveness_max_delivery_waves_per_tick",
        "liveness_round_budget_provenance",
        "liveness_minimum_rounds",
        "liveness_rounds_per_node",
        "liveness_rounds_per_queued_message",
        "liveness_rounds_per_proposal",
        "liveness_rounds_per_membership_change",
        "liveness_rounds_per_partition",
        "liveness_snapshot_catchup_rounds",
        "liveness_phase_count",
        "liveness_fixed_rounds",
    ];
    reject_fields_outside_layer(index, record, layer, "tests", &test_fields)?;
    reject_fields_outside_layer(index, record, layer, "simulator", &simulator_fields)?;

    validate_negative_fixture_shape(index, record, layer, strength)
}

fn validate_negative_fixture_shape(
    index: usize,
    record: &BTreeMap<String, String>,
    layer: &str,
    strength: &str,
) -> Result<(), RegistryParseError> {
    let fixture = record.contains_key("negative_fixture");
    let exemption = record.contains_key("negative_fixture_exemption");
    let direct_simulator = layer == "simulator" && strength == "direct";
    if fixture && exemption {
        return Err(RegistryParseError(format!(
            "evidence record {} declares both negative_fixture and negative_fixture_exemption",
            index + 1
        )));
    }
    if record.contains_key("negative_fixture_path") && !fixture {
        return Err(RegistryParseError(format!(
            "evidence record {} declares negative_fixture_path without negative_fixture",
            index + 1
        )));
    }
    if record.contains_key("negative_fixture_detector")
        && (!fixture || layer != "simulator" || strength != "direct")
    {
        return Err(RegistryParseError(format!(
            "evidence record {} has a misplaced negative_fixture_detector",
            index + 1
        )));
    }
    if record.contains_key("negative_fixture_detector_path")
        && (!fixture
            || !record.contains_key("negative_fixture_detector")
            || layer != "simulator"
            || strength != "direct")
    {
        return Err(RegistryParseError(format!(
            "evidence record {} has a misplaced negative_fixture_detector_path",
            index + 1
        )));
    }
    if direct_simulator && exemption {
        return Err(RegistryParseError(format!(
            "direct simulator evidence record {} may not use negative_fixture_exemption",
            index + 1
        )));
    }
    if direct_simulator && !fixture {
        return Err(RegistryParseError(format!(
            "direct simulator evidence record {} lacks detector qualification",
            index + 1
        )));
    }
    if direct_simulator && !record.contains_key("negative_fixture_detector") {
        return Err(RegistryParseError(format!(
            "direct simulator evidence record {} lacks negative_fixture_detector",
            index + 1
        )));
    }
    Ok(())
}

fn reject_fields_outside_layer(
    index: usize,
    record: &BTreeMap<String, String>,
    actual_layer: &str,
    owning_layer: &str,
    fields: &[&str],
) -> Result<(), RegistryParseError> {
    if actual_layer == owning_layer {
        return Ok(());
    }
    if let Some(field) = fields.iter().find(|field| record.contains_key(**field)) {
        return Err(RegistryParseError(format!(
            "evidence record {} uses {owning_layer}-only field {field} in layer {actual_layer}",
            index + 1
        )));
    }
    Ok(())
}
