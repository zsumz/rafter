use std::collections::BTreeMap;

use crate::catalog::{CatalogError, EvidenceDescriptor, TestIdentity};

use super::simulator::parse_simulator_identity;

pub(super) fn parse_evidence_record(
    index: usize,
    record: &BTreeMap<String, String>,
) -> Result<Vec<EvidenceDescriptor>, CatalogError> {
    let required = |field: &str| {
        record.get(field).cloned().ok_or_else(|| {
            CatalogError(format!(
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
        Some(parse_simulator_identity(record, &required)?)
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
        return Err(CatalogError(format!(
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
    if let Some(identity) = &test {
        validate_test_identity(index, identity, &symbol)?;
    }
    Ok(clause_ids
        .into_iter()
        .map(|clause_id| EvidenceDescriptor {
            invariant_id: invariant_id.clone(),
            clause_id,
            layer: layer.clone(),
            strength: strength.clone(),
            path: path.clone(),
            symbol: symbol.clone(),
            atomic_group: atomic_group.clone(),
            negative_fixture: record.get("negative_fixture").cloned(),
            negative_fixture_path: record.get("negative_fixture_path").cloned(),
            negative_fixture_detector: record.get("negative_fixture_detector").cloned(),
            test: test.clone(),
            simulator: simulator.clone(),
        })
        .collect())
}

fn validate_test_identity(
    index: usize,
    identity: &TestIdentity,
    symbol: &str,
) -> Result<(), CatalogError> {
    if !matches!(identity.target_kind.as_str(), "lib" | "test" | "bin") {
        return Err(CatalogError(format!(
            "tests evidence record {} has unsupported Cargo target kind {}",
            index + 1,
            identity.target_kind
        )));
    }
    if identity.package.trim().is_empty()
        || identity.target.trim().is_empty()
        || identity.test_name.split("::").any(str::is_empty)
    {
        return Err(CatalogError(format!(
            "tests evidence record {} has a malformed test identity",
            index + 1
        )));
    }
    if identity.test_name.rsplit("::").next() != Some(symbol) {
        return Err(CatalogError(format!(
            "tests evidence record {} symbol must equal the exact test-name leaf",
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
) -> Result<(), CatalogError> {
    if layer == "tests" && strength == "direct" && clause_ids.len() != 1 {
        return Err(CatalogError(format!(
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
) -> Result<(), CatalogError> {
    if !matches!(layer, "tests" | "simulator" | "tla" | "maelstrom") {
        return Err(CatalogError(format!(
            "evidence record {} has unsupported layer {layer}",
            index + 1
        )));
    }
    if !matches!(strength, "direct" | "e2e") {
        return Err(CatalogError(format!(
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

    let fixture = record.contains_key("negative_fixture");
    let exemption = record.contains_key("negative_fixture_exemption");
    let direct_simulator = layer == "simulator" && strength == "direct";
    if fixture && exemption {
        return Err(CatalogError(format!(
            "evidence record {} declares both negative_fixture and negative_fixture_exemption",
            index + 1
        )));
    }
    if record.contains_key("negative_fixture_path") && !fixture {
        return Err(CatalogError(format!(
            "evidence record {} declares negative_fixture_path without negative_fixture",
            index + 1
        )));
    }
    if record.contains_key("negative_fixture_detector")
        && (!fixture || layer != "simulator" || strength != "direct")
    {
        return Err(CatalogError(format!(
            "evidence record {} has a misplaced negative_fixture_detector",
            index + 1
        )));
    }
    if direct_simulator && exemption {
        return Err(CatalogError(format!(
            "direct simulator evidence record {} may not use negative_fixture_exemption",
            index + 1
        )));
    }
    if direct_simulator && !fixture {
        return Err(CatalogError(format!(
            "direct simulator evidence record {} lacks detector qualification",
            index + 1
        )));
    }
    if direct_simulator && !record.contains_key("negative_fixture_detector") {
        return Err(CatalogError(format!(
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
) -> Result<(), CatalogError> {
    if actual_layer == owning_layer {
        return Ok(());
    }
    if let Some(field) = fields.iter().find(|field| record.contains_key(**field)) {
        return Err(CatalogError(format!(
            "evidence record {} uses {owning_layer}-only field {field} in layer {actual_layer}",
            index + 1
        )));
    }
    Ok(())
}

fn validate_atomic_group(
    index: usize,
    record: &BTreeMap<String, String>,
    invariant_id: &str,
    clause_ids: &[String],
    layer: &str,
    strength: &str,
    atomic_group: Option<&str>,
) -> Result<(), CatalogError> {
    let direct_simulator = layer == "simulator" && strength == "direct";
    if direct_simulator && clause_ids.len() > 1 && atomic_group.is_none() {
        return Err(CatalogError(format!(
            "direct simulator evidence record {} spans multiple clauses without a reviewed atomic_group",
            index + 1
        )));
    }
    let Some(group) = atomic_group else {
        return Ok(());
    };
    if !direct_simulator || clause_ids.len() < 2 {
        return Err(CatalogError(format!(
            "evidence record {} declares atomic_group outside multi-clause direct simulator evidence",
            index + 1
        )));
    }
    if group.trim().is_empty() || !group.starts_with(&format!("{invariant_id}/")) {
        return Err(CatalogError(format!(
            "evidence record {} atomic_group must be a nonempty stable ID prefixed with {invariant_id}/",
            index + 1
        )));
    }
    let reviewed =
        group == "CM-03/current-term-commit-point" && clause_ids == ["CM-03.a", "CM-03.b"];
    if !reviewed {
        return Err(CatalogError(format!(
            "evidence record {} atomic_group `{group}` is not a reviewed atomic clause set",
            index + 1
        )));
    }
    if !record.contains_key("negative_fixture") || !record.contains_key("negative_fixture_detector")
    {
        return Err(CatalogError(format!(
            "evidence record {} atomic_group must bind a detector-level negative fixture",
            index + 1
        )));
    }
    Ok(())
}
