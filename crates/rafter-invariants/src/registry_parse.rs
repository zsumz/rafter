use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::{
    CatalogError, ClauseDescriptor, EvidenceDescriptor, InvariantDescriptor, SimulatorIdentity,
    TestIdentity,
};
use crate::types::SimulatorLivenessContract;

pub(super) fn parse_registry_schema_version(source: &str) -> Result<u32, CatalogError> {
    let mut schema_version = None;
    for (index, line) in source.lines().enumerate() {
        if let Some(value) = line.strip_prefix("schema_version: ") {
            if schema_version.is_some() {
                return Err(CatalogError(format!(
                    "registry has duplicate schema_version at line {}",
                    index + 1
                )));
            }
            schema_version = Some(parse_u32(&parse_scalar(
                value,
                index + 1,
                "schema_version",
                false,
            )?)?);
        } else if line.starts_with("schema_version:") {
            return Err(CatalogError(format!(
                "registry has malformed schema_version at line {}",
                index + 1
            )));
        }
    }
    schema_version.ok_or_else(|| CatalogError("registry is missing schema_version".to_owned()))
}

pub(super) fn parse_invariants(
    source: &str,
) -> Result<(Vec<InvariantDescriptor>, BTreeSet<String>), CatalogError> {
    let records = parse_section_records(source, "invariants:")?;
    if records.is_empty() {
        return Err(CatalogError(
            "registry contains no invariant IDs".to_owned(),
        ));
    }
    ensure_unique_record_ids("invariant", &records)?;
    let invariants = records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let required = required_field("invariant", index, record);
            Ok(InvariantDescriptor {
                id: required("id")?,
                statement: required("statement")?,
                scope: required("scope")?,
                assumptions: required("assumptions")?,
            })
        })
        .collect::<Result<Vec<_>, CatalogError>>()?;
    let canonical_ids = records
        .iter()
        .filter(|record| record.get("tier").is_some_and(|tier| tier == "canonical"))
        .filter_map(|record| record.get("id").cloned())
        .collect();
    Ok((invariants, canonical_ids))
}

pub(super) fn parse_clauses(source: &str) -> Result<Vec<ClauseDescriptor>, CatalogError> {
    let records = parse_section_records(source, "clauses:")?;
    ensure_unique_record_ids("clause", &records)?;
    records
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            let required = required_field("clause", index, &record);
            Ok(ClauseDescriptor {
                clause_id: required("id")?,
                invariant_id: required("invariant_id")?,
                statement: required("statement")?,
                scope: required("scope")?,
                assumptions: required("assumptions")?,
                required: parse_bool(&required("required")?)?,
            })
        })
        .collect()
}

pub(super) fn parse_evidence(source: &str) -> Result<Vec<EvidenceDescriptor>, CatalogError> {
    parse_section_records(source, "evidence:")?
        .into_iter()
        .enumerate()
        .map(|(index, record)| parse_evidence_record(index, &record))
        .collect::<Result<Vec<_>, _>>()
        .map(|records| records.into_iter().flatten().collect())
}

fn parse_evidence_record(
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
            test: test.clone(),
            simulator: simulator.clone(),
        })
        .collect())
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

fn parse_simulator_identity(
    record: &BTreeMap<String, String>,
    required: &impl Fn(&str) -> Result<String, CatalogError>,
) -> Result<SimulatorIdentity, CatalogError> {
    let checks = required("simulator_check")?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let negative_test = if record.contains_key("negative_fixture") {
        Some(TestIdentity {
            package: required("negative_fixture_package")?,
            target_kind: required("negative_fixture_target_kind")?,
            target: required("negative_fixture_target")?,
            test_name: required("negative_fixture_test_name")?,
        })
    } else {
        None
    };
    let liveness_report = record
        .get("required_liveness_feature")
        .cloned()
        .map(|feature_id| parse_liveness_contract(feature_id, required))
        .transpose()?;
    let identity = SimulatorIdentity {
        checks,
        required_observation: required("required_observation")?,
        minimum_observation: parse_usize(&required("minimum_observation")?)?,
        minimum_protocol_states: optional_usize(record, "minimum_protocol_states")?,
        minimum_verifier_states: optional_usize(record, "minimum_verifier_states")?,
        minimum_runs_per_check: optional_usize(record, "minimum_runs_per_check")?,
        minimum_steps: optional_usize(record, "minimum_steps")?,
        liveness_report,
        negative_test,
    };
    let safety = identity.liveness_report.is_none()
        && identity.minimum_protocol_states.is_some()
        && identity.minimum_verifier_states.is_some();
    let liveness = identity.liveness_report.as_ref().is_some_and(|contract| {
        contract.invariant_id == record.get("id").cloned().unwrap_or_default()
            && !contract.clause_ids.is_empty()
            && split_list(
                record
                    .get("clauses")
                    .map(String::as_str)
                    .unwrap_or_default(),
            ) == contract.clause_ids
            && contract.observation_id == identity.required_observation
            && !contract.feature_id.is_empty()
            && !contract.scenario_id.is_empty()
            && !contract.fairness_policy_id.is_empty()
            && contract.round_budget_provenance == "liveness-round-budget-v1"
            && matches!(
                contract.stable_leader_rounds_relation.as_str(),
                "none" | "exact" | "probe-rounds"
            )
    }) && identity.minimum_runs_per_check.is_some()
        && identity.minimum_steps.is_some();
    if identity.checks.is_empty() || identity.minimum_observation == 0 || !(safety || liveness) {
        return Err(CatalogError(
            "simulator evidence has an incomplete execution contract".to_owned(),
        ));
    }
    Ok(identity)
}

fn parse_liveness_contract(
    feature_id: String,
    required: &impl Fn(&str) -> Result<String, CatalogError>,
) -> Result<SimulatorLivenessContract, CatalogError> {
    Ok(SimulatorLivenessContract {
        invariant_id: required("required_liveness_invariant")?,
        clause_ids: split_list(&required("required_liveness_clauses")?),
        feature_id,
        scenario_id: required("required_liveness_scenario")?,
        observation_id: required("required_observation")?,
        fault_requirement: required("liveness_fault_requirement")?,
        stable_leader_retained: parse_optional_bool(&required("liveness_stable_leader_retained")?)?,
        stable_leader_rounds_minimum: parse_optional_u64(&required(
            "liveness_stable_leader_rounds_minimum",
        )?)?,
        stable_leader_rounds_exact: parse_optional_u64(&required(
            "liveness_stable_leader_rounds_exact",
        )?)?,
        stable_leader_rounds_relation: required("liveness_stable_leader_rounds_relation")?,
        proposal_outcome: required("liveness_proposal_outcome")?,
        authority_loss_required: parse_bool(&required("liveness_authority_loss_required")?)?,
        fault_cycle_required: parse_bool(&required("liveness_fault_cycle_required")?)?,
        fairness_policy_id: required("liveness_fairness_policy")?,
        fairness_tick_bound_rounds: parse_u64(&required("liveness_tick_bound_rounds")?)?,
        fairness_delivery_bound_rounds: parse_u64(&required("liveness_delivery_bound_rounds")?)?,
        fairness_max_delivery_waves_per_tick: parse_u64(&required(
            "liveness_max_delivery_waves_per_tick",
        )?)?,
        round_budget_provenance: required("liveness_round_budget_provenance")?,
        minimum_rounds: parse_u64(&required("liveness_minimum_rounds")?)?,
        rounds_per_node: parse_u64(&required("liveness_rounds_per_node")?)?,
        rounds_per_queued_message: parse_u64(&required("liveness_rounds_per_queued_message")?)?,
        rounds_per_proposal: parse_u64(&required("liveness_rounds_per_proposal")?)?,
        rounds_per_membership_change: parse_u64(&required(
            "liveness_rounds_per_membership_change",
        )?)?,
        rounds_per_partition: parse_u64(&required("liveness_rounds_per_partition")?)?,
        snapshot_catchup_rounds: parse_u64(&required("liveness_snapshot_catchup_rounds")?)?,
        phase_count: parse_u64(&required("liveness_phase_count")?)?,
        fixed_rounds: parse_u64(&required("liveness_fixed_rounds")?)?,
    })
}

fn optional_usize(
    record: &BTreeMap<String, String>,
    field: &str,
) -> Result<Option<usize>, CatalogError> {
    record
        .get(field)
        .map(|value| parse_usize(value))
        .transpose()
}

fn parse_usize(value: &str) -> Result<usize, CatalogError> {
    value
        .parse()
        .map_err(|error| CatalogError(format!("invalid integer {value}: {error}")))
}

fn parse_u64(value: &str) -> Result<u64, CatalogError> {
    value
        .parse()
        .map_err(|error| CatalogError(format!("invalid integer {value}: {error}")))
}

fn parse_u32(value: &str) -> Result<u32, CatalogError> {
    value
        .parse()
        .map_err(|error| CatalogError(format!("invalid schema version {value}: {error}")))
}

fn parse_bool(value: &str) -> Result<bool, CatalogError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(CatalogError(format!("invalid boolean {value}"))),
    }
}

fn parse_optional_bool(value: &str) -> Result<Option<bool>, CatalogError> {
    match value {
        "none" => Ok(None),
        _ => parse_bool(value).map(Some),
    }
}

fn parse_optional_u64(value: &str) -> Result<Option<u64>, CatalogError> {
    match value {
        "none" => Ok(None),
        _ => parse_u64(value).map(Some),
    }
}

fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn required_field<'a>(
    kind: &'static str,
    index: usize,
    record: &'a BTreeMap<String, String>,
) -> impl Fn(&str) -> Result<String, CatalogError> + 'a {
    move |field| {
        record.get(field).cloned().ok_or_else(|| {
            CatalogError(format!(
                "{kind} record {} is missing required field {field}",
                index + 1
            ))
        })
    }
}

fn parse_section_records(
    source: &str,
    section: &'static str,
) -> Result<Vec<BTreeMap<String, String>>, CatalogError> {
    let mut records = Vec::new();
    let mut current = None::<BTreeMap<String, String>>;
    let mut nested_field = None::<String>;
    let mut found = false;
    let mut active = false;
    for (index, raw_line) in source.lines().enumerate() {
        let line_number = index + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if raw_line == line {
            if line == section {
                if found {
                    return Err(CatalogError(format!(
                        "registry has duplicate {section} section at line {line_number}"
                    )));
                }
                found = true;
                active = true;
                continue;
            }
            if active {
                if !is_record_section(line) {
                    return Err(unsupported_line(section, line_number, raw_line));
                }
                if let Some(record) = current.take() {
                    records.push(record);
                }
                nested_field = None;
                active = false;
            }
            continue;
        }
        if !active {
            continue;
        }
        parse_record_line(
            raw_line,
            line_number,
            section,
            &mut records,
            &mut current,
            &mut nested_field,
        )?;
    }
    if active {
        if let Some(record) = current {
            records.push(record);
        }
    }
    if !found {
        return Err(CatalogError(format!(
            "registry is missing {section} section"
        )));
    }
    Ok(records)
}

fn parse_record_line(
    raw_line: &str,
    line_number: usize,
    section: &str,
    records: &mut Vec<BTreeMap<String, String>>,
    current: &mut Option<BTreeMap<String, String>>,
    nested_field: &mut Option<String>,
) -> Result<(), CatalogError> {
    let content = raw_line.trim_start_matches(' ');
    let indent = raw_line.len() - content.len();
    if content.starts_with('\t') {
        return Err(unsupported_line(section, line_number, raw_line));
    }
    match indent {
        2 => {
            let Some(value) = content.strip_prefix("- id: ") else {
                return Err(CatalogError(format!(
                    "malformed record start in {section} at line {line_number}: {}",
                    raw_line.trim()
                )));
            };
            if let Some(record) = current.take() {
                records.push(record);
            }
            let mut record = BTreeMap::new();
            record.insert(
                "id".to_owned(),
                parse_scalar(value, line_number, "id", true)?,
            );
            *current = Some(record);
            *nested_field = None;
        }
        4 => {
            let Some(record) = current.as_mut() else {
                return Err(CatalogError(format!(
                    "field appears before the first {section} record at line {line_number}"
                )));
            };
            if !content.contains(": ") {
                if let Some(key) = content.strip_suffix(':') {
                    if nested_fields(section, key).is_none() {
                        return Err(CatalogError(format!(
                            "unsupported field {key} in {section} at line {line_number}"
                        )));
                    }
                    insert_field(record, key, String::new(), section, line_number)?;
                    *nested_field = Some(key.to_owned());
                    return Ok(());
                }
            }
            let (key, value) = parse_field(content, section, line_number)?;
            if key != "id" && !section_fields(section).contains(&key) {
                return Err(CatalogError(format!(
                    "unsupported field {key} in {section} at line {line_number}"
                )));
            }
            let value = parse_scalar(value, line_number, key, true)?;
            insert_field(record, key, value, section, line_number)?;
            *nested_field = None;
        }
        6 => {
            let Some(parent) = nested_field.as_deref() else {
                return Err(unsupported_line(section, line_number, raw_line));
            };
            let (key, value) = parse_field(content, section, line_number)?;
            let supported =
                nested_fields(section, parent).is_some_and(|fields| fields.contains(&key));
            if !supported {
                return Err(CatalogError(format!(
                    "unsupported nested field {parent}.{key} in {section} at line {line_number}"
                )));
            }
            let value = parse_scalar(value, line_number, key, true)?;
            let flattened = format!("{parent}.{key}");
            let Some(record) = current.as_mut() else {
                return Err(CatalogError(format!(
                    "nested field appears before the first {section} record at line {line_number}"
                )));
            };
            insert_field(record, &flattened, value, section, line_number)?;
        }
        _ => return Err(unsupported_line(section, line_number, raw_line)),
    }
    Ok(())
}

fn parse_field<'a>(
    content: &'a str,
    section: &str,
    line_number: usize,
) -> Result<(&'a str, &'a str), CatalogError> {
    content.split_once(": ").ok_or_else(|| {
        CatalogError(format!(
            "malformed field in {section} at line {line_number}: {content}"
        ))
    })
}

fn insert_field(
    record: &mut BTreeMap<String, String>,
    key: &str,
    value: String,
    section: &str,
    line_number: usize,
) -> Result<(), CatalogError> {
    if record.insert(key.to_owned(), value).is_some() {
        return Err(CatalogError(format!(
            "duplicate field {key} in {section} at line {line_number}"
        )));
    }
    Ok(())
}

fn unsupported_line(section: &str, line_number: usize, raw_line: &str) -> CatalogError {
    CatalogError(format!(
        "unsupported syntax in {section} at line {line_number}: {}",
        raw_line.trim()
    ))
}

fn parse_scalar(
    value: &str,
    line_number: usize,
    field: &str,
    require_quoted: bool,
) -> Result<String, CatalogError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CatalogError(format!(
            "field {field} has an empty value at line {line_number}"
        )));
    }
    if value.starts_with('"') || value.ends_with('"') {
        return serde_json::from_str(value).map_err(|error| {
            CatalogError(format!(
                "field {field} has a malformed quoted value at line {line_number}: {error}"
            ))
        });
    }
    if require_quoted {
        return Err(CatalogError(format!(
            "field {field} must use a quoted scalar at line {line_number}"
        )));
    }
    if value.contains('#') || value.contains('"') {
        return Err(CatalogError(format!(
            "field {field} has unsupported scalar syntax at line {line_number}"
        )));
    }
    Ok(value.to_owned())
}

fn ensure_unique_record_ids(
    kind: &str,
    records: &[BTreeMap<String, String>],
) -> Result<(), CatalogError> {
    let mut seen = BTreeSet::new();
    for (index, record) in records.iter().enumerate() {
        let Some(id) = record.get("id") else {
            return Err(CatalogError(format!(
                "{kind} record {} is missing required field id",
                index + 1
            )));
        };
        if !seen.insert(id) {
            return Err(CatalogError(format!(
                "duplicate {kind} ID {id} in record {}",
                index + 1
            )));
        }
    }
    Ok(())
}

fn section_fields(section: &str) -> &'static [&'static str] {
    match section {
        "invariants:" => &[
            "kind",
            "family",
            "tier",
            "priority",
            "title",
            "statement",
            "scope",
            "assumptions",
            "action_class",
            "next_action",
        ],
        "clauses:" => &[
            "invariant_id",
            "statement",
            "scope",
            "assumptions",
            "required",
        ],
        "evidence:" => &[
            "clauses",
            "layer",
            "strength",
            "path",
            "symbol",
            "atomic_group",
            "package",
            "target_kind",
            "target",
            "test_name",
            "simulator_check",
            "minimum_protocol_states",
            "minimum_verifier_states",
            "minimum_runs_per_check",
            "minimum_steps",
            "required_observation",
            "minimum_observation",
            "negative_fixture",
            "negative_fixture_path",
            "negative_fixture_detector",
            "negative_fixture_package",
            "negative_fixture_target_kind",
            "negative_fixture_target",
            "negative_fixture_test_name",
            "negative_fixture_exemption",
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
        ],
        _ => &[],
    }
}

fn nested_fields(section: &str, field: &str) -> Option<&'static [&'static str]> {
    match (section, field) {
        ("invariants:", "current_coverage") => Some(&["tla", "simulator", "tests", "maelstrom"]),
        _ => None,
    }
}

fn is_record_section(line: &str) -> bool {
    matches!(line, "evidence:" | "clauses:" | "invariants:")
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, path::PathBuf};

    use super::{parse_clauses, parse_evidence, parse_invariants, parse_registry_schema_version};

    const VALID_INVARIANT: &str = r#"invariants:
  - id: "AA-01"
    kind: "safety"
    family: "test"
    tier: "feature"
    priority: "p1"
    title: "Test invariant"
    statement: "The statement holds."
    scope: "Test scope."
    assumptions: "Test assumptions."
    current_coverage:
      tla: "none"
      simulator: "direct"
      tests: "direct"
      maelstrom: "none"
    action_class: "retain"
    next_action: "Keep testing."
"#;

    const VALID_EVIDENCE: &str = r#"evidence:
  - id: "AA-01"
    clauses: "AA-01.a"
    layer: "tests"
    strength: "direct"
    path: "src/lib.rs"
    symbol: "test_symbol"
    package: "test-package"
    target_kind: "lib"
    target: "test_package"
    test_name: "tests::test_symbol"
"#;

    const VALID_ATOMIC_SIMULATOR_EVIDENCE: &str = r#"evidence:
  - id: "CM-03"
    clauses: "CM-03.a,CM-03.b"
    layer: "simulator"
    strength: "direct"
    path: "src/model.rs"
    symbol: "check_atomic_rule"
    atomic_group: "CM-03/current-term-commit-point"
    simulator_check: "model-check"
    minimum_protocol_states: "1"
    minimum_verifier_states: "1"
    required_observation: "atomic_rule_checks"
    minimum_observation: "1"
    negative_fixture: "atomic_rule_rejects_mutation"
    negative_fixture_path: "src/model/tests.rs"
    negative_fixture_detector: "check_atomic_rule"
    negative_fixture_package: "test-package"
    negative_fixture_target_kind: "lib"
    negative_fixture_target: "test_package"
    negative_fixture_test_name: "tests::atomic_rule_rejects_mutation"
"#;

    #[test]
    fn current_registry_parses_as_exactly_44_unique_invariants() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("verification/raft-invariants.yaml");
        let source = fs::read_to_string(path).expect("read current registry");

        assert_eq!(parse_registry_schema_version(&source).unwrap(), 3);
        let (invariants, _) = parse_invariants(&source).expect("parse current invariants");
        assert_eq!(invariants.len(), 44);
        assert_eq!(
            invariants
                .iter()
                .map(|invariant| invariant.id.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            44
        );
        parse_clauses(&source).expect("parse current clauses");
        parse_evidence(&source).expect("parse current evidence");
    }

    #[test]
    fn malformed_invariant_additions_cannot_disappear() {
        let cases = [
            (
                "unknown field",
                VALID_INVARIANT.replace(
                    "    next_action: \"Keep testing.\"",
                    "    unknown_future_field: \"ignored before\"\n    next_action: \"Keep testing.\"",
                ),
            ),
            (
                "malformed field",
                VALID_INVARIANT.replace(
                    "    statement: \"The statement holds.\"",
                    "    statement \"The statement disappears.\"",
                ),
            ),
            (
                "unsupported indentation",
                VALID_INVARIANT.replace(
                    "    statement: \"The statement holds.\"",
                    "   statement: \"The statement disappears.\"",
                ),
            ),
            (
                "malformed record start",
                format!("{VALID_INVARIANT}  - statement: \"A future row\"\n"),
            ),
            (
                "unindented record start",
                format!("{VALID_INVARIANT}- id: \"AA-02\"\n"),
            ),
            (
                "malformed quoted value",
                VALID_INVARIANT.replace(
                    "    statement: \"The statement holds.\"",
                    "    statement: \"The statement disappears.",
                ),
            ),
            (
                "unquoted value",
                VALID_INVARIANT.replace(
                    "    statement: \"The statement holds.\"",
                    "    statement: The statement disappears.",
                ),
            ),
        ];

        for (case, source) in cases {
            assert!(
                parse_invariants(&source).is_err(),
                "{case} was silently accepted"
            );
        }
    }

    #[test]
    fn duplicate_fields_and_nested_fields_are_rejected() {
        let duplicate_statement = VALID_INVARIANT.replace(
            "    scope: \"Test scope.\"",
            "    statement: \"A replacement.\"\n    scope: \"Test scope.\"",
        );
        let duplicate_coverage = VALID_INVARIANT.replace(
            "      simulator: \"direct\"",
            "      tla: \"replacement\"\n      simulator: \"direct\"",
        );

        for source in [duplicate_statement, duplicate_coverage] {
            let error = parse_invariants(&source).expect_err("duplicate field must fail");
            assert!(error.to_string().contains("duplicate field"));
        }
    }

    #[test]
    fn malformed_evidence_rows_cannot_hide_behind_the_invariant_count() {
        parse_evidence(VALID_EVIDENCE).expect("control evidence parses");
        let cases = [
            VALID_EVIDENCE.replace(
                "    symbol: \"test_symbol\"",
                "    unsupported_binding: \"ignored before\"\n    symbol: \"test_symbol\"",
            ),
            format!("{VALID_EVIDENCE}  - layer: \"tests\"\n"),
            VALID_EVIDENCE.replace(
                "    symbol: \"test_symbol\"",
                "    path: \"replacement.rs\"\n    symbol: \"test_symbol\"",
            ),
        ];

        for source in cases {
            assert!(
                parse_evidence(&source).is_err(),
                "malformed evidence was silently accepted"
            );
        }
    }

    #[test]
    fn multi_clause_simulator_evidence_requires_a_qualified_atomic_group() {
        let parsed =
            parse_evidence(VALID_ATOMIC_SIMULATOR_EVIDENCE).expect("qualified atomic group parses");
        assert_eq!(parsed.len(), 2);
        assert!(parsed
            .iter()
            .all(|descriptor| descriptor.atomic_group.as_deref()
                == Some("CM-03/current-term-commit-point")));

        let cases = [
            VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
                "    atomic_group: \"CM-03/current-term-commit-point\"\n",
                "",
            ),
            VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
                "    clauses: \"CM-03.a,CM-03.b\"",
                "    clauses: \"CM-03.a\"",
            ),
            VALID_ATOMIC_SIMULATOR_EVIDENCE
                .replace("    negative_fixture_detector: \"check_atomic_rule\"\n", ""),
            VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
                "    atomic_group: \"CM-03/current-term-commit-point\"",
                "    atomic_group: \"other/atomic-rule\"",
            ),
            VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
                "    atomic_group: \"CM-03/current-term-commit-point\"",
                "    atomic_group: \"CM-03/unreviewed-rule\"",
            ),
        ];
        for source in cases {
            assert!(
                parse_evidence(&source).is_err(),
                "invalid atomic group was accepted"
            );
        }
    }

    #[test]
    fn duplicate_invariant_and_clause_ids_are_rejected() {
        let duplicate_invariant = format!(
            "{VALID_INVARIANT}{}",
            VALID_INVARIANT.trim_start_matches("invariants:\n")
        );
        let duplicate_clause = r#"clauses:
  - id: "AA-01.a"
    invariant_id: "AA-01"
    statement: "First."
    scope: "Scope."
    assumptions: "Assumptions."
    required: "true"
  - id: "AA-01.a"
    invariant_id: "AA-01"
    statement: "Second."
    scope: "Scope."
    assumptions: "Assumptions."
    required: "true"
"#;

        assert!(parse_invariants(&duplicate_invariant)
            .expect_err("duplicate invariant ID must fail")
            .to_string()
            .contains("duplicate invariant ID"));
        assert!(parse_clauses(duplicate_clause)
            .expect_err("duplicate clause ID must fail")
            .to_string()
            .contains("duplicate clause ID"));
    }

    #[test]
    fn duplicate_or_malformed_schema_version_is_rejected() {
        for source in [
            "schema_version: 2\nschema_version: 2\n",
            "schema_version:2\n",
            "schema_version: \"unterminated\n",
        ] {
            assert!(parse_registry_schema_version(source).is_err());
        }
    }
}
