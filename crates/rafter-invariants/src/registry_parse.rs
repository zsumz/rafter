use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::{
    CatalogError, ClauseDescriptor, EvidenceDescriptor, InvariantDescriptor, SimulatorIdentity,
    TestIdentity,
};

pub(super) fn parse_registry_schema_version(source: &str) -> Result<u32, CatalogError> {
    source
        .lines()
        .find_map(|line| line.strip_prefix("schema_version: "))
        .ok_or_else(|| CatalogError("registry is missing schema_version".to_owned()))
        .and_then(|value| parse_u32(&yaml_value(value)))
}

pub(super) fn parse_invariants(
    source: &str,
) -> Result<(Vec<InvariantDescriptor>, BTreeSet<String>), CatalogError> {
    let records = parse_section_records(source, "invariants:");
    if records.is_empty() {
        return Err(CatalogError(
            "registry contains no invariant IDs".to_owned(),
        ));
    }
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
    parse_section_records(source, "clauses:")
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
    parse_section_records(source, "evidence:")
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
            negative_fixture: record.get("negative_fixture").cloned(),
            test: test.clone(),
            simulator: simulator.clone(),
        })
        .collect())
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
    let identity = SimulatorIdentity {
        checks,
        required_observation: required("required_observation")?,
        minimum_observation: parse_usize(&required("minimum_observation")?)?,
        minimum_protocol_states: optional_usize(record, "minimum_protocol_states")?,
        minimum_verifier_states: optional_usize(record, "minimum_verifier_states")?,
        minimum_runs_per_check: optional_usize(record, "minimum_runs_per_check")?,
        minimum_steps: optional_usize(record, "minimum_steps")?,
        required_liveness_feature: record.get("required_liveness_feature").cloned(),
        negative_test,
    };
    let safety = identity.required_liveness_feature.is_none()
        && identity.minimum_protocol_states.is_some()
        && identity.minimum_verifier_states.is_some();
    let liveness = identity.required_liveness_feature.is_some()
        && identity.minimum_runs_per_check.is_some()
        && identity.minimum_steps.is_some();
    if identity.checks.is_empty() || identity.minimum_observation == 0 || !(safety || liveness) {
        return Err(CatalogError(
            "simulator evidence has an incomplete execution contract".to_owned(),
        ));
    }
    Ok(identity)
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

fn parse_section_records(source: &str, section: &str) -> Vec<BTreeMap<String, String>> {
    let mut records = Vec::new();
    let mut current = None::<BTreeMap<String, String>>;
    let mut active = false;
    for raw_line in source.lines() {
        let indent = raw_line.chars().take_while(|ch| *ch == ' ').count();
        let line = raw_line.trim();
        if indent == 0 {
            if active {
                if let Some(record) = current.take() {
                    records.push(record);
                }
            }
            active = line == section;
            continue;
        }
        if !active || line.is_empty() || line.starts_with('#') {
            continue;
        }
        if indent == 2 && line.starts_with("- id: ") {
            if let Some(record) = current.take() {
                records.push(record);
            }
            let mut record = BTreeMap::new();
            record.insert("id".to_owned(), yaml_value(&line[6..]));
            current = Some(record);
        } else if indent == 4 {
            if let Some((key, value)) = line.split_once(": ") {
                if let Some(record) = current.as_mut() {
                    record.insert(key.to_owned(), yaml_value(value));
                }
            }
        }
    }
    if active {
        if let Some(record) = current {
            records.push(record);
        }
    }
    records
}

fn yaml_value(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1].replace("\\\"", "\"")
    } else {
        value.to_owned()
    }
}
