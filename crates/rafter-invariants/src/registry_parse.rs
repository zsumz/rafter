use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::{CatalogError, EvidenceDescriptor, TestIdentity};

pub(super) fn parse_invariants(
    source: &str,
) -> Result<(Vec<String>, BTreeSet<String>), CatalogError> {
    let records = parse_section_records(source, "invariants:");
    let ids = records
        .iter()
        .filter_map(|record| record.get("id").cloned())
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Err(CatalogError(
            "registry contains no invariant IDs".to_owned(),
        ));
    }
    let canonical_ids = records
        .iter()
        .filter(|record| record.get("tier").is_some_and(|tier| tier == "canonical"))
        .filter_map(|record| record.get("id").cloned())
        .collect();
    Ok((ids, canonical_ids))
}

pub(super) fn parse_evidence(source: &str) -> Result<Vec<EvidenceDescriptor>, CatalogError> {
    parse_section_records(source, "evidence:")
        .into_iter()
        .enumerate()
        .map(|(index, record)| parse_evidence_record(index, &record))
        .collect()
}

fn parse_evidence_record(
    index: usize,
    record: &BTreeMap<String, String>,
) -> Result<EvidenceDescriptor, CatalogError> {
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
    Ok(EvidenceDescriptor {
        invariant_id: required("id")?,
        layer,
        strength: required("strength")?,
        path: required("path")?,
        symbol: required("symbol")?,
        negative_fixture: record.get("negative_fixture").cloned(),
        test,
    })
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
