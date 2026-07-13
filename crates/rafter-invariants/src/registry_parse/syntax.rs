use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::CatalogError;

use super::fields::{nested_fields, section_fields};

pub(super) fn parse_section_records(
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

pub(super) fn parse_field<'a>(
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

pub(super) fn insert_field(
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

pub(super) fn parse_scalar(
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

pub(super) fn ensure_unique_record_ids(
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

pub(super) fn required_field<'a>(
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

pub(super) fn parse_usize(value: &str) -> Result<usize, CatalogError> {
    value
        .parse()
        .map_err(|error| CatalogError(format!("invalid integer {value}: {error}")))
}

pub(super) fn parse_u64(value: &str) -> Result<u64, CatalogError> {
    value
        .parse()
        .map_err(|error| CatalogError(format!("invalid integer {value}: {error}")))
}

pub(super) fn parse_bool(value: &str) -> Result<bool, CatalogError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(CatalogError(format!("invalid boolean {value}"))),
    }
}

pub(super) fn parse_optional_bool(value: &str) -> Result<Option<bool>, CatalogError> {
    match value {
        "none" => Ok(None),
        _ => parse_bool(value).map(Some),
    }
}

pub(super) fn parse_optional_u64(value: &str) -> Result<Option<u64>, CatalogError> {
    match value {
        "none" => Ok(None),
        _ => parse_u64(value).map(Some),
    }
}

pub(super) fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn unsupported_line(section: &str, line_number: usize, raw_line: &str) -> CatalogError {
    CatalogError(format!(
        "unsupported syntax in {section} at line {line_number}: {}",
        raw_line.trim()
    ))
}

fn is_record_section(line: &str) -> bool {
    matches!(line, "evidence:" | "clauses:" | "invariants:")
}
