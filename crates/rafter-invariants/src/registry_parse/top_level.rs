use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::CatalogError;

use super::syntax::{insert_field, parse_field, parse_scalar, parse_usize};

pub(super) struct ParsedTopLevel {
    pub(super) metadata: BTreeMap<String, String>,
    pub(super) counts: BTreeMap<String, usize>,
}

pub(super) fn parse_top_level(source: &str) -> Result<ParsedTopLevel, CatalogError> {
    let mut metadata = BTreeMap::new();
    let mut counts = BTreeMap::new();
    let mut sections = BTreeSet::new();
    let mut active_section = None::<&str>;

    for (index, raw_line) in source.lines().enumerate() {
        let line_number = index + 1;
        if raw_line.contains('\t') {
            return Err(CatalogError(format!(
                "registry contains a tab at line {line_number}"
            )));
        }
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let indent = raw_line.len() - raw_line.trim_start_matches(' ').len();
        if indent == 0 {
            if let Some(section) = line.strip_suffix(':') {
                if !matches!(section, "counting" | "evidence" | "clauses" | "invariants") {
                    return Err(CatalogError(format!(
                        "unsupported top-level section {section} at line {line_number}"
                    )));
                }
                if !sections.insert(section.to_owned()) {
                    return Err(CatalogError(format!(
                        "registry has duplicate {section}: section at line {line_number}"
                    )));
                }
                active_section = Some(section);
                continue;
            }

            let (key, value) = parse_field(line, "registry", line_number)?;
            if !matches!(
                key,
                "schema_version"
                    | "repository"
                    | "catalog_origin_ref"
                    | "catalog_working_ref"
                    | "audit_date"
                    | "document"
                    | "scope"
            ) {
                return Err(CatalogError(format!(
                    "unsupported top-level field {key} at line {line_number}"
                )));
            }
            let value = if key == "schema_version" {
                parse_unquoted_integer(value, line_number, key)?
            } else {
                parse_scalar(value, line_number, key, true)?
            };
            insert_field(&mut metadata, key, value, "registry", line_number)?;
            active_section = None;
            continue;
        }

        match active_section {
            Some("counting") if indent == 2 => {
                let (key, value) = parse_field(line, "counting:", line_number)?;
                if !matches!(
                    key,
                    "canonical_raft_safety_properties"
                        | "tla_predicates_now"
                        | "well_formedness_meta_invariants"
                        | "semantic_safety_invariants"
                        | "liveness_obligations"
                        | "total_entries"
                ) {
                    return Err(CatalogError(format!(
                        "unsupported field {key} in counting: at line {line_number}"
                    )));
                }
                let value = parse_unquoted_integer(value, line_number, key)?;
                let value = parse_usize(&value)?;
                if counts.insert(key.to_owned(), value).is_some() {
                    return Err(CatalogError(format!(
                        "duplicate field {key} in counting: at line {line_number}"
                    )));
                }
            }
            Some("evidence" | "clauses" | "invariants") if matches!(indent, 2 | 4 | 6) => {}
            Some(section) => {
                return Err(CatalogError(format!(
                    "unsupported syntax in {section}: at line {line_number}: {line}"
                )));
            }
            None => {
                return Err(CatalogError(format!(
                    "indented field outside a top-level section at line {line_number}: {line}"
                )));
            }
        }
    }

    for section in ["counting", "evidence", "clauses", "invariants"] {
        if !sections.contains(section) {
            return Err(CatalogError(format!(
                "registry is missing {section}: section"
            )));
        }
    }
    Ok(ParsedTopLevel { metadata, counts })
}

pub(super) fn required_top_level<'a>(
    metadata: &'a BTreeMap<String, String>,
    field: &str,
) -> Result<&'a str, CatalogError> {
    metadata.get(field).map(String::as_str).ok_or_else(|| {
        CatalogError(format!(
            "registry is missing required top-level field {field}"
        ))
    })
}

pub(super) fn required_count(
    counts: &BTreeMap<String, usize>,
    field: &str,
) -> Result<usize, CatalogError> {
    counts
        .get(field)
        .copied()
        .ok_or_else(|| CatalogError(format!("registry is missing count {field}")))
}

pub(super) fn parse_u32(value: &str) -> Result<u32, CatalogError> {
    value
        .parse()
        .map_err(|error| CatalogError(format!("invalid schema version {value}: {error}")))
}

fn parse_unquoted_integer(
    value: &str,
    line_number: usize,
    field: &str,
) -> Result<String, CatalogError> {
    let value = value.trim();
    if value.is_empty() || !value.chars().all(|character| character.is_ascii_digit()) {
        return Err(CatalogError(format!(
            "field {field} must use an unquoted nonnegative integer at line {line_number}"
        )));
    }
    Ok(value.to_owned())
}
