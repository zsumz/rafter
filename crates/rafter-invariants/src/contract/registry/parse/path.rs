//! Canonical repository-relative path parsing for registry bindings.

use std::path::Path;

use crate::contract::registry::RegistryParseError;

pub(super) fn parse_repository_path(
    index: usize,
    field: &str,
    value: &str,
) -> Result<String, RegistryParseError> {
    if value.trim().is_empty()
        || Path::new(value).is_absolute()
        || has_windows_prefix(value)
        || value.contains('\\')
        || value.contains('\0')
        || value.split('/').any(is_noncanonical_component)
    {
        return Err(RegistryParseError(format!(
            "evidence record {} has non-canonical repository-relative {field} {value:?}",
            index + 1
        )));
    }
    Ok(value.to_owned())
}

fn has_windows_prefix(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn is_noncanonical_component(component: &str) -> bool {
    component.is_empty() || matches!(component, "." | "..")
}
