//! Typed scalar and comma-separated-list parsing.

use crate::contract::registry::RegistryParseError;

pub(super) fn parse_usize(value: &str) -> Result<usize, RegistryParseError> {
    value
        .parse()
        .map_err(|error| RegistryParseError(format!("invalid integer {value}: {error}")))
}

pub(super) fn parse_u64(value: &str) -> Result<u64, RegistryParseError> {
    value
        .parse()
        .map_err(|error| RegistryParseError(format!("invalid integer {value}: {error}")))
}

pub(super) fn parse_bool(value: &str) -> Result<bool, RegistryParseError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(RegistryParseError(format!("invalid boolean {value}"))),
    }
}

pub(super) fn parse_optional_bool(value: &str) -> Result<Option<bool>, RegistryParseError> {
    match value {
        "none" => Ok(None),
        _ => parse_bool(value).map(Some),
    }
}

pub(super) fn parse_optional_u64(value: &str) -> Result<Option<u64>, RegistryParseError> {
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
