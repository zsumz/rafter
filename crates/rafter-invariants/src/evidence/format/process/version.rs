//! Reviewed process evidence schema versions.

pub(crate) const MAELSTROM_PROCESS_SCHEMA_VERSION: u32 = 2;
pub(crate) const TLA_PROCESS_SCHEMA_VERSION: u32 = 3;
pub(crate) const COMBINED_PROCESS_SCHEMA_VERSION: u32 = 3;
pub(crate) const DETECTOR_PROCESS_SCHEMA_VERSION: u32 = 4;

pub(super) fn is_combined_process_schema(version: u32) -> bool {
    matches!(
        version,
        COMBINED_PROCESS_SCHEMA_VERSION | DETECTOR_PROCESS_SCHEMA_VERSION
    )
}
