//! Process receipt vocabulary and strict combined-transcript decoding.

mod combined;
mod error;
mod model;
mod structured;
mod version;

pub(crate) use combined::{
    encode_combined_v4, encode_detector_v5, parse_combined_processes, parse_combined_v4,
};
pub(crate) use error::ProcessFormatError;
pub(crate) use model::{
    LabeledProcess, ProcessLog, ProcessMetrics, ProcessObservation, TerminationReceipt,
};
pub(crate) use structured::{encode_maelstrom_v3, encode_tla_v4, parse_maelstrom_v3, parse_tla_v4};
pub(crate) use version::{
    COMBINED_PROCESS_SCHEMA_VERSION, DETECTOR_PROCESS_SCHEMA_VERSION,
    MAELSTROM_PROCESS_SCHEMA_VERSION, TLA_PROCESS_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
