//! Version-specific JSON codecs for Maelstrom and TLA+ process evidence.

use std::str;

use super::{
    error::{validate_label, ProcessFormatError},
    ProcessLog, ProcessObservation, MAELSTROM_PROCESS_SCHEMA_VERSION, TLA_PROCESS_SCHEMA_VERSION,
};

pub(crate) fn encode_maelstrom_v3(
    label: &str,
    observation: ProcessObservation<'_>,
) -> Result<Vec<u8>, ProcessFormatError> {
    if observation.termination.is_some() {
        return Err(ProcessFormatError::UnexpectedTermination);
    }
    encode_structured(MAELSTROM_PROCESS_SCHEMA_VERSION, label, observation, None)
}

pub(crate) fn encode_tla_v4(
    label: &str,
    observation: ProcessObservation<'_>,
) -> Result<Vec<u8>, ProcessFormatError> {
    let termination = observation
        .termination
        .cloned()
        .ok_or(ProcessFormatError::MissingTermination)?;
    encode_structured(
        TLA_PROCESS_SCHEMA_VERSION,
        label,
        observation,
        Some(termination),
    )
}

pub(crate) fn parse_maelstrom_v3(source: &str) -> Result<ProcessLog, ProcessFormatError> {
    let log = parse_version(source, MAELSTROM_PROCESS_SCHEMA_VERSION)?;
    if log.termination.is_some() {
        return Err(ProcessFormatError::UnexpectedTermination);
    }
    Ok(log)
}

pub(crate) fn parse_tla_v4(source: &str) -> Result<ProcessLog, ProcessFormatError> {
    let log = parse_version(source, TLA_PROCESS_SCHEMA_VERSION)?;
    if log.termination.is_none() {
        return Err(ProcessFormatError::MissingTermination);
    }
    Ok(log)
}

fn encode_structured(
    schema_version: u32,
    label: &str,
    observation: ProcessObservation<'_>,
    termination: Option<super::TerminationReceipt>,
) -> Result<Vec<u8>, ProcessFormatError> {
    validate_label(label)?;
    let log = ProcessLog {
        schema_version,
        label: label.to_owned(),
        invocation: observation.invocation.clone(),
        exit_code: observation.exit_code,
        timed_out: observation.timed_out,
        termination,
        duration_ms: observation.duration_ms,
        peak_rss_kib: observation.peak_rss_kib,
        stdout: str::from_utf8(observation.stdout)?.to_owned(),
        stderr: str::from_utf8(observation.stderr)?.to_owned(),
    };
    Ok(serde_json::to_vec_pretty(&log)?)
}

fn parse_version(source: &str, expected: u32) -> Result<ProcessLog, ProcessFormatError> {
    let log: ProcessLog = serde_json::from_str(source)?;
    if log.schema_version != expected {
        return Err(ProcessFormatError::UnsupportedSchema {
            expected,
            observed: log.schema_version,
        });
    }
    validate_label(&log.label)?;
    Ok(log)
}
