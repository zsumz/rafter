//! UTF-8, length-framed combined process transcript encoding and decoding.

use std::str;

use super::{
    error::{validate_label, ProcessFormatError},
    version::is_combined_process_schema,
    LabeledProcess, ProcessMetrics, ProcessObservation, COMBINED_PROCESS_SCHEMA_VERSION,
    DETECTOR_PROCESS_SCHEMA_VERSION,
};

pub(crate) fn encode_combined_v3(
    label: &str,
    observation: ProcessObservation<'_>,
) -> Result<Vec<u8>, ProcessFormatError> {
    encode_combined(COMBINED_PROCESS_SCHEMA_VERSION, label, observation, None)
}

pub(crate) fn encode_detector_v4(
    label: &str,
    observation: ProcessObservation<'_>,
    detector_challenge: &str,
) -> Result<Vec<u8>, ProcessFormatError> {
    if !valid_detector_challenge(detector_challenge) {
        return Err(ProcessFormatError::InvalidDetectorChallenge);
    }
    encode_combined(
        DETECTOR_PROCESS_SCHEMA_VERSION,
        label,
        observation,
        Some(detector_challenge),
    )
}

fn encode_combined(
    schema_version: u32,
    label: &str,
    observation: ProcessObservation<'_>,
    detector_challenge: Option<&str>,
) -> Result<Vec<u8>, ProcessFormatError> {
    validate_label(label)?;
    if observation.termination.is_some() {
        return Err(ProcessFormatError::UnexpectedTermination);
    }
    let invocation = serde_json::to_string(observation.invocation)?;
    let stdout = str::from_utf8(observation.stdout)?;
    let stderr = str::from_utf8(observation.stderr)?;
    let detector_header = detector_challenge
        .map(|challenge| format!("detector_challenge: {challenge}\n"))
        .unwrap_or_default();
    Ok(format!(
        "schema_version: {schema_version}\nlabel: {label}\ninvocation: {invocation}\n{detector_header}exit_code: {:?}\ntimed_out: {}\nduration_ms: {}\npeak_rss_kib: {}\nstdout_bytes: {}\nstderr_bytes: {}\n\n{}{}",
        observation.exit_code,
        observation.timed_out,
        observation.duration_ms,
        observation.peak_rss_kib,
        stdout.len(),
        stderr.len(),
        stdout,
        stderr,
    )
    .into_bytes())
}

pub(crate) fn parse_combined_v3(source: &str) -> Result<Vec<LabeledProcess>, ProcessFormatError> {
    let processes = parse_combined_processes(source)?;
    if let Some(process) = processes
        .iter()
        .find(|process| process.schema_version != COMBINED_PROCESS_SCHEMA_VERSION)
    {
        return Err(ProcessFormatError::UnsupportedSchema {
            expected: COMBINED_PROCESS_SCHEMA_VERSION,
            observed: process.schema_version,
        });
    }
    Ok(processes)
}

pub(crate) fn parse_combined_processes(
    source: &str,
) -> Result<Vec<LabeledProcess>, ProcessFormatError> {
    let mut remaining = source;
    let mut processes = Vec::new();
    while !remaining.is_empty() {
        let (header, payload) =
            remaining
                .split_once("\n\n")
                .ok_or(ProcessFormatError::MalformedCombinedHeader(
                    "framed payload",
                ))?;
        let header = parse_header(header)?;
        let (stdout, stderr, next) =
            parse_payload(payload, header.stdout_bytes, header.stderr_bytes)?;
        remaining = next;
        processes.push(header.into_process(stdout, stderr));
    }
    if processes.is_empty() {
        return Err(ProcessFormatError::EmptyTranscript);
    }
    Ok(processes)
}

struct CombinedHeader {
    schema_version: u32,
    label: String,
    invocation: crate::evidence::InvocationReceipt,
    exit_code: Option<i32>,
    timed_out: bool,
    duration_ms: u64,
    peak_rss_kib: u64,
    stdout_bytes: u64,
    stderr_bytes: u64,
    detector_challenge: Option<String>,
}

impl CombinedHeader {
    fn into_process(self, stdout: &str, stderr: &str) -> LabeledProcess {
        LabeledProcess {
            schema_version: self.schema_version,
            label: self.label,
            invocation: self.invocation,
            exit_code: self.exit_code,
            timed_out: self.timed_out,
            metrics: ProcessMetrics {
                duration_ms: self.duration_ms,
                peak_rss_kib: self.peak_rss_kib,
            },
            stdout: stdout.to_owned(),
            stderr: stderr.to_owned(),
            detector_challenge: self.detector_challenge,
        }
    }
}

fn parse_header(source: &str) -> Result<CombinedHeader, ProcessFormatError> {
    let mut lines = source.lines();
    let schema_version = lines
        .next()
        .and_then(|line| line.strip_prefix("schema_version: "))
        .ok_or(ProcessFormatError::MalformedCombinedHeader(
            "schema version",
        ))?
        .parse::<u32>()
        .map_err(|_| ProcessFormatError::MalformedCombinedHeader("schema version"))?;
    if !is_combined_process_schema(schema_version) {
        return Err(ProcessFormatError::UnsupportedCombinedSchema(
            schema_version,
        ));
    }
    let label = lines
        .next()
        .and_then(|line| line.strip_prefix("label: "))
        .ok_or(ProcessFormatError::MalformedCombinedHeader("label"))?
        .to_owned();
    validate_label(&label)?;
    let invocation = lines
        .next()
        .and_then(|line| line.strip_prefix("invocation: "))
        .ok_or(ProcessFormatError::MalformedCombinedHeader("invocation"))?;
    let detector_challenge = parse_detector_challenge(schema_version, &mut lines)?;
    let exit_code = lines
        .next()
        .and_then(|line| line.strip_prefix("exit_code: "))
        .ok_or(ProcessFormatError::MalformedCombinedHeader("exit code"))?;
    let timed_out = lines
        .next()
        .and_then(|line| line.strip_prefix("timed_out: "))
        .ok_or(ProcessFormatError::MalformedCombinedHeader(
            "timeout status",
        ))?
        .parse::<bool>()
        .map_err(|_| ProcessFormatError::InvalidTimeout)?;
    let header = CombinedHeader {
        schema_version,
        label,
        invocation: serde_json::from_str(invocation)?,
        exit_code: parse_exit_code(exit_code)?,
        timed_out,
        duration_ms: metric_line(&mut lines, "duration_ms: ")?,
        peak_rss_kib: metric_line(&mut lines, "peak_rss_kib: ")?,
        stdout_bytes: metric_line(&mut lines, "stdout_bytes: ")?,
        stderr_bytes: metric_line(&mut lines, "stderr_bytes: ")?,
        detector_challenge,
    };
    if lines.next().is_some() {
        return Err(ProcessFormatError::MalformedCombinedHeader(
            "header field order",
        ));
    }
    if header.peak_rss_kib == 0 {
        return Err(ProcessFormatError::ZeroPeakRss);
    }
    Ok(header)
}

fn parse_detector_challenge<'a>(
    schema_version: u32,
    lines: &mut impl Iterator<Item = &'a str>,
) -> Result<Option<String>, ProcessFormatError> {
    if schema_version != DETECTOR_PROCESS_SCHEMA_VERSION {
        return Ok(None);
    }
    lines
        .next()
        .and_then(|line| line.strip_prefix("detector_challenge: "))
        .filter(|challenge| valid_detector_challenge(challenge))
        .map(str::to_owned)
        .map(Some)
        .ok_or(ProcessFormatError::InvalidDetectorChallenge)
}

fn parse_payload(
    payload: &str,
    stdout_bytes: u64,
    stderr_bytes: u64,
) -> Result<(&str, &str, &str), ProcessFormatError> {
    let stdout_bytes =
        usize::try_from(stdout_bytes).map_err(|_| ProcessFormatError::PayloadLengthOverflow)?;
    let stderr_bytes =
        usize::try_from(stderr_bytes).map_err(|_| ProcessFormatError::PayloadLengthOverflow)?;
    let payload_bytes = stdout_bytes
        .checked_add(stderr_bytes)
        .ok_or(ProcessFormatError::PayloadLengthOverflow)?;
    let framed = payload
        .get(..payload_bytes)
        .ok_or(ProcessFormatError::TruncatedPayload)?;
    let stdout = framed
        .get(..stdout_bytes)
        .ok_or(ProcessFormatError::InvalidUtf8Boundary("stdout"))?;
    let stderr = framed
        .get(stdout_bytes..)
        .ok_or(ProcessFormatError::InvalidUtf8Boundary("stderr"))?;
    let remaining = payload
        .get(payload_bytes..)
        .ok_or(ProcessFormatError::TruncatedPayload)?;
    Ok((stdout, stderr, remaining))
}

fn valid_detector_challenge(challenge: &str) -> bool {
    challenge.len() == 64
        && challenge
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn parse_exit_code(source: &str) -> Result<Option<i32>, ProcessFormatError> {
    if source == "None" {
        return Ok(None);
    }
    source
        .strip_prefix("Some(")
        .and_then(|value| value.strip_suffix(')'))
        .ok_or(ProcessFormatError::InvalidExitCode)?
        .parse::<i32>()
        .map(Some)
        .map_err(|_| ProcessFormatError::InvalidExitCode)
}

fn metric_line<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    prefix: &'static str,
) -> Result<u64, ProcessFormatError> {
    lines
        .next()
        .and_then(|line| line.strip_prefix(prefix))
        .ok_or(ProcessFormatError::InvalidMetric(prefix))?
        .parse()
        .map_err(|_| ProcessFormatError::InvalidMetric(prefix))
}
