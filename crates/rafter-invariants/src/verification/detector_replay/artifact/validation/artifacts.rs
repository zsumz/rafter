//! Exact cross-links from report process records to archived log envelopes.

use std::{collections::BTreeMap, collections::BTreeSet, path::Path};

use sha2::{Digest, Sha256};

use crate::evidence::ArtifactRef;

use super::super::model::{ProcessReport, ReplayReport};

pub(super) fn validate(
    report: &ReplayReport,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<(), String> {
    let mut referenced = BTreeSet::new();
    for process in &report.compilation.processes {
        validate_process(process, files, &mut referenced)?;
    }
    for fixture in &report.fixtures {
        let Some(process) = &fixture.process else {
            continue;
        };
        let logs = validate_process(process, files, &mut referenced)?;
        if fixture.status
            == crate::verification::detector_replay::result::FixtureReplayStatus::Passed
        {
            crate::verification::qualify_detector_execution(
                logs.stdout,
                logs.stderr,
                &fixture.test_name,
                fixture
                    .token
                    .as_deref()
                    .ok_or_else(|| "passed fixture has no archived token".to_owned())?,
                fixture
                    .challenge
                    .as_deref()
                    .ok_or_else(|| "passed fixture has no archived challenge".to_owned())?,
                &fixture.source.expected_witnesses,
            )
            .map_err(|error| {
                format!(
                    "archived transcript does not qualify fixture {}: {error}",
                    fixture.test_name
                )
            })?;
        }
    }
    let archived_logs = files
        .keys()
        .filter(|name| name.starts_with("verifier-replay-process-log-"))
        .cloned()
        .collect::<BTreeSet<_>>();
    if referenced != archived_logs {
        return Err(
            "verifier archive process-log inventory is not exactly report-referenced".to_owned(),
        );
    }
    Ok(())
}

fn validate_process<'a>(
    process: &ProcessReport,
    files: &'a BTreeMap<String, Vec<u8>>,
    referenced: &mut BTreeSet<String>,
) -> Result<ValidatedLogs<'a>, String> {
    let (role, execution_id, logs, allowed_streams) = match process {
        ProcessReport::Completed {
            role,
            execution_id,
            logs,
            ..
        } => (
            role.as_str(),
            execution_id.as_str(),
            logs.as_slice(),
            &["stderr", "stdout"][..],
        ),
        ProcessReport::LifecycleError {
            role,
            execution_id,
            logs,
            ..
        } => (
            role.as_str(),
            execution_id.as_str(),
            logs.as_slice(),
            &["stderr", "stdout", "telemetry"][..],
        ),
    };
    let mut streams = BTreeSet::new();
    let mut stdout = None;
    let mut stderr = None;
    for artifact in logs {
        let name = artifact_name(artifact)?;
        let bytes = files
            .get(&name)
            .ok_or_else(|| format!("verifier archive omits report-referenced artifact {name}"))?;
        if bytes.len() as u64 != artifact.size_bytes
            || format!("{:x}", Sha256::digest(bytes)) != artifact.sha256
        {
            return Err(format!("report-referenced artifact {name} has changed"));
        }
        let (stream, payload) = validate_envelope(bytes, role, execution_id)?;
        if !allowed_streams.contains(&stream.as_str()) || streams.contains(stream.as_str()) {
            return Err(format!(
                "replay process {role} has an invalid log-stream inventory"
            ));
        }
        match stream.as_str() {
            "stdout" => stdout = Some(payload),
            "stderr" => stderr = Some(payload),
            _ => {}
        }
        streams.insert(stream);
        if !referenced.insert(name) {
            return Err("replay process log is referenced by more than one execution".to_owned());
        }
    }
    match (stdout, stderr) {
        (Some(stdout), Some(stderr)) => Ok(ValidatedLogs { stdout, stderr }),
        _ => Err(format!("replay process {role} omits stdout or stderr")),
    }
}

struct ValidatedLogs<'a> {
    stdout: &'a [u8],
    stderr: &'a [u8],
}

fn artifact_name(artifact: &ArtifactRef) -> Result<String, String> {
    if artifact.kind != "verifier-replay-process-log" {
        return Err("replay report references an unsupported artifact kind".to_owned());
    }
    let name = Path::new(&artifact.path)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "replay report artifact path has no canonical filename".to_owned())?;
    let expected = format!("{}-{}", artifact.kind, artifact.sha256);
    if name != expected {
        return Err("replay report artifact path is not content-addressed".to_owned());
    }
    Ok(name.to_owned())
}

fn validate_envelope<'a>(
    bytes: &'a [u8],
    expected_role: &str,
    expected_execution_id: &str,
) -> Result<(String, &'a [u8]), String> {
    let split = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .ok_or_else(|| "replay process log has no envelope terminator".to_owned())?;
    let header = std::str::from_utf8(&bytes[..split])
        .map_err(|_| "replay process log envelope is not UTF-8".to_owned())?;
    let mut lines = header.lines();
    if lines.next() != Some("rafter-verifier-process-log-v2") {
        return Err("replay process log uses an unsupported envelope".to_owned());
    }
    let role = lines
        .next()
        .and_then(|line| line.strip_prefix("role:"))
        .ok_or_else(|| "replay process log omits its role".to_owned())?;
    let execution_id = lines
        .next()
        .and_then(|line| line.strip_prefix("execution-id:"))
        .ok_or_else(|| "replay process log omits its execution identity".to_owned())?;
    let stream = lines
        .next()
        .and_then(|line| line.strip_prefix("stream:"))
        .ok_or_else(|| "replay process log omits its stream".to_owned())?;
    let payload_bytes = lines
        .next()
        .and_then(|line| line.strip_prefix("payload-bytes:"))
        .ok_or_else(|| "replay process log omits its payload length".to_owned())?
        .parse::<usize>()
        .map_err(|_| "replay process log payload length is invalid".to_owned())?;
    if lines.next().is_some()
        || role != expected_role
        || execution_id != expected_execution_id
        || bytes.len() - split - 2 != payload_bytes
    {
        return Err("replay process log envelope does not match its report".to_owned());
    }
    Ok((stream.to_owned(), &bytes[split + 2..]))
}

#[cfg(test)]
#[path = "artifacts_tests.rs"]
mod tests;
