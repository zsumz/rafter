//! Exact process-log envelopes and machine-readable process reports.

use std::error::Error;

use crate::evidence::ArtifactRef;
use crate::{evidence::limits::MAX_ARTIFACT_BYTES, execution::filesystem::read_file_bounded};

use super::{
    model::{ProcessExitReport, ProcessReport, ProcessResourceReport, ProcessTerminationReport},
    publisher::ReplayArtifactPublisher,
};
use crate::verification::detector_replay::process::{
    ReplayProcessOutput, RetainedProcessDiagnostics,
};

pub(super) fn reports<'a, const N: usize>(
    publisher: &ReplayArtifactPublisher,
    processes: [(&'a str, Option<&'a ReplayProcessOutput>); N],
    artifacts: &mut Vec<ArtifactRef>,
) -> Result<Vec<ProcessReport>, Box<dyn Error>> {
    let mut reports = Vec::new();
    for (role, output) in processes {
        if let Some(output) = output {
            let report = report(publisher, role, role, output)?;
            artifacts.extend(report.logs().iter().cloned());
            reports.push(report);
        }
    }
    Ok(reports)
}

pub(super) fn report(
    publisher: &ReplayArtifactPublisher,
    role: &str,
    execution_id: &str,
    output: &ReplayProcessOutput,
) -> Result<ProcessReport, Box<dyn Error>> {
    let logs = [
        ("stdout", output.stdout.as_slice()),
        ("stderr", output.stderr.as_slice()),
    ]
    .into_iter()
    .map(|(stream, bytes)| {
        publisher.capture(
            "verifier-replay-process-log",
            &log_envelope(role, execution_id, stream, bytes),
        )
    })
    .collect::<Result<Vec<_>, _>>()?;
    Ok(ProcessReport::Completed {
        role: role.to_owned(),
        execution_id: execution_id.to_owned(),
        exit: ProcessExitReport {
            success: output.status.success(),
            exit_code: output.status.code(),
            timed_out: output.timed_out,
        },
        resources: ProcessResourceReport {
            duration_ms: duration_millis(output.duration),
            peak_rss_kib: output.peak_rss_kib,
        },
        termination: ProcessTerminationReport {
            process_group: output.termination.process_group,
            term_signal_sent: output.termination.term_signal_sent,
            termination_grace_ms: duration_millis(output.termination.grace),
            kill_signal_sent: output.termination.kill_signal_sent,
        },
        logs,
    })
}

pub(super) fn lifecycle_error(
    publisher: &ReplayArtifactPublisher,
    role: &str,
    execution_id: &str,
    message: &str,
    diagnostics: &RetainedProcessDiagnostics,
) -> Result<ProcessReport, Box<dyn Error>> {
    let mut logs = Vec::new();
    for (stream, path) in [
        ("stdout", Some(&diagnostics.stdout)),
        ("stderr", Some(&diagnostics.stderr)),
        ("telemetry", diagnostics.telemetry.as_ref()),
    ] {
        let Some(path) = path else {
            continue;
        };
        let payload = match read_file_bounded(path, MAX_ARTIFACT_BYTES) {
            Ok(bytes) => bytes,
            Err(error) => format!(
                "retained {stream} diagnostics at {} could not be read: {error}",
                path.display()
            )
            .into_bytes(),
        };
        logs.push(publisher.capture(
            "verifier-replay-process-log",
            &log_envelope(role, execution_id, stream, &payload),
        )?);
    }
    Ok(ProcessReport::LifecycleError {
        role: role.to_owned(),
        execution_id: execution_id.to_owned(),
        message: message.to_owned(),
        logs,
    })
}

pub(super) fn fixture_execution_id(target: &super::model::TargetReport, test_name: &str) -> String {
    use sha2::{Digest as _, Sha256};

    let identity = format!(
        "{}\0{}\0{}\0{test_name}",
        target.package, target.kind, target.name
    );
    format!("detector-fixture:{:x}", Sha256::digest(identity))
}

fn log_envelope(role: &str, execution_id: &str, stream: &str, bytes: &[u8]) -> Vec<u8> {
    let header = format!(
        "rafter-verifier-process-log-v2\nrole:{role}\nexecution-id:{execution_id}\nstream:{stream}\npayload-bytes:{}\n\n",
        bytes.len()
    );
    let mut envelope = Vec::with_capacity(header.len().saturating_add(bytes.len()));
    envelope.extend_from_slice(header.as_bytes());
    envelope.extend_from_slice(bytes);
    envelope
}

fn duration_millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;
