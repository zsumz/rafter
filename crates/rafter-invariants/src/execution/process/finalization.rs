//! Deadline- and size-bounded receipt reads with immutable replay retention.

use std::{error::Error, time::Instant};

use crate::execution::filesystem::OperationDeadline;

use super::{
    measurement_error, parse_peak_rss, retained_error, retained_result, FinalizationPolicy,
    ProcessArtifacts, ProcessCompletion, ProcessOutput, ProcessTermination, TerminationPolicy,
};

#[derive(Debug)]
pub(crate) struct PendingProcessOutput {
    output: ProcessOutput,
    artifacts: ProcessArtifacts,
}

impl PendingProcessOutput {
    pub(crate) fn retained_error(&self, error: impl std::fmt::Display) -> Box<dyn Error> {
        retained_error(
            error,
            &self.artifacts.stdout_path(),
            &self.artifacts.stderr_path(),
            Some(&self.artifacts.resource_path()),
        )
    }

    pub(crate) fn finalize(self) -> Result<ProcessOutput, Box<dyn Error>> {
        let Self { output, artifacts } = self;
        artifacts.verify_path_bindings().map_err(|error| {
            retained_error(
                error,
                &artifacts.stdout_path(),
                &artifacts.stderr_path(),
                Some(&artifacts.resource_path()),
            )
        })?;
        Ok(output)
    }

    #[cfg(test)]
    pub(crate) fn artifact_paths(&self) -> super::artifacts::ProcessArtifactPaths {
        self.artifacts.path_snapshot()
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn finalize_process_output(
    started: Instant,
    termination_policy: TerminationPolicy,
    finalization_policy: FinalizationPolicy,
    mut peak_rss_kib: u64,
    completion: ProcessCompletion,
    lifecycle_deadline: Instant,
    artifacts: &ProcessArtifacts,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let phase_deadline = Instant::now()
        .checked_add(finalization_policy.timeout)
        .ok_or("process finalization deadline overflow")?;
    let deadline = OperationDeadline::at(
        phase_deadline.min(lifecycle_deadline),
        "process receipt finalization",
    );
    let stdout_path = artifacts.stdout_path();
    let stderr_path = artifacts.stderr_path();
    let resource_path = artifacts.resource_path();
    let read = |result| retained_result(result, &stdout_path, &stderr_path, Some(&resource_path));
    let stdout = read(artifacts.read_stdout(deadline, finalization_policy.stdout_max_bytes))?;
    let stderr = read(artifacts.read_stderr(deadline, finalization_policy.stderr_max_bytes))?;
    let resource_telemetry =
        read(artifacts.read_resource(deadline, finalization_policy.telemetry_max_bytes))?;
    let authoritative_peak = parse_peak_rss(&resource_telemetry).ok_or_else(|| {
        measurement_error(
            "/usr/bin/time did not report maximum resident set size",
            &stdout_path,
            &stderr_path,
            &resource_path,
        )
    })?;
    peak_rss_kib = peak_rss_kib.max(authoritative_peak);
    if peak_rss_kib == 0 {
        return Err(measurement_error(
            "resource measurement did not observe the process group",
            &stdout_path,
            &stderr_path,
            &resource_path,
        ));
    }
    Ok(ProcessOutput {
        status: completion.status,
        stdout,
        stderr,
        duration: started.elapsed(),
        peak_rss_kib,
        timed_out: completion.timed_out,
        termination: ProcessTermination {
            process_group: true,
            term_signal_sent: completion.term_signal_sent,
            grace: termination_policy.grace,
            kill_signal_sent: completion.kill_signal_sent,
        },
    })
}

impl PendingProcessOutput {
    pub(super) fn new(output: ProcessOutput, artifacts: ProcessArtifacts) -> Self {
        Self { output, artifacts }
    }
}
