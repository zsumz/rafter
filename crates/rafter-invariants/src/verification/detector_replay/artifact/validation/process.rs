//! Semantic validation for typed replay process observations.

use std::collections::BTreeSet;

use super::{
    super::model::{ProcessReport, PROCESS_TERMINATION_GRACE_MS},
    value,
};
use crate::evidence::ArtifactRef;

pub(super) fn validate_all(
    processes: &[ProcessReport],
    require_success: bool,
) -> Result<(), String> {
    let mut roles = BTreeSet::new();
    for process in processes {
        let role = role(process);
        value::require_nonempty(role, "process role")?;
        if !roles.insert(role) {
            return Err("replay report repeats a process role".to_owned());
        }
        validate(process, require_success)?;
    }
    if require_success && processes.is_empty() {
        return Err("passed replay compilation has no process reports".to_owned());
    }
    Ok(())
}

pub(super) fn role(process: &ProcessReport) -> &str {
    match process {
        ProcessReport::Completed { role, .. } | ProcessReport::LifecycleError { role, .. } => role,
    }
}

pub(super) fn execution_id(process: &ProcessReport) -> &str {
    match process {
        ProcessReport::Completed { execution_id, .. }
        | ProcessReport::LifecycleError { execution_id, .. } => execution_id,
    }
}

pub(super) fn duration_ms(process: &ProcessReport) -> Option<u64> {
    match process {
        ProcessReport::Completed { resources, .. } => Some(resources.duration_ms),
        ProcessReport::LifecycleError { .. } => None,
    }
}

pub(super) fn require_duration_at_most(
    process: &ProcessReport,
    maximum_ms: u64,
) -> Result<(), String> {
    if let Some(duration_ms) = duration_ms(process) {
        if duration_ms > maximum_ms {
            return Err(format!(
                "replay process duration {duration_ms}ms exceeds its {maximum_ms}ms phase budget"
            ));
        }
    }
    Ok(())
}

pub(super) fn validate(process: &ProcessReport, require_success: bool) -> Result<(), String> {
    let role = role(process);
    let execution_id = execution_id(process);
    value::require_nonempty(role, "process role")?;
    value::require_nonempty(execution_id, "process execution identity")?;
    if role.contains(['\r', '\n']) || execution_id.contains(['\r', '\n']) {
        return Err("replay process role or execution identity is not one line".to_owned());
    }
    let (logs, expected_count) = match process {
        ProcessReport::Completed {
            exit,
            resources,
            termination,
            logs,
            ..
        } => {
            if require_success && (!exit.success || exit.timed_out) {
                return Err("passed replay process did not exit successfully".to_owned());
            }
            if exit.success && (exit.timed_out || exit.exit_code != Some(0)) {
                return Err("successful replay process has inconsistent exit facts".to_owned());
            }
            if !exit.success && exit.exit_code == Some(0) {
                return Err("failed replay process has a successful exit code".to_owned());
            }
            if resources.peak_rss_kib == 0 {
                return Err("replay process has no peak-RSS observation".to_owned());
            }
            if !termination.process_group {
                return Err("replay process was not isolated in a process group".to_owned());
            }
            if termination.termination_grace_ms != PROCESS_TERMINATION_GRACE_MS {
                return Err("replay process termination grace differs from policy".to_owned());
            }
            if exit.success && (termination.term_signal_sent || termination.kill_signal_sent) {
                return Err("successful replay process carries termination signals".to_owned());
            }
            if termination.kill_signal_sent && !termination.term_signal_sent {
                return Err("replay process kill signal was not preceded by termination".to_owned());
            }
            (logs, Some(2))
        }
        ProcessReport::LifecycleError { message, logs, .. } => {
            if require_success {
                return Err("passed replay report contains a process lifecycle error".to_owned());
            }
            value::require_nonempty(message, "process lifecycle error")?;
            (logs, None)
        }
    };
    if expected_count.is_some_and(|expected| logs.len() != expected)
        || expected_count.is_none() && !(2..=3).contains(&logs.len())
    {
        return Err("replay process carries an invalid log-stream count".to_owned());
    }
    let mut paths = BTreeSet::new();
    for artifact in logs {
        validate_artifact(artifact)?;
        if !paths.insert(&artifact.path) {
            return Err("replay process repeats a log artifact".to_owned());
        }
    }
    Ok(())
}

fn validate_artifact(artifact: &ArtifactRef) -> Result<(), String> {
    if artifact.kind != "verifier-replay-process-log" {
        return Err("replay process references an unsupported artifact kind".to_owned());
    }
    value::require_nonempty(&artifact.path, "artifact path")?;
    value::require_digest(&artifact.sha256, "artifact")?;
    if artifact.size_bytes == 0 {
        return Err("replay artifact is empty".to_owned());
    }
    Ok(())
}

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;
