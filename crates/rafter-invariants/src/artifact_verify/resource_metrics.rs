use std::{collections::BTreeSet, fs, path::Path};

use crate::{aggregate::AggregateError, ResultBundle};

pub(super) fn verify_resource_metrics(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<(), AggregateError> {
    for check in &bundle.execution.checks {
        let artifacts = check_metric_artifacts(&bundle.runner, &check.artifacts);
        let derived = derive_process_metrics(artifacts.into_iter(), root)?;
        if check.duration_ms != derived.duration_ms || check.peak_rss_kib != derived.peak_rss_kib {
            return Err(AggregateError::new(format!(
                "check resource metrics disagree with hashed process logs for {}",
                check.check_id
            )));
        }
    }

    let artifacts = bundle
        .execution
        .artifacts
        .iter()
        .chain(
            bundle
                .execution
                .checks
                .iter()
                .flat_map(|check| check.artifacts.iter()),
        )
        .filter(|artifact| is_process_log_kind(&artifact.kind));
    let derived = derive_process_metrics(artifacts, root)?;
    if bundle.execution.duration_ms != derived.duration_ms
        || bundle.execution.peak_rss_kib != derived.peak_rss_kib
    {
        return Err(AggregateError::new(
            "execution resource metrics disagree with hashed process logs".to_owned(),
        ));
    }
    Ok(())
}

fn check_metric_artifacts<'a>(
    runner: &str,
    artifacts: &'a [crate::ArtifactRef],
) -> Vec<&'a crate::ArtifactRef> {
    let has_runtime = artifacts.iter().any(|artifact| {
        matches!(
            artifact.kind.as_str(),
            "test-log" | "simulator-log" | "maelstrom-process-log"
        ) || is_tla_process_log(&artifact.kind)
    });
    artifacts
        .iter()
        .filter(|artifact| {
            is_process_log_kind(&artifact.kind)
                && (runner == "tla" || artifact.kind != "compile-log" || !has_runtime)
        })
        .collect()
}

#[cfg(test)]
impl ResultBundle {
    pub(crate) fn verify_resource_metrics_for_test(
        &self,
        root: &Path,
    ) -> Result<(), AggregateError> {
        verify_resource_metrics(self, root)
    }
}

fn derive_process_metrics<'a>(
    artifacts: impl Iterator<Item = &'a crate::ArtifactRef>,
    root: &Path,
) -> Result<crate::producer::process::ProcessMetrics, AggregateError> {
    let mut duration_ms = 0_u64;
    let mut peak_rss_kib = 0_u64;
    let mut paths = BTreeSet::new();
    for artifact in artifacts {
        if !paths.insert(artifact.path.as_str()) {
            continue;
        }
        let bytes = fs::read(root.join(&artifact.path)).map_err(|error| {
            AggregateError::new(format!("read process log {}: {error}", artifact.path))
        })?;
        let metrics = process_log_metrics(&artifact.kind, &bytes).map_err(|error| {
            AggregateError::new(format!("parse process log {}: {error}", artifact.path))
        })?;
        for metric in metrics {
            duration_ms = duration_ms.checked_add(metric.duration_ms).ok_or_else(|| {
                AggregateError::new("process duration total overflowed".to_owned())
            })?;
            peak_rss_kib = peak_rss_kib.max(metric.peak_rss_kib);
        }
    }
    if paths.is_empty() || peak_rss_kib == 0 {
        return Err(AggregateError::new(
            "receipt has no measurable hashed process logs".to_owned(),
        ));
    }
    Ok(crate::producer::process::ProcessMetrics {
        duration_ms,
        peak_rss_kib,
    })
}

fn process_log_metrics(
    kind: &str,
    bytes: &[u8],
) -> Result<Vec<crate::producer::process::ProcessMetrics>, String> {
    if matches!(kind, "compile-log" | "test-log" | "simulator-log") {
        let source = std::str::from_utf8(bytes)
            .map_err(|error| format!("combined process log is not UTF-8: {error}"))?;
        return crate::producer::process::parse_combined_processes(source).map(|processes| {
            processes
                .into_iter()
                .map(|process| process.metrics)
                .collect()
        });
    }
    let process: crate::producer::ProcessLog =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    if process.peak_rss_kib == 0 {
        return Err("structured process log omitted peak RSS".to_owned());
    }
    Ok(vec![crate::producer::process::ProcessMetrics {
        duration_ms: process.duration_ms,
        peak_rss_kib: process.peak_rss_kib,
    }])
}

fn is_process_log_kind(kind: &str) -> bool {
    matches!(
        kind,
        "compile-log" | "test-log" | "simulator-log" | "maelstrom-process-log"
    ) || is_tla_process_log(kind)
}

fn is_tla_process_log(kind: &str) -> bool {
    matches!(kind, "tla-log" | "tla-trace-log" | "tla-mutation-log")
        || kind.starts_with("tla-detector-log")
}
