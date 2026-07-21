//! Cross-checks aggregate resource claims against hashed process receipts.

use std::collections::BTreeSet;

#[cfg(test)]
use std::path::Path;

use crate::{
    contract::profile::EvidenceLayer,
    evidence::{ArtifactRef, ResultBundle},
    verification::{AggregateError, AuthenticatedArtifacts},
};

#[derive(Clone, Copy)]
enum ProcessArtifactKind {
    Compile,
    Test,
    Simulator,
    Maelstrom,
    Tla,
}

impl ProcessArtifactKind {
    fn from_wire(kind: &str) -> Option<Self> {
        match kind {
            "compile-log" => Some(Self::Compile),
            "test-log" => Some(Self::Test),
            "simulator-log" => Some(Self::Simulator),
            "maelstrom-process-log" => Some(Self::Maelstrom),
            "tla-log" | "tla-trace-log" | "tla-mutation-log" => Some(Self::Tla),
            kind if kind.starts_with("tla-detector-log") => Some(Self::Tla),
            _ => None,
        }
    }

    fn is_runtime(self) -> bool {
        !matches!(self, Self::Compile)
    }
}

#[derive(Clone, Copy)]
enum MetricScope {
    Check {
        layer: EvidenceLayer,
        has_runtime: bool,
    },
    Execution,
}

impl MetricScope {
    fn includes(self, kind: ProcessArtifactKind) -> bool {
        match self {
            Self::Execution => true,
            Self::Check { layer, has_runtime } => {
                layer == EvidenceLayer::Tla
                    || !has_runtime
                    || !matches!(kind, ProcessArtifactKind::Compile)
            }
        }
    }
}

struct ProcessArtifact<'a> {
    artifact: &'a ArtifactRef,
    kind: ProcessArtifactKind,
}

pub(super) fn verify_resource_metrics_authenticated(
    bundle: &ResultBundle,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    let layer = evidence_layer(&bundle.runner)?;
    for check in &bundle.execution.checks {
        let artifacts = process_artifacts(&check.artifacts);
        let scope = MetricScope::Check {
            layer,
            has_runtime: artifacts.iter().any(|artifact| artifact.kind.is_runtime()),
        };
        let derived = derive_process_metrics(
            artifacts
                .into_iter()
                .filter(|artifact| scope.includes(artifact.kind)),
            authenticated,
        )?;
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
        .filter_map(process_artifact)
        .filter(|artifact| MetricScope::Execution.includes(artifact.kind));
    let derived = derive_process_metrics(artifacts, authenticated)?;
    if bundle.execution.duration_ms != derived.duration_ms
        || bundle.execution.peak_rss_kib != derived.peak_rss_kib
    {
        return Err(AggregateError::new(
            "execution resource metrics disagree with hashed process logs".to_owned(),
        ));
    }
    Ok(())
}

fn evidence_layer(runner: &str) -> Result<EvidenceLayer, AggregateError> {
    match runner {
        "tests" => Ok(EvidenceLayer::Tests),
        "simulator" => Ok(EvidenceLayer::Simulator),
        "tla" => Ok(EvidenceLayer::Tla),
        "maelstrom" => Ok(EvidenceLayer::Maelstrom),
        _ => Err(AggregateError::new(format!(
            "no resource metric policy exists for runner {runner}"
        ))),
    }
}

fn process_artifacts(artifacts: &[ArtifactRef]) -> Vec<ProcessArtifact<'_>> {
    artifacts.iter().filter_map(process_artifact).collect()
}

fn process_artifact(artifact: &ArtifactRef) -> Option<ProcessArtifact<'_>> {
    Some(ProcessArtifact {
        kind: ProcessArtifactKind::from_wire(&artifact.kind)?,
        artifact,
    })
}

#[cfg(test)]
pub(crate) fn verify_resource_metrics(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<(), AggregateError> {
    let authenticated = crate::verification::snapshot_available_artifacts(bundle, root)?;
    verify_resource_metrics_authenticated(bundle, &authenticated)
}

fn derive_process_metrics<'a>(
    artifacts: impl Iterator<Item = ProcessArtifact<'a>>,
    authenticated: &AuthenticatedArtifacts,
) -> Result<crate::evidence::format::process::ProcessMetrics, AggregateError> {
    let mut duration_ms = 0_u64;
    let mut peak_rss_kib = 0_u64;
    let mut paths = BTreeSet::new();
    for artifact in artifacts {
        if !paths.insert(artifact.artifact.path.as_str()) {
            continue;
        }
        let metrics = process_log_metrics(artifact.kind, artifact.artifact, authenticated)?;
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
    Ok(crate::evidence::format::process::ProcessMetrics {
        duration_ms,
        peak_rss_kib,
    })
}

fn process_log_metrics(
    kind: ProcessArtifactKind,
    artifact: &ArtifactRef,
    authenticated: &AuthenticatedArtifacts,
) -> Result<Vec<crate::evidence::format::process::ProcessMetrics>, AggregateError> {
    if matches!(
        kind,
        ProcessArtifactKind::Compile | ProcessArtifactKind::Test | ProcessArtifactKind::Simulator
    ) {
        return combined_process_metrics(artifact, authenticated);
    }
    let bytes = authenticated.bytes(artifact)?;
    let source = std::str::from_utf8(bytes).map_err(|error| {
        AggregateError::new(format!("structured process log is not UTF-8: {error}"))
    })?;
    let process = match kind {
        ProcessArtifactKind::Maelstrom => {
            crate::evidence::format::process::parse_maelstrom_v3(source)
                .map_err(|error| AggregateError::new(error.to_string()))?
        }
        ProcessArtifactKind::Tla => crate::evidence::format::process::parse_tla_v4(source)
            .map_err(|error| AggregateError::new(error.to_string()))?,
        ProcessArtifactKind::Compile
        | ProcessArtifactKind::Test
        | ProcessArtifactKind::Simulator => {
            return combined_process_metrics(artifact, authenticated)
        }
    };
    if process.peak_rss_kib == 0 {
        return Err(AggregateError::new(
            "structured process log omitted peak RSS".to_owned(),
        ));
    }
    Ok(vec![crate::evidence::format::process::ProcessMetrics {
        duration_ms: process.duration_ms,
        peak_rss_kib: process.peak_rss_kib,
    }])
}

fn combined_process_metrics(
    artifact: &ArtifactRef,
    authenticated: &AuthenticatedArtifacts,
) -> Result<Vec<crate::evidence::format::process::ProcessMetrics>, AggregateError> {
    authenticated
        .combined_processes(artifact)
        .map(|processes| processes.iter().map(|process| process.metrics).collect())
}
