//! Simulator model and detector resource-accounting contract.

use crate::producer::{simulator_model::SimulatorExecution, test_exec::TestOutcome};

use super::detector::DetectorRun;

#[derive(Clone, Copy)]
pub(super) struct ResourceMetrics {
    pub(super) duration_ms: u64,
    pub(super) peak_rss_kib: u64,
}

pub(super) fn resource_metrics(
    model: &SimulatorExecution,
    detector: Option<&TestOutcome>,
) -> ResourceMetrics {
    // Detector compilation is paid by the aggregate run; compile-only outcomes have no check runtime.
    let detector = detector.filter(|outcome| {
        !outcome
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "compile-log")
    });
    ResourceMetrics {
        duration_ms: model
            .duration_ms
            .saturating_add(detector.map_or(0, |outcome| outcome.duration_ms)),
        peak_rss_kib: model
            .runtime_peak_rss_kib
            .max(detector.map_or(0, |outcome| outcome.peak_rss_kib)),
    }
}

pub(super) fn execution_resource_metrics(
    model: &SimulatorExecution,
    detectors: &DetectorRun,
) -> ResourceMetrics {
    ResourceMetrics {
        duration_ms: model
            .build_duration_ms
            .saturating_add(model.duration_ms)
            .saturating_add(detectors.duration_ms),
        peak_rss_kib: model
            .build_peak_rss_kib
            .max(model.runtime_peak_rss_kib)
            .max(detectors.peak_rss_kib),
    }
}
