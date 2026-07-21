//! TLA+ terminal classification, checkpoint preparation, and failure outcomes.

use std::{collections::BTreeMap, error::Error, path::Path, time::Instant};

use crate::evidence::ArtifactRef;

use super::{
    super::{checkpoint, process, tla_output},
    budget::ExecutionBudget,
    model::{DetectorProbes, MainStatus, ProbeStatus, TlaExecution, TlcRun},
    probes::empty_detector_qualifications,
};

pub(super) struct MainCompletion<'a> {
    pub(super) trace: &'a TlcRun,
    pub(super) detectors: DetectorProbes,
    pub(super) artifacts: Vec<ArtifactRef>,
    pub(super) checkpoint: Option<checkpoint::Preparation>,
    pub(super) checkpoint_report: Option<checkpoint::RecoveryReport>,
    pub(super) output_dir: &'a Path,
    pub(super) total_deadline: Instant,
}

pub(in crate::producer) fn parse_main_summary(
    output: &[u8],
) -> (Option<tla_output::TlcSummary>, Option<String>) {
    match tla_output::parse(output) {
        Ok(summary) => (Some(summary), None),
        Err(error) => match tla_output::parse_complete_prefix(output) {
            Ok(summary) if summary.violated_invariant.is_some() => (Some(summary), Some(error)),
            _ => (None, Some(error)),
        },
    }
}

pub(super) fn complete_main_execution(
    completion: MainCompletion<'_>,
    main: TlcRun,
) -> Result<TlaExecution, Box<dyn Error>> {
    let MainCompletion {
        trace,
        detectors,
        mut artifacts,
        checkpoint,
        checkpoint_report,
        output_dir,
        total_deadline,
    } = completion;
    let (summary, main_parse_error) = parse_main_summary(&main.output.stdout);
    let main_progress = main
        .output
        .timed_out
        .then(|| {
            tla_output::parse_latest_progress(&main.output.stdout)
                .ok()
                .flatten()
        })
        .flatten();
    let peak_rss_kib = trace
        .output
        .peak_rss_kib
        .max(detectors.peak_rss_kib)
        .max(main.output.peak_rss_kib);
    let duration_ms = process::duration_ms(trace.output.duration)
        .saturating_add(detectors.duration_ms)
        .saturating_add(process::duration_ms(main.output.duration));
    let main_status = classify_main_status(&main.output);
    artifacts.push(main.artifact);
    if let Some(preparation) = checkpoint {
        if summary
            .as_ref()
            .and_then(|summary| summary.violated_invariant.as_ref())
            .is_some()
        {
            artifacts.extend(preparation.abandon());
        } else {
            artifacts.extend(preparation.finish(output_dir, total_deadline)?);
        }
    }
    Ok(TlaExecution {
        main: summary,
        main_progress,
        main_parse_error,
        main_status,
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Passed,
        detector_qualifications: detectors.qualifications,
        peak_rss_kib,
        duration_ms,
        artifacts,
        checkpoint_report,
        checkpoint_error: None,
    })
}

fn classify_main_status(output: &process::ProcessOutput) -> MainStatus {
    if output.timed_out {
        MainStatus::TimedOut
    } else if output.status.success() {
        MainStatus::Succeeded
    } else {
        MainStatus::Failed
    }
}

pub(super) fn prepare_checkpoint(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    artifacts: &[ArtifactRef],
    output_dir: &Path,
    budget: ExecutionBudget,
) -> Result<Option<checkpoint::Preparation>, Box<dyn Error>> {
    checkpoint::enabled(configuration)
        .then(|| {
            checkpoint::prepare(
                profile,
                source_ref,
                configuration,
                artifacts,
                output_dir,
                budget.execution_deadline,
            )
        })
        .transpose()
}

pub(super) fn trace_failure(trace: &TlcRun, artifacts: Vec<ArtifactRef>) -> TlaExecution {
    TlaExecution {
        main: None,
        main_progress: None,
        main_parse_error: None,
        main_status: MainStatus::NotRun,
        trace_status: ProbeStatus::Failed,
        detector_status: ProbeStatus::NotRun,
        detector_qualifications: empty_detector_qualifications(),
        peak_rss_kib: trace.output.peak_rss_kib,
        duration_ms: process::duration_ms(trace.output.duration),
        artifacts,
        checkpoint_report: None,
        checkpoint_error: None,
    }
}

pub(super) fn trace_budget_failure(artifacts: Vec<ArtifactRef>) -> TlaExecution {
    TlaExecution {
        main: None,
        main_progress: None,
        main_parse_error: None,
        main_status: MainStatus::NotRun,
        trace_status: ProbeStatus::Failed,
        detector_status: ProbeStatus::NotRun,
        detector_qualifications: empty_detector_qualifications(),
        peak_rss_kib: 0,
        duration_ms: 0,
        artifacts,
        checkpoint_report: None,
        checkpoint_error: None,
    }
}

pub(super) fn detector_failure(
    trace: &TlcRun,
    detectors: DetectorProbes,
    artifacts: Vec<ArtifactRef>,
) -> TlaExecution {
    TlaExecution {
        main: None,
        main_progress: None,
        main_parse_error: None,
        main_status: MainStatus::NotRun,
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Failed,
        detector_qualifications: detectors.qualifications,
        peak_rss_kib: trace.output.peak_rss_kib.max(detectors.peak_rss_kib),
        duration_ms: process::duration_ms(trace.output.duration)
            .saturating_add(detectors.duration_ms),
        artifacts,
        checkpoint_report: None,
        checkpoint_error: None,
    }
}

pub(super) fn checkpoint_failure(
    trace: &TlcRun,
    detectors: DetectorProbes,
    artifacts: Vec<ArtifactRef>,
    checkpoint_report: Option<checkpoint::RecoveryReport>,
    error: String,
) -> TlaExecution {
    TlaExecution {
        main: None,
        main_progress: None,
        main_parse_error: None,
        main_status: MainStatus::NotRun,
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Passed,
        detector_qualifications: detectors.qualifications,
        peak_rss_kib: trace.output.peak_rss_kib.max(detectors.peak_rss_kib),
        duration_ms: process::duration_ms(trace.output.duration)
            .saturating_add(detectors.duration_ms),
        artifacts,
        checkpoint_report,
        checkpoint_error: Some(error),
    }
}

pub(super) fn main_budget_failure(
    trace: &TlcRun,
    detectors: DetectorProbes,
    artifacts: Vec<ArtifactRef>,
    checkpoint_report: Option<checkpoint::RecoveryReport>,
) -> TlaExecution {
    TlaExecution {
        main: None,
        main_progress: None,
        main_parse_error: None,
        main_status: MainStatus::TimedOut,
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Passed,
        detector_qualifications: detectors.qualifications,
        peak_rss_kib: trace.output.peak_rss_kib.max(detectors.peak_rss_kib),
        duration_ms: process::duration_ms(trace.output.duration)
            .saturating_add(detectors.duration_ms),
        artifacts,
        checkpoint_report,
        checkpoint_error: None,
    }
}
