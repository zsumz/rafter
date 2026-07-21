//! Tests for replay process-log publication.

use std::time::{Duration, Instant};

use super::{lifecycle_error, ProcessReport, ReplayArtifactPublisher};
use crate::verification::detector_replay::process::RetainedProcessDiagnostics;

#[test]
fn lifecycle_failure_publishes_retained_logs_and_missing_file_diagnostic() {
    let root = std::path::PathBuf::from("target/rafter-invariants/replay-process-failure-tests")
        .join(std::process::id().to_string());
    std::fs::create_dir_all(&root).expect("create process failure fixture");
    let stdout = root.join("stdout");
    let stderr = root.join("stderr");
    std::fs::write(&stdout, b"partial stdout").expect("write stdout");
    std::fs::write(&stderr, b"partial stderr").expect("write stderr");
    let diagnostics = RetainedProcessDiagnostics {
        stdout,
        stderr,
        telemetry: Some(root.join("missing-telemetry")),
    };
    let publisher = ReplayArtifactPublisher::create(
        "pr",
        &format!("process-failure-{}", std::process::id()),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("create publisher");

    let report = lifecycle_error(
        &publisher,
        "fixture",
        "fixture-1",
        "lifecycle failed",
        &diagnostics,
    )
    .expect("publish retained diagnostics");
    let ProcessReport::LifecycleError { message, logs, .. } = report else {
        panic!("expected lifecycle-error process report");
    };
    assert_eq!(message, "lifecycle failed");
    assert_eq!(logs.len(), 3);
    publisher.publish_manifest().expect("publish manifest");
    let guard = publisher.seal().expect("seal diagnostics");
    assert_eq!(guard.references().len(), 4);
}
