//! Runtime execution and evidence binding for simulator fixtures.

use std::{collections::BTreeMap, fs, path::Path};

use super::{
    io::{framed_process_log, source_bound_launchers, write_fixture_artifact},
    model::{CompileFixture, RuntimeDefect, RuntimeFixture, RuntimeFixtureInput},
};

pub(super) fn materialize_runtime_fixture(
    input: &RuntimeFixtureInput<'_>,
    defect: RuntimeDefect,
) -> RuntimeFixture {
    if matches!(defect, RuntimeDefect::ProvenanceOnly) {
        return materialize_provenance_runtime(
            input.root,
            input.current_dir,
            input.environment,
            input.process_runtime,
            input.compile,
        );
    }
    let arguments = ["--profile".into(), "fast".into()];
    let invocation = crate::producer::SimulatorFixtureInvocation {
        label: "fast",
        program: input
            .compile
            .binary_path
            .to_str()
            .expect("UTF-8 simulator fixture path"),
        arguments: &arguments,
        environment: input.environment,
        current_dir: input.current_dir,
        output_dir: input.output_dir,
    };
    let model = match defect {
        RuntimeDefect::ProvenanceOnly => unreachable!("handled before runtime execution"),
        RuntimeDefect::Timeout | RuntimeDefect::MalformedEvent => {
            let (model, receipt) = crate::producer::timed_out_zero_exit_fixture_at(
                "pr",
                input.source_ref,
                &invocation,
            )
            .expect("run real TERM-trap fixture through production reduction");
            assert_eq!(receipt.exit_code, Some(0));
            assert!(receipt.timed_out);
            model
        }
        RuntimeDefect::LaunchFailure => {
            let model =
                crate::producer::later_launch_error_fixture_at("pr", input.source_ref, &invocation);
            assert!(model
                .harness_errors
                .iter()
                .any(|error| error.contains("injected raft-soak launch failure")));
            model
        }
        RuntimeDefect::PassExitOne | RuntimeDefect::CounterexampleExitOne => {
            let model =
                crate::producer::later_launch_error_fixture_at("pr", input.source_ref, &invocation);
            assert!(!model.processes_succeeded);
            assert!(model
                .harness_errors
                .iter()
                .any(|error| error.contains("fast did not complete successfully")));
            model
        }
    };
    let [real_artifact] = model.artifacts.as_slice() else {
        panic!("timeout fixture must retain one simulator log")
    };
    let real_log = fs::read(&real_artifact.path).expect("read timeout process artifact");
    let fast_artifact = write_fixture_artifact(
        input.root,
        "artifacts/invariants/fast.log",
        "simulator-log",
        &real_log,
    );
    let (catalog, manifest) = crate::tests::loaded();
    let (checks, results) =
        crate::producer::evaluate_model_fixture(&catalog, &manifest, "pr", &model)
            .expect("evaluate real timeout events through simulator receipt production");
    RuntimeFixture {
        fast_artifact,
        producer_artifact: write_fixture_artifact(
            input.root,
            "artifacts/invariants/rafter-invariants",
            "producer-binary",
            b"fixture producer binary",
        ),
        duration_ms: model.duration_ms,
        peak_rss_kib: model.runtime_peak_rss_kib.max(1),
        checks,
        results,
    }
}

fn materialize_provenance_runtime(
    root: &Path,
    current_dir: &Path,
    environment: &BTreeMap<String, String>,
    process_runtime: &BTreeMap<String, crate::ExecutableReceipt>,
    compile: &CompileFixture,
) -> RuntimeFixture {
    let invocation = crate::InvocationReceipt {
        program: compile.binary_path.to_string_lossy().into_owned(),
        program_sha256: compile.binary_artifact.sha256.clone(),
        arguments: vec!["--profile".to_owned(), "fast".to_owned()],
        current_dir: current_dir.to_string_lossy().into_owned(),
        environment: environment.clone(),
        environment_sha256: crate::provenance::invocation::digest_environment(environment)
            .expect("valid fixture environment"),
        launchers: source_bound_launchers(process_runtime),
    };
    let event = serde_json::json!({
        "event": "check-failure",
        "event_version": 2,
        "check_id": "raft-commit",
        "status": "fail",
        "classification": "invariant-violation",
        "invariant_id": "CM-02",
        "invariant": "CM-02 commit requires effective quorum",
    });
    let stdout = format!("RAFTER_EVENT {event}\n");
    let log = framed_process_log("fast", &invocation, false, &stdout, "");
    RuntimeFixture {
        fast_artifact: write_fixture_artifact(
            root,
            "artifacts/invariants/fast.log",
            "simulator-log",
            log.as_bytes(),
        ),
        producer_artifact: write_fixture_artifact(
            root,
            "artifacts/invariants/rafter-invariants",
            "producer-binary",
            b"fixture producer binary",
        ),
        duration_ms: 1,
        peak_rss_kib: 1,
        checks: Vec::new(),
        results: Vec::new(),
    }
}

pub(super) fn bind_fixture_evidence(
    bundle: &mut crate::ResultBundle,
    current_dir: &Path,
    compile: &CompileFixture,
    runtime: &RuntimeFixture,
) {
    bundle.execution.producer = crate::ProducerBindingReceipt {
        binding: crate::provenance::image::PRODUCER_BINDING.to_owned(),
        executable: runtime.producer_artifact.clone(),
    };
    bundle.execution.invocation.program_sha256 = runtime.producer_artifact.sha256.clone();
    bundle.execution.invocation.program =
        crate::provenance::image::image_path(current_dir, &runtime.producer_artifact.sha256)
            .to_string_lossy()
            .into_owned();
    bundle.execution.invocation.current_dir = current_dir.to_string_lossy().into_owned();
    let has_runtime_checks = !runtime.checks.is_empty();
    if has_runtime_checks {
        // Preserve semantic receipts while binding missing-detector failures to real compilation.
        bundle.execution.checks = runtime
            .checks
            .iter()
            .cloned()
            .map(|mut check| {
                check
                    .observations
                    .insert("detector_qualified".to_owned(), 0);
                check.artifacts = vec![compile.compile_artifact.clone()];
                check
            })
            .collect();
    } else {
        bundle.execution.checks.truncate(1);
    }
    if !runtime.results.is_empty() {
        bundle.results = runtime.results.clone();
    }
    for result in &mut bundle.results {
        if result.status != crate::EvidenceStatus::Pass {
            result.artifacts = vec![runtime.fast_artifact.clone()];
        }
    }
    for check in &mut bundle.execution.checks {
        if has_runtime_checks {
            check.duration_ms = 1;
            check.peak_rss_kib = 1;
        } else {
            check.artifacts = vec![runtime.fast_artifact.clone()];
            check.duration_ms = runtime.duration_ms;
            check.peak_rss_kib = runtime.peak_rss_kib;
        }
    }
    bundle.execution.artifacts = vec![
        runtime.producer_artifact.clone(),
        compile.binary_artifact.clone(),
        compile.compile_artifact.clone(),
        runtime.fast_artifact.clone(),
    ];
    bundle.execution.duration_ms = runtime.duration_ms.saturating_add(1);
    bundle.execution.peak_rss_kib = runtime.peak_rss_kib;
}
