use sha2::{Digest, Sha256};

#[test]
fn detector_compile_failure_round_trips_without_charging_the_scoped_check() {
    let (catalog, manifest) = crate::tests::loaded();
    let failing = safety_descriptor(&catalog.evidence).clone();
    let identity = failing.simulator.as_ref().expect("simulator identity");
    let negative_test = identity
        .negative_test
        .as_ref()
        .expect("direct safety descriptor has a negative test");
    let mut unaffected = failing.clone();
    unaffected.invariant_id.push_str("-unaffected");
    unaffected.clause_id.push_str("-unaffected");
    unaffected
        .simulator
        .as_mut()
        .expect("simulator identity")
        .negative_test = None;
    let descriptors = vec![failing.clone(), unaffected.clone()];

    let root = Path::new("target/rafter-invariants/tests").join(format!(
        "simulator-detector-compile-metrics-{}",
        std::process::id()
    ));
    if root.exists() {
        fs::remove_dir_all(&root).expect("remove stale resource fixture");
    }
    fs::create_dir_all(&root).expect("create resource fixture");
    let model_log = write_process_metric_fixture(&root, "model.log", "simulator-log", 0, 5, 13);
    let compile_log = write_process_metric_fixture(&root, "compile.log", "compile-log", 1, 7, 29);

    let events = identity
        .checks
        .iter()
        .map(|check| {
            (
                check.clone(),
                vec![json!({
                    "event": "exhaustive-check",
                    "check_id": check,
                    "status": "pass",
                    "classification": null,
                    "unique_protocol_states": identity.minimum_protocol_states.unwrap_or_default(),
                    "unique_verifier_states": identity.minimum_verifier_states.unwrap_or_default(),
                    "observations": {
                        identity.required_observation.clone(): identity.minimum_observation,
                    },
                })],
            )
        })
        .collect();
    let mut model = model_fixture(events);
    model.artifacts.push(model_log.clone());
    model.duration_ms = 5;
    model.runtime_peak_rss_kib = 13;
    let detectors = DetectorRun {
        outcomes: BTreeMap::from([(
            negative_test.check_id(),
            super::test_exec::TestOutcome {
                completion: CheckCompletion::HarnessError,
                status: EvidenceStatus::Error,
                classification: Some(FailureClassification::HarnessError),
                message: Some("cargo test --no-run failed for detector target".to_owned()),
                observations: BTreeMap::new(),
                duration_ms: 7,
                peak_rss_kib: 29,
                artifacts: vec![compile_log.clone()],
            },
        )]),
        artifacts: vec![compile_log.clone()],
        peak_rss_kib: 29,
        duration_ms: 7,
        harness_error: None,
    };

    let (checks, results) = evaluate_descriptors(
        &descriptors,
        "pr",
        &BTreeMap::new(),
        &[],
        &model,
        &detectors,
    )
    .expect("produce scoped compile-failure receipts");
    assert_compile_failure_is_scoped(&failing, &unaffected, &checks, &results);
    let aggregate = execution_resource_metrics(&model, &detectors);
    assert_eq!(aggregate.duration_ms, 12);
    assert_eq!(aggregate.peak_rss_kib, 29);

    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle");
    bundle.execution.checks = checks;
    bundle.results = results;
    bundle.execution.artifacts = vec![model_log, compile_log];
    bundle.execution.duration_ms = aggregate.duration_ms;
    bundle.execution.peak_rss_kib = aggregate.peak_rss_kib;
    bundle
        .verify_resource_metrics_for_test(&root)
        .expect("producer metrics verify with compile cost exactly once");
    fs::remove_dir_all(root).expect("remove resource fixture");
}

fn assert_compile_failure_is_scoped(
    failing: &EvidenceDescriptor,
    unaffected: &EvidenceDescriptor,
    checks: &[crate::CheckReceipt],
    results: &[crate::EvidenceResult],
) {
    let failed_check = checks
        .iter()
        .find(|check| check.evidence_ids == [failing.evidence_id()])
        .expect("compile-failure check");
    assert_eq!(failed_check.completion, CheckCompletion::HarnessError);
    assert_eq!(failed_check.duration_ms, 5);
    assert_eq!(failed_check.peak_rss_kib, 13);
    let failed_result = results
        .iter()
        .find(|result| result.evidence_id == failing.evidence_id())
        .expect("compile-failure result");
    assert_eq!(failed_result.status, EvidenceStatus::Error);
    assert_eq!(
        failed_result.classification,
        Some(FailureClassification::HarnessError)
    );
    let unaffected_result = results
        .iter()
        .find(|result| result.evidence_id == unaffected.evidence_id())
        .expect("unaffected result");
    assert_eq!(unaffected_result.status, EvidenceStatus::Pass);
    assert_eq!(unaffected_result.classification, None);
}

fn write_process_metric_fixture(
    root: &Path,
    relative: &str,
    kind: &str,
    exit_code: i32,
    duration_ms: u64,
    peak_rss_kib: u64,
) -> crate::ArtifactRef {
    let source = format!(
        concat!(
            "schema_version: 3\n",
            "label: fixture\n",
            "invocation: {{\"program\":\"/bin/test\",\"program_sha256\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"arguments\":[\"test\"],\"current_dir\":\"/workspace\",\"environment\":{{}},\"environment_sha256\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\"}}\n",
            "exit_code: Some({exit_code})\n",
            "timed_out: false\n",
            "duration_ms: {duration_ms}\n",
            "peak_rss_kib: {peak_rss_kib}\n",
            "stdout_bytes: 2\n",
            "stderr_bytes: 0\n\n",
            "ok",
        ),
        exit_code = exit_code,
        duration_ms = duration_ms,
        peak_rss_kib = peak_rss_kib,
    );
    fs::write(root.join(relative), &source).expect("write process metric fixture");
    crate::ArtifactRef {
        kind: kind.to_owned(),
        path: relative.to_owned(),
        sha256: format!("{:x}", Sha256::digest(source.as_bytes())),
        size_bytes: source.len() as u64,
    }
}
