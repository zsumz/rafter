#[test]
fn raw_reports_reject_coordinated_receipt_binding_tampering() {
    let (catalog, manifest) = crate::tests::loaded();
    let descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| {
            descriptor.simulator.as_ref().is_some_and(|identity| {
                identity.liveness_report.as_ref().is_some_and(|contract| {
                    contract.feature_id == "proposal-progress" && descriptor.clause_id == "LV-02.a"
                })
            })
        })
        .expect("proposal progress descriptor");
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle");
    let check_index = bundle
        .execution
        .checks
        .iter()
        .position(|check| check.evidence_ids == [descriptor.evidence_id()])
        .expect("proposal progress check");
    let (fixture_identity, contracts, events) =
        crate::verification::simulator::liveness_report_tests::fixture();
    let binding = crate::verification::simulator::derive_verified_liveness_binding(
        "pr",
        &fixture_identity,
        &contracts,
        &events,
    )
    .expect("valid raw reports bind");
    let check = &mut bundle.execution.checks[check_index];
    check.observations = BTreeMap::from([
        ("runs:raft-soak".to_owned(), 1),
        ("passes:raft-soak".to_owned(), 1),
        ("steps:raft-soak".to_owned(), 320),
        (
            fixture_identity.required_observation.clone(),
            binding.reports.len() as u64,
        ),
    ]);
    check.simulator_liveness = Some(binding);
    verify_simulator_observations(
        &bundle,
        &bundle.execution.checks[check_index],
        &fixture_identity,
        &contracts,
        &events,
    )
    .expect("raw report binding verifies");

    let mut tampered = bundle.execution.checks[check_index].clone();
    let binding = tampered
        .simulator_liveness
        .as_mut()
        .expect("liveness binding remains");
    binding.reports[0].report_sha256 = "f".repeat(64);
    binding.reports_sha256 = crate::evidence::liveness_reports_digest(&binding.reports);
    let error =
        verify_simulator_observations(&bundle, &tampered, &fixture_identity, &contracts, &events)
            .expect_err("coordinated binding tamper must fail");
    assert!(error.to_string().contains("disagrees with raw logs"));
}

#[test]
fn raw_reports_reject_coordinated_execution_contract_tampering() {
    let (catalog, manifest) = crate::tests::loaded();
    let descriptor = proposal_progress_descriptor(&catalog);
    let mut bundle = simulator_bundle(&catalog, &manifest);
    let check_index = proposal_progress_check_index(&bundle, descriptor);
    let (identity, contracts, mut events) =
        crate::verification::simulator::liveness_report_tests::fixture();
    let binding = crate::verification::simulator::derive_verified_liveness_binding(
        "pr", &identity, &contracts, &events,
    )
    .expect("valid raw reports bind");
    prepare_fixture_check(
        &mut bundle.execution.checks[check_index],
        &identity,
        binding,
    );

    events.get_mut("raft-soak").expect("soak event")[0]["execution_contract"]["max_proposals"] =
        json!(25);
    let receipt = bundle.execution.checks[check_index]
        .simulator_liveness
        .as_mut()
        .expect("liveness binding");
    for report in &mut receipt.reports {
        report.execution_contract.max_proposals = 25;
        report.execution_contract_sha256 =
            crate::evidence::execution_contract_digest(&report.execution_contract);
    }
    receipt.reports_sha256 = crate::evidence::liveness_reports_digest(&receipt.reports);

    let error = verify_simulator_observations(
        &bundle,
        &bundle.execution.checks[check_index],
        &identity,
        &contracts,
        &events,
    )
    .expect_err("coordinated execution-contract tamper must fail");
    assert!(error.to_string().contains("execution contract"));
}

#[test]
fn raw_reports_reject_complete_report_set_substitution() {
    let (catalog, manifest) = crate::tests::loaded();
    let descriptor = proposal_progress_descriptor(&catalog);
    let mut bundle = simulator_bundle(&catalog, &manifest);
    let check_index = proposal_progress_check_index(&bundle, descriptor);
    let (identity, contracts, mut events) =
        crate::verification::simulator::liveness_report_tests::fixture();
    let binding = crate::verification::simulator::derive_verified_liveness_binding(
        "pr", &identity, &contracts, &events,
    )
    .expect("valid raw reports bind");
    prepare_fixture_check(
        &mut bundle.execution.checks[check_index],
        &identity,
        binding,
    );

    let reports = events.get_mut("raft-soak").expect("soak event")[0]["liveness_reports"]
        .as_array_mut()
        .expect("liveness report array");
    reports
        .iter_mut()
        .find(|report| report["feature_id"] == "snapshot-catch-up")
        .expect("snapshot report")["feature_id"] = json!("invented-feature");

    let error = verify_simulator_observations(
        &bundle,
        &bundle.execution.checks[check_index],
        &identity,
        &contracts,
        &events,
    )
    .expect_err("complete report-set substitution must fail");
    assert!(error.to_string().contains("unknown"));
}

#[test]
fn nonpassing_receipt_cannot_mask_a_malformed_structured_liveness_report() {
    let (catalog, manifest) = crate::tests::loaded();
    let descriptor = proposal_progress_descriptor(&catalog);
    let mut bundle = simulator_bundle(&catalog, &manifest);
    let check_index = proposal_progress_check_index(&bundle, descriptor);
    let (identity, contracts, mut events) =
        crate::verification::simulator::liveness_report_tests::fixture();
    let execution_id = bundle.execution.checks[check_index].execution_id.clone();
    for result in bundle
        .results
        .iter_mut()
        .filter(|result| result.execution_id == execution_id)
    {
        result.status = crate::EvidenceStatus::Incomplete;
        result.classification = Some(crate::FailureClassification::CoverageNotReached);
    }
    let check = &mut bundle.execution.checks[check_index];
    check.completion = crate::CheckCompletion::CoverageNotReached;
    check.simulator_liveness = None;
    events.get_mut("raft-soak").expect("soak events")[0]["status"] = json!("incomplete");
    events.get_mut("raft-soak").expect("soak events")[0]["liveness_reports"][0]["preconditions"]
        ["reachable_voters"] = json!(99);

    let error = verify_liveness_observations(
        &bundle,
        &bundle.execution.checks[check_index],
        &identity,
        &contracts,
        &events,
        &mut BTreeMap::new(),
    )
    .expect_err("malformed nonpassing reports must remain a harness error");
    assert!(error
        .to_string()
        .contains("raw liveness reports are invalid"));
}

fn proposal_progress_descriptor(catalog: &crate::Catalog) -> &crate::EvidenceDescriptor {
    catalog
        .evidence
        .iter()
        .find(|descriptor| {
            descriptor.simulator.as_ref().is_some_and(|identity| {
                identity.liveness_report.as_ref().is_some_and(|contract| {
                    contract.feature_id == "proposal-progress" && descriptor.clause_id == "LV-02.a"
                })
            })
        })
        .expect("proposal progress descriptor")
}

fn simulator_bundle(
    catalog: &crate::Catalog,
    manifest: &crate::ProfileManifest,
) -> crate::ResultBundle {
    crate::tests::passing_bundles(catalog, manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle")
}

fn proposal_progress_check_index(
    bundle: &crate::ResultBundle,
    descriptor: &crate::EvidenceDescriptor,
) -> usize {
    bundle
        .execution
        .checks
        .iter()
        .position(|check| check.evidence_ids == [descriptor.evidence_id()])
        .expect("proposal progress check")
}

fn prepare_fixture_check(
    check: &mut crate::CheckReceipt,
    identity: &crate::SimulatorIdentity,
    binding: crate::evidence::SimulatorLivenessBinding,
) {
    check.observations = BTreeMap::from([
        ("runs:raft-soak".to_owned(), 1),
        ("passes:raft-soak".to_owned(), 1),
        ("steps:raft-soak".to_owned(), 320),
        (
            identity.required_observation.clone(),
            binding.reports.len() as u64,
        ),
    ]);
    check.simulator_liveness = Some(binding);
}

fn scheduled_log(source_ref: &str) -> String {
    let seeds = scheduled_simulator_seeds("nightly", source_ref, 6).expect("nightly seeds");
    let mut lines = vec![
        "label: raft-nightly".to_owned(),
        "exit_code: Some(0)".to_owned(),
        "model-check profile=raft-nightly expected_runtime=scheduled".to_owned(),
        format!("model-check raft-nightly-soak seeds source=replay seeds={seeds}"),
        event(&json!({
            "event": "exhaustive-check",
            "check_id": "raft-election-nightly",
            "status": "pass",
            "unique_protocol_states": 5_000_000,
            "unique_verifier_states": 5_000_000,
        })),
        event(&json!({
            "event": "exhaustive-check",
            "check_id": "raft-commit-nightly",
            "status": "pass",
            "unique_protocol_states": 8_000_000,
            "unique_verifier_states": 8_000_000,
        })),
        event(&json!({
            "event": "profile-total",
            "check_id": "raft-profile-total-nightly",
            "profile": "raft-nightly",
            "status": "pass",
            "unique_protocol_states": 13_000_000,
            "unique_verifier_states": 13_000_000,
            "target_protocol_states": 13_000_000,
            "target_verifier_states": 13_000_000,
        })),
    ];
    for seed in seeds.split(',') {
        let seed = u64::from_str_radix(seed.trim_start_matches("0x"), 16).expect("hex seed");
        for check_id in [
            "raft-nightly-soak",
            "raft-nightly-soak-lease",
            "raft-nightly-soak-membership",
        ] {
            lines.push(event(&json!({
                "event": "soak-check",
                "check_id": check_id,
            "status": "pass",
            "seed": seed,
            "steps": 1024,
            })));
        }
    }
    format!("{}\n", lines.join("\n"))
}

fn simulator_configuration(
    profile: &str,
) -> crate::contract::profile::SimulatorRunnerConfiguration {
    let (_, manifest) = crate::tests::loaded();
    manifest.profiles[profile].runners["simulator"]
        .simulator_configuration()
        .expect("typed simulator configuration")
}

fn event(value: &serde_json::Value) -> String {
    format!("{}{}", super::EVENT_PREFIX, value)
}
