#[test]
fn aggregate_rederives_maelstrom_semantics_from_trial_artifacts(
) -> Result<(), Box<dyn std::error::Error>> {
    let root = temporary_root()?;
    write(&root, "scripts/maelstrom-lin-kv", "runner")?;
    prepare_state(&root)?;
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(
        &root,
        "artifacts/invariants/evidence/inputs/runner",
        "runner",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/inputs/binary",
        "binary",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/results.edn",
        VALID,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 0, "")?,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/node.log",
        "role=leader",
    )?;
    let bundle = bundle();

    crate::artifact_verify_maelstrom::verify(&bundle, &root)?;

    let mut wrong_invocation: serde_json::Value =
        serde_json::from_str(&process_log(&root, 0, "")?)?;
    wrong_invocation["invocation"]["arguments"] = serde_json::json!(["--test-count", "2"]);
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &wrong_invocation.to_string(),
    )?;
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root)
        .expect_err("wrong complete invocation is rejected")
        .to_string()
        .contains("arguments"));
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 0, "")?,
    )?;

    assert_substituted_launcher_rejected(&root, &bundle)?;
    assert_script_launcher_chain_rejected(&root, &bundle)?;

    let mut forged = bundle.clone();
    forged.execution.checks[0]
        .observations
        .insert("read_ok".to_owned(), 99);
    assert!(crate::artifact_verify_maelstrom::verify(&forged, &root)
        .expect_err("forged observation is rejected")
        .to_string()
        .contains("observations disagree"));

    let mut stale: serde_json::Value = serde_json::from_str(&process_log(&root, 0, "")?)?;
    stale["schema_version"] = serde_json::json!(1);
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &stale.to_string(),
    )?;
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root)
        .expect_err("stale process schema is rejected")
        .to_string()
        .contains("schema version 1 does not match required version 3"));

    let mut incomplete: serde_json::Value = serde_json::from_str(&process_log(&root, 0, "")?)?;
    incomplete["invocation"]["program"] = serde_json::json!("");
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &incomplete.to_string(),
    )?;
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root)
        .expect_err("incomplete invocation is rejected")
        .to_string()
        .contains("exact invocation"));
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 0, "")?,
    )?;

    let mut missing = bundle;
    missing.execution.checks[0]
        .artifacts
        .retain(|artifact| artifact.kind != "maelstrom-process-log");
    assert!(crate::artifact_verify_maelstrom::verify(&missing, &root)
        .expect_err("missing process evidence is rejected")
        .to_string()
        .contains("process-log is missing"));

    fs::remove_dir_all(root)?;
    Ok(())
}
fn assert_substituted_launcher_rejected(
    root: &Path,
    bundle: &ResultBundle,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut substituted: serde_json::Value = serde_json::from_str(&process_log(root, 0, "")?)?;
    substituted["invocation"]["launchers"][0]["sha256"] = serde_json::json!("f".repeat(64));
    write(
        root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &substituted.to_string(),
    )?;
    assert!(crate::artifact_verify_maelstrom::verify(bundle, root)
        .expect_err("source-mismatched launcher digest is rejected")
        .to_string()
        .contains("exact invocation"));
    write(
        root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(root, 0, "")?,
    )?;
    Ok(())
}

fn assert_script_launcher_chain_rejected(
    root: &Path,
    bundle: &ResultBundle,
) -> Result<(), Box<dyn std::error::Error>> {
    let canonical: serde_json::Value = serde_json::from_str(&process_log(root, 0, "")?)?;
    let launchers = canonical["invocation"]["launchers"]
        .as_array()
        .ok_or("fixture launchers are not an array")?;

    let mut omitted = canonical.clone();
    omitted["invocation"]["launchers"] = serde_json::json!(launchers[..4]);

    let mut reordered = canonical.clone();
    let mut reordered_launchers = launchers.clone();
    reordered_launchers.swap(3, 4);
    reordered["invocation"]["launchers"] = serde_json::json!(reordered_launchers);

    let mut wrong_bash = canonical.clone();
    wrong_bash["invocation"]["launchers"][4]["sha256"] = serde_json::json!("f".repeat(64));

    for (label, altered) in [
        ("omitted Bash launcher", omitted),
        ("reordered Bash launcher", reordered),
        ("substituted Bash launcher", wrong_bash),
    ] {
        write(
            root,
            "artifacts/invariants/evidence/trial-0/process.json",
            &altered.to_string(),
        )?;
        let error = crate::artifact_verify_maelstrom::verify(bundle, root)
            .expect_err("an invalid script launcher chain is rejected");
        assert!(
            error.to_string().contains("exact invocation"),
            "{label} produced the wrong verifier error: {error}"
        );
    }

    write(
        root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &canonical.to_string(),
    )?;
    Ok(())
}

#[test]
fn misplaced_tool_inputs_and_unscoped_trial_evidence_are_rejected(
) -> Result<(), Box<dyn std::error::Error>> {
    let root = temporary_root()?;
    write(&root, "scripts/maelstrom-lin-kv", "runner")?;
    prepare_state(&root)?;
    write(&root, "target/debug/rafter-maelstrom", "binary")?;

    let mut trial_scoped_input = bundle();
    let runner = trial_scoped_input.execution.checks[0]
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.kind == "maelstrom-runner")
        .ok_or("fixture omitted runner")?;
    runner.path = "artifacts/invariants/evidence/trial-0/runner".to_owned();
    assert!(
        crate::artifact_verify_maelstrom::verify(&trial_scoped_input, &root)
            .expect_err("trial-scoped tool input is rejected")
            .to_string()
            .contains("carries a trial path")
    );

    let mut unscoped_evidence = bundle();
    let results = unscoped_evidence.execution.checks[0]
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.kind == "maelstrom-results")
        .ok_or("fixture omitted results")?;
    results.path = "artifacts/invariants/evidence/results.edn".to_owned();
    assert!(
        crate::artifact_verify_maelstrom::verify(&unscoped_evidence, &root)
            .expect_err("trial evidence without a trial path is rejected")
            .to_string()
            .contains("lacks a trial path")
    );

    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn invalid_history_can_only_fail_client_linearizability() -> Result<(), Box<dyn std::error::Error>>
{
    let root = temporary_root()?;
    write(&root, "scripts/maelstrom-lin-kv", "runner")?;
    prepare_state(&root)?;
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(
        &root,
        "artifacts/invariants/evidence/inputs/runner",
        "runner",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/inputs/binary",
        "binary",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/results.edn",
        &VALID.replace(
            ":linearizable {:valid? true}",
            ":linearizable {:valid? false}",
        ),
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 0, "")?,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/node.log",
        "role=leader",
    )?;
    let mut bundle = bundle();
    bundle.execution.checks[0]
        .observations
        .insert("valid_trials".to_owned(), 0);
    bundle.execution.checks[0]
        .observations
        .insert("invalid_trials".to_owned(), 1);
    bundle.execution.checks[0].completion = CheckCompletion::Counterexample;
    bundle.results[0].status = EvidenceStatus::Fail;

    let diagnostics = crate::artifact_verify_maelstrom::verify(&bundle, &root)?;
    assert!(diagnostics.is_empty());

    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 1, "checker exited after reporting a counterexample")?,
    )?;
    let diagnostics = crate::artifact_verify_maelstrom::verify(&bundle, &root)?;
    assert_eq!(diagnostics.len(), 1);
    assert!(diagnostics[0].contains("counterexample alongside a harness error"));

    bundle.results[0].invariant_id = "LG-04".to_owned();
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root).is_err());
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn nonzero_process_exit_is_always_a_harness_error() -> Result<(), Box<dyn std::error::Error>> {
    let root = temporary_root()?;
    write(&root, "scripts/maelstrom-lin-kv", "runner")?;
    prepare_state(&root)?;
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(
        &root,
        "artifacts/invariants/evidence/inputs/runner",
        "runner",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/inputs/binary",
        "binary",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/results.edn",
        VALID,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/process.json",
        &process_log(&root, 1, "failed")?,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/node.log",
        "role=leader",
    )?;
    let mut bundle = bundle();
    bundle.execution.checks[0].completion = CheckCompletion::HarnessError;
    bundle.results[0].status = EvidenceStatus::Error;

    crate::artifact_verify_maelstrom::verify(&bundle, &root)?;

    bundle.execution.checks[0].completion = CheckCompletion::CoverageNotReached;
    bundle.results[0].status = EvidenceStatus::Incomplete;
    assert!(crate::artifact_verify_maelstrom::verify(&bundle, &root).is_err());
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn serialized_counterexample_retains_secondary_harness_diagnostic(
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = serialized_counterexample_fixture()?;
    let (catalog, manifest) = crate::tests::loaded();
    let intake = canonical_counterexample_intake(&catalog, &manifest, &fixture)?;
    assert_eq!(intake.defects().len(), 1, "{:?}", intake.defects());
    assert!(intake.defects()[0]
        .message()
        .contains("counterexample alongside a harness error"));
    let report = crate::verdict::reduce(&catalog, &manifest, &intake)?;
    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 0);
    assert!(report
        .invariants
        .iter()
        .all(|verdict| verdict.issues.iter().any(|issue| {
            issue.classification == FailureClassification::HarnessError
                && issue
                    .message
                    .contains("counterexample alongside a harness error")
        })));
    let rd06 = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == "RD-06")
        .expect("RD-06 verdict");
    assert!(rd06.issues.iter().any(|issue| {
        issue.classification == FailureClassification::InvariantViolation
            && issue.message == "Maelstrom reported a non-linearizable client history"
    }));
    Ok(())
}
