struct SerializedCounterexampleFixture {
    root: PathBuf,
    result_path: PathBuf,
    bundle: ResultBundle,
    diagnostic: String,
}
impl Drop for SerializedCounterexampleFixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.result_path);
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn serialized_counterexample_fixture(
) -> Result<SerializedCounterexampleFixture, Box<dyn std::error::Error>> {
    let root = temporary_root()?;
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    materialize_checkout(&root, &workspace)?;
    write(&root, "scripts/maelstrom-lin-kv", "runner")?;
    prepare_state(&root)?;
    write(&root, "target/debug/rafter-maelstrom", "binary")?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/runner",
        "runner",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/binary",
        "binary",
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/maelstrom.jar",
        "jar",
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
        &process_log(&root, 1, "checker exited after reporting a counterexample")?,
    )?;
    write(
        &root,
        "artifacts/invariants/evidence/trial-0/node.log",
        "role=leader",
    )?;
    write(&root, "artifacts/invariants/evidence/producer", "producer")?;
    initialize_fixture_repository(&root)?;

    let mut bundle = bundle();
    bind_serialized_bundle(&mut bundle, &root, &workspace)?;
    let result_path = root.with_extension("result.json");
    fs::write(&result_path, serde_json::to_vec_pretty(&bundle)?)?;
    let value: serde_json::Value = serde_json::from_slice(&fs::read(&result_path)?)?;
    crate::evidence::validate_result_value(&value)?;
    let serialized: ResultBundle = serde_json::from_value(value)?;
    let catalog = crate::Catalog::load(&root.join(&serialized.execution.plan.registry.path))?;
    let (diagnostics, _artifact_guard) = crate::verification::verify_bundle_artifacts(
        &serialized,
        &root,
        &root,
        &catalog,
        &serialized.profile,
        &serialized.runner,
    )?;
    assert_eq!(diagnostics.len(), 1);
    assert!(diagnostics[0].contains("counterexample alongside a harness error"));
    Ok(SerializedCounterexampleFixture {
        root,
        result_path,
        bundle: serialized,
        diagnostic: diagnostics.into_iter().next().expect("one diagnostic"),
    })
}

fn canonical_counterexample_intake(
    catalog: &crate::Catalog,
    manifest: &crate::ProfileManifest,
    fixture: &SerializedCounterexampleFixture,
) -> Result<crate::verification::EvidenceIntake, crate::verification::AggregateError> {
    let mut bundles = crate::tests::passing_bundles_for_profile(catalog, manifest, "nightly");
    let maelstrom = bundles
        .iter_mut()
        .find(|candidate| candidate.runner == "maelstrom")
        .expect("canonical nightly Maelstrom receipt");
    assert_eq!(
        maelstrom.execution.checks.len(),
        6,
        "{:?}",
        maelstrom
            .execution
            .checks
            .iter()
            .map(|check| &check.check_id)
            .collect::<Vec<_>>()
    );
    // One result per clause-bound Maelstrom record; the lin-kv scenario carries
    // both RD-04 clauses since one run evidences the dispatch floor and the
    // application floor together.
    assert_eq!(maelstrom.results.len(), 20);
    let execution_id = maelstrom
        .results
        .iter()
        .find(|result| result.invariant_id == "RD-06")
        .expect("nightly receipt contains RD-06")
        .execution_id
        .clone();
    for result in maelstrom
        .results
        .iter_mut()
        .filter(|result| result.execution_id == execution_id)
    {
        result.artifacts = fixture.bundle.results[0].artifacts.clone();
        if result.invariant_id == "RD-06" {
            result.status = EvidenceStatus::Fail;
            result.classification = Some(FailureClassification::InvariantViolation);
            result.message = fixture.bundle.results[0].message.clone();
        } else {
            result.status = EvidenceStatus::Incomplete;
            result.classification = Some(FailureClassification::CoverageNotReached);
            result.message = Some("counterexample stopped the shared scenario".to_owned());
        }
    }
    maelstrom
        .execution
        .checks
        .iter_mut()
        .find(|check| check.execution_id == execution_id)
        .expect("RD-06 check receipt")
        .completion = CheckCompletion::Counterexample;
    let maelstrom = maelstrom.clone();
    let plan = crate::tests::plan_receipt(manifest, "nightly");
    let request = crate::verification::VerificationRequest::new(
        catalog,
        manifest,
        &plan,
        "abc",
        &fixture.root,
    );
    crate::verification::verify_receipts_for_test(
        request,
        std::slice::from_ref(&maelstrom),
        vec![crate::verification::IntakeDefect::unverifiable(
            fixture.diagnostic.clone(),
        )],
    )
}

fn materialize_checkout(root: &Path, workspace: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let tracked = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .current_dir(workspace)
        .output()?;
    if !tracked.status.success() {
        return Err(format!(
            "git source inventory failed while materializing Maelstrom fixture: {}",
            String::from_utf8_lossy(&tracked.stderr)
        )
        .into());
    }
    for path in tracked.stdout.split(|byte| *byte == 0) {
        if path.is_empty() {
            continue;
        }
        let path = std::str::from_utf8(path)?;
        let source = workspace.join(path);
        if !source.try_exists()? {
            continue;
        }
        let destination = root.join(path);
        fs::create_dir_all(destination.parent().ok_or("plan input has no parent")?)?;
        fs::copy(source, destination)?;
    }
    write(root, ".gitignore", "/artifacts/invariants/\n/target/\n")?;
    Ok(())
}

fn initialize_fixture_repository(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for arguments in [
        vec!["init", "-q"],
        vec!["add", "."],
        vec![
            "-c",
            "user.name=Rafter Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            "test: materialize Maelstrom evidence fixture",
        ],
    ] {
        let output = Command::new("git")
            .args(&arguments)
            .current_dir(root)
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "git {arguments:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
    }
    Ok(())
}

fn bind_serialized_bundle(
    bundle: &mut ResultBundle,
    root: &Path,
    workspace: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for (path, input) in [
        (
            "verification/raft-invariants.yaml",
            &mut bundle.execution.plan.registry,
        ),
        (
            "verification/raft-invariant-profiles.json",
            &mut bundle.execution.plan.manifest,
        ),
        (
            "verification/invariant-result-schema.json",
            &mut bundle.execution.plan.result_schema,
        ),
        (
            "verification/invariant-verdict-schema.json",
            &mut bundle.execution.plan.verdict_schema,
        ),
    ] {
        let bytes = fs::read(workspace.join(path))?;
        input.path = path.to_owned();
        input.sha256 = format!("{:x}", Sha256::digest(&bytes));
        input.size_bytes = bytes.len() as u64;
    }

    let mut source = crate::producer::source::capture_for_layer_at("tests", root)?;
    source.build_profile = "maelstrom-debug".to_owned();
    source.features.clear();
    source.tools = ["java", "maelstrom", "dot", "gnuplot"]
        .into_iter()
        .map(|name| {
            (
                name.to_owned(),
                ToolReceipt {
                    version: format!("{name} fixture"),
                    sha256: "0".repeat(64),
                },
            )
        })
        .collect();
    source.process_runtime = crate::receipt::fixture_process_runtime(true);
    bundle.execution.source = source;

    let producer = bound_artifact(
        root,
        "artifacts/invariants/evidence/producer",
        "producer-binary",
    )?;
    bundle.execution.artifacts = vec![producer.clone()];
    bundle.execution.producer.executable = producer.clone();
    let repository = fs::canonicalize(root)?;
    bundle.execution.invocation.current_dir = repository.to_string_lossy().into_owned();
    bundle.execution.invocation.program_sha256 = producer.sha256.clone();
    bundle.execution.invocation.program =
        crate::provenance::image::image_path(&repository, &producer.sha256)
            .to_string_lossy()
            .into_owned();
    bundle.execution.invocation.environment = process_environment();
    bundle.execution.invocation.environment_sha256 =
        crate::provenance::invocation::digest_environment(&bundle.execution.invocation.environment)
            .expect("valid fixture environment");

    let check = &mut bundle.execution.checks[0];
    check.completion = CheckCompletion::Counterexample;
    check.observations.insert("valid_trials".to_owned(), 0);
    check.observations.insert("invalid_trials".to_owned(), 1);
    for artifact in &mut check.artifacts {
        *artifact = bound_artifact(root, &artifact.path, &artifact.kind)?;
    }
    let jar = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "maelstrom-tool-jar")
        .ok_or("Maelstrom fixture omitted jar")?;
    bundle
        .execution
        .plan
        .contract
        .runners
        .get_mut("maelstrom")
        .ok_or("Maelstrom fixture omitted runner contract")?
        .configuration
        .insert("maelstrom_jar_sha256".to_owned(), jar.sha256.clone());

    let replay = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "maelstrom-process-log")
        .ok_or("Maelstrom fixture omitted process log")?
        .clone();
    let result = &mut bundle.results[0];
    result.status = EvidenceStatus::Fail;
    result.classification = Some(FailureClassification::InvariantViolation);
    result.message = Some("Maelstrom reported a non-linearizable client history".to_owned());
    result.artifacts = vec![replay];
    Ok(())
}

fn bound_artifact(
    root: &Path,
    path: &str,
    kind: &str,
) -> Result<ArtifactRef, Box<dyn std::error::Error>> {
    let bytes = fs::read(root.join(path))?;
    Ok(ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        size_bytes: bytes.len() as u64,
    })
}
