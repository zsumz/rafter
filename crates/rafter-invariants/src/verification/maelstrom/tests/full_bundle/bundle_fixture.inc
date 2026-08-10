fn bundle() -> ResultBundle {
    let execution_id = "maelstrom-base".to_owned();
    let process_environment = process_environment();
    ResultBundle {
        schema_version: crate::evidence::RESULT_SCHEMA_VERSION,
        runner: "maelstrom".to_owned(),
        profile: "nightly".to_owned(),
        source_ref: "abc".to_owned(),
        execution: ExecutionReceipt {
            plan: ExecutionPlanReceipt {
                schema_version: PLAN_SCHEMA_VERSION,
                profile: "nightly".to_owned(),
                registry: plan_input("verification/raft-invariants.yaml"),
                manifest: plan_input("verification/raft-invariant-profiles.json"),
                result_schema: plan_input("verification/invariant-result-schema.json"),
                verdict_schema: plan_input("verification/invariant-verdict-schema.json"),
                contract: maelstrom_contract(),
            },
            invocation: InvocationReceipt {
                program: format!(
                    "/workspace/rafter/target/rafter-invariants/producer-images/{}/rafter-invariants",
                    "0".repeat(64)
                ),
                program_sha256: "0".repeat(64),
                arguments: vec!["run".to_owned()],
                current_dir: "/workspace/rafter".to_owned(),
                environment: process_environment.clone(),
                environment_sha256: crate::provenance::invocation::digest_environment(
                    &process_environment,
                )
                .expect("valid fixture environment"),
                launchers: Vec::new(),
            },
            producer: crate::ProducerBindingReceipt {
                binding: crate::provenance::image::PRODUCER_BINDING.to_owned(),
                executable: artifact(
                    "artifacts/invariants/evidence/producer",
                    "producer-binary",
                ),
            },
            source: source(),
            checks: vec![maelstrom_check(&execution_id)],
            duration_ms: 1,
            peak_rss_kib: 1,
            artifacts: Vec::new(),
        },
        results: vec![EvidenceResult {
            invariant_id: "RD-06".to_owned(),
            evidence_id: "RD-06/test".to_owned(),
            execution_id,
            status: EvidenceStatus::Pass,
            classification: None,
            message: None,
            artifacts: Vec::new(),
        }],
    }
}

fn maelstrom_contract() -> ProfileContract {
    let configuration = BTreeMap::from([
        ("trials".to_owned(), "1".to_owned()),
        ("maelstrom_jar_sha256".to_owned(), "0".repeat(64)),
        ("duration_seconds".to_owned(), "5".to_owned()),
        ("rate".to_owned(), "10".to_owned()),
    ]);
    ProfileContract {
        description: "test".to_owned(),
        evidence_policy: crate::EvidencePolicy::AllMatchingRegistryEvidence,
        clause_policy: crate::ClausePolicy::AllRequiredClauses,
        required_clause_strength: crate::RequiredClauseStrength::Direct,
        required_layers: vec![crate::EvidenceLayer::Maelstrom],
        required_strengths: vec![crate::EvidenceStrength::E2e],
        canonical_minimum_independent_layers: 2,
        runners: BTreeMap::from([(
            "maelstrom".to_owned(),
            RunnerContract {
                producer: "test".to_owned(),
                command: vec!["test".to_owned()],
                configuration,
                simulator_checks: BTreeMap::new(),
                minimum_observed_checks: 1,
                require_peak_rss: true,
            },
        )]),
    }
}

fn maelstrom_check(execution_id: &str) -> CheckReceipt {
    CheckReceipt {
        execution_id: execution_id.to_owned(),
        check_id: "maelstrom/base".to_owned(),
        evidence_ids: vec!["RD-06/test".to_owned()],
        completion: CheckCompletion::Completed,
        observations: observations(),
        simulator_liveness: None,
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts: [
            ("inputs/runner", "maelstrom-runner"),
            ("inputs/binary", "maelstrom-binary"),
            ("inputs/maelstrom.jar", "maelstrom-tool-jar"),
            ("trial-0/results.edn", "maelstrom-results"),
            ("trial-0/process.json", "maelstrom-process-log"),
            ("trial-0/node.log", "maelstrom-node-log"),
        ]
        .into_iter()
        .map(|(path, kind)| artifact(&format!("artifacts/invariants/evidence/{path}"), kind))
        .collect(),
    }
}

fn process_log(
    root: &std::path::Path,
    exit_code: i32,
    stderr: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let state_dir =
        fs::canonicalize(root.join("target/rafter-invariants/maelstrom/abc/nightly/base/trial-0"))?;
    let durable = state_dir.join("durable");
    let environment = process_environment()
        .into_iter()
        .chain(BTreeMap::from([
            (
                "RAFTER_MAELSTROM_ROOT".to_owned(),
                durable.to_string_lossy().into_owned(),
            ),
            (
                "RAFTER_MAELSTROM_SCRIPT_DIR".to_owned(),
                fs::canonicalize(root.join("scripts"))?
                    .to_string_lossy()
                    .into_owned(),
            ),
            ("RAFTER_MAELSTROM_TIME_LIMIT".to_owned(), "5".to_owned()),
            ("RAFTER_MAELSTROM_RATE".to_owned(), "10".to_owned()),
            ("RAFTER_MAELSTROM_CONCURRENCY".to_owned(), "6".to_owned()),
        ]))
        .collect::<BTreeMap<_, _>>();
    let mut invocation = crate::producer::process::expected_invocation(
        fs::canonicalize(root.join("scripts/maelstrom-lin-kv"))?
            .to_str()
            .ok_or("script path is not UTF-8")?,
        &["--test-count".into(), "1".into()],
        &environment,
        &state_dir,
    )?;
    invocation.launchers = crate::receipt::fixture_launchers(true);
    Ok(serde_json::json!({
        "schema_version": 3,
        "label": "base",
        "invocation": invocation,
        "exit_code": exit_code,
        "timed_out": false,
        "duration_ms": 1,
        "peak_rss_kib": 1,
        "stdout": "",
        "stderr": stderr,
    })
    .to_string())
}

fn prepare_state(root: &std::path::Path) -> Result<(), std::io::Error> {
    fs::create_dir_all(
        root.join("target/rafter-invariants/maelstrom/abc/nightly/base/trial-0/durable"),
    )?;
    write(
        root,
        "artifacts/invariants/evidence/inputs/maelstrom.jar",
        "jar",
    )
}

fn plan_input(path: &str) -> PlanInput {
    PlanInput {
        path: path.to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    }
}

fn observations() -> BTreeMap<String, u64> {
    BTreeMap::from([
        ("trials".to_owned(), 1),
        ("valid_trials".to_owned(), 1),
        ("invalid_trials".to_owned(), 0),
        ("operation_count".to_owned(), 9),
        ("ok_count".to_owned(), 6),
        ("read_ok".to_owned(), 2),
        ("write_ok".to_owned(), 3),
        ("cas_ok".to_owned(), 1),
        ("membership_enter".to_owned(), 0),
        ("membership_leave".to_owned(), 0),
        ("membership_complete".to_owned(), 0),
        ("restarts".to_owned(), 0),
        ("post_restart_progress".to_owned(), 0),
        ("crashpoints".to_owned(), 0),
        ("post_crash_progress".to_owned(), 0),
        ("snapshots_compacted".to_owned(), 0),
        ("snapshots_applied".to_owned(), 0),
        ("post_restart_snapshots_applied".to_owned(), 0),
        ("lease_fast_path_read_ok".to_owned(), 0),
        ("lease_read_buffered".to_owned(), 0),
        ("lease_expired_while_leader".to_owned(), 0),
        ("lease_post_expiry_released".to_owned(), 0),
        ("lease_post_expiry_handler".to_owned(), 0),
        ("lease_post_expiry_unavailable".to_owned(), 0),
        ("lease_post_expiry_read_served".to_owned(), 0),
        ("lease_post_expiry_renewed".to_owned(), 0),
        ("lease_post_expiry_unexpected_error".to_owned(), 0),
        ("lease_duplicate_terminal".to_owned(), 0),
        ("lease_coverage_lost".to_owned(), 0),
        ("lease_history_probe_matches".to_owned(), 0),
        ("lease_history_probe_mismatches".to_owned(), 0),
        ("lease_sequence_complete".to_owned(), 0),
        ("lease_sequence_invalid".to_owned(), 0),
    ])
}

fn artifact(path: &str, kind: &str) -> ArtifactRef {
    ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_owned(),
        sha256: if kind == "maelstrom-runner" {
            format!("{:x}", Sha256::digest(b"runner"))
        } else {
            "0".repeat(64)
        },
        size_bytes: 1,
    }
}

fn source() -> SourceReceipt {
    SourceReceipt {
        commit: "abc".to_owned(),
        tree: "tree".to_owned(),
        materialization: SourceMaterializationReceipt {
            contract: "git-head-worktree-raw-v1".to_owned(),
            sha256: "0".repeat(64),
            tracked_entries: 1,
            submodules: 0,
        },
        cargo_lock_sha256: "0".repeat(64),
        cargo: "cargo".to_owned(),
        cargo_sha256: "0".repeat(64),
        cargo_config_sha256: "0".repeat(64),
        rustc: "rustc".to_owned(),
        rustc_sha256: "0".repeat(64),
        target: "target".to_owned(),
        build_profile: "test".to_owned(),
        features: Vec::new(),
        tools: BTreeMap::new(),
        process_runtime: crate::receipt::fixture_process_runtime(true),
        environment_sha256: crate::provenance::invocation::digest_environment(
            &process_environment(),
        )
        .expect("valid fixture environment"),
        clean: true,
    }
}

fn temporary_root() -> Result<PathBuf, std::io::Error> {
    let root = Path::new("target/rafter-invariants/tests").join(format!(
        "maelstrom-artifact-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }
    fs::create_dir_all(&root)?;
    Ok(root)
}

fn process_environment() -> BTreeMap<String, String> {
    crate::producer::process::base_environment()
}

fn write(root: &std::path::Path, path: &str, source: &str) -> Result<(), std::io::Error> {
    let path = root.join(path);
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "artifact has no parent")
    })?;
    fs::create_dir_all(parent)?;
    fs::write(path, source)
}
