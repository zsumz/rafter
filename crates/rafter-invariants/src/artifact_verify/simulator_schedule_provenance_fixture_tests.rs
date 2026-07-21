use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, MutexGuard,
    },
};

use sha2::{Digest, Sha256};

use super::super::simulator_compiler_artifact_executable;

static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
static FIXTURE_SERIAL: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy)]
pub(super) enum RuntimeDefect {
    ProvenanceOnly,
    Timeout,
    MalformedEvent,
    LaunchFailure,
    PassExitOne,
    CounterexampleExitOne,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ProvenanceSubstitution {
    Package,
    Source,
    TargetName,
    TargetKind,
    Executable,
    CompileRoot,
}

const SIMULATOR_FIXTURE_SOURCE: &str = r##"use std::{
    io::{self, Write as _},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

static TERMINATED: AtomicBool = AtomicBool::new(false);

unsafe extern "C" {
    fn signal(signal: i32, handler: extern "C" fn(i32)) -> usize;
}

extern "C" fn handle_term(_signal: i32) {
    TERMINATED.store(true, Ordering::SeqCst);
}

fn main() {
    unsafe {
        let _ = signal(15, handle_term);
    }
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "{}", "RAFTER_FIXTURE_READY").expect("write readiness marker");
    writeln!(stdout, "{}", r#"RAFTER_EVENT __EVENT__"#)
        .expect("write semantic event");
__MALFORMED_EVENT__
    stdout.flush().expect("flush fixture output");
    drop(stdout);
__TERMINATION_WAIT__
}
"##;

pub(super) struct SimulatorFixture {
    pub(super) root: PathBuf,
    pub(super) producer_root: PathBuf,
    pub(super) bundle_path: PathBuf,
    timeout_output_dir: PathBuf,
    pub(super) catalog: crate::Catalog,
    pub(super) manifest: crate::ProfileManifest,
    _serial: MutexGuard<'static, ()>,
}

struct PendingSimulatorFixture {
    root: PathBuf,
    producer_root: PathBuf,
    bundle_path: PathBuf,
    timeout_output_dir: PathBuf,
    armed: bool,
}

impl PendingSimulatorFixture {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingSimulatorFixture {
    fn drop(&mut self) {
        if self.armed {
            cleanup_fixture_artifacts(
                &self.root,
                &self.producer_root,
                &self.bundle_path,
                &self.timeout_output_dir,
            );
        }
    }
}

impl Drop for SimulatorFixture {
    fn drop(&mut self) {
        cleanup_fixture_artifacts(
            &self.root,
            &self.producer_root,
            &self.bundle_path,
            &self.timeout_output_dir,
        );
    }
}

fn cleanup_fixture_artifacts(
    root: &Path,
    producer_root: &Path,
    bundle_path: &Path,
    timeout_output_dir: &Path,
) {
    let _ = fs::remove_dir_all(timeout_output_dir);
    let _ = fs::remove_file(bundle_path);
    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(producer_root);
}

impl SimulatorFixture {
    pub(super) fn serialized_bundle(&self) -> crate::ResultBundle {
        serde_json::from_slice(
            &fs::read(&self.bundle_path).expect("read serialized simulator bundle"),
        )
        .expect("decode serialized simulator bundle")
    }

    pub(super) fn substitute_provenance(&self, substitution: ProvenanceSubstitution) {
        let mut bundle = self.serialized_bundle();
        let compile_path = bundle
            .execution
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "compile-log")
            .expect("compile artifact")
            .path
            .clone();
        let source =
            fs::read_to_string(self.root.join(&compile_path)).expect("read serialized compile log");
        let processes = crate::evidence::format::process::parse_combined_processes(&source)
            .expect("parse serialized compile log");
        let [process] = processes.as_slice() else {
            panic!("serialized compile log must contain exactly one process")
        };
        let mut invocation = process.invocation.clone();
        let stdout = if matches!(substitution, ProvenanceSubstitution::CompileRoot) {
            invocation.current_dir = self
                .producer_root
                .with_extension("substituted-root")
                .to_string_lossy()
                .into_owned();
            process.stdout.clone()
        } else {
            substitute_compiler_message(&process.stdout, &self.producer_root, substitution)
        };
        let rewritten = framed_process_log(
            "simulator compile",
            &invocation,
            process.timed_out,
            &stdout,
            &process.stderr,
        );
        fs::write(self.root.join(&compile_path), rewritten.as_bytes())
            .expect("rewrite substituted compile log");
        let artifact = bundle
            .execution
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.path == compile_path)
            .expect("serialized compile artifact");
        artifact.sha256 = format!("{:x}", Sha256::digest(rewritten.as_bytes()));
        artifact.size_bytes = rewritten.len() as u64;
        fs::write(
            &self.bundle_path,
            format!(
                "{}\n",
                serde_json::to_string_pretty(&bundle)
                    .expect("serialize substituted simulator bundle")
            ),
        )
        .expect("write substituted simulator bundle");
    }
}

fn substitute_compiler_message(
    stdout: &str,
    producer_root: &Path,
    substitution: ProvenanceSubstitution,
) -> String {
    let mut replacements = 0_usize;
    let rewritten = stdout
        .lines()
        .map(|line| {
            let Ok(mut message) = serde_json::from_str::<serde_json::Value>(line) else {
                return line.to_owned();
            };
            if message["reason"] != "compiler-artifact"
                || message["target"]["name"] != "rafter-model-check-fast"
            {
                return line.to_owned();
            }
            replacements += 1;
            match substitution {
                ProvenanceSubstitution::Package => {
                    message["package_id"] = serde_json::json!(format!(
                        "path+file://{}#0.0.1",
                        producer_root.join("crates/rafter-alt").display()
                    ));
                }
                ProvenanceSubstitution::Source => {
                    message["target"]["src_path"] = serde_json::json!(producer_root
                        .join("crates/rafter-sim/src/bin/rafter-model-check-substituted.rs"));
                }
                ProvenanceSubstitution::TargetName => {
                    message["target"]["name"] = serde_json::json!("rafter-model-check-substituted");
                }
                ProvenanceSubstitution::TargetKind => {
                    message["target"]["kind"] = serde_json::json!(["bin", "test"]);
                }
                ProvenanceSubstitution::Executable => {
                    let executable = Path::new(
                        message["executable"]
                            .as_str()
                            .expect("compiler executable path"),
                    );
                    message["executable"] = serde_json::json!(executable
                        .parent()
                        .expect("compiler executable parent")
                        .join("rafter-model-check-substituted"));
                }
                ProvenanceSubstitution::CompileRoot => unreachable!("handled by caller"),
            }
            message.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(replacements, 1, "substitute one simulator compiler message");
    format!("{rewritten}\n")
}

struct CompileFixture {
    binary_path: PathBuf,
    binary_artifact: crate::ArtifactRef,
    compile_artifact: crate::ArtifactRef,
}

struct RuntimeFixture {
    fast_artifact: crate::ArtifactRef,
    producer_artifact: crate::ArtifactRef,
    duration_ms: u64,
    peak_rss_kib: u64,
    checks: Vec<crate::CheckReceipt>,
    results: Vec<crate::EvidenceResult>,
}

struct RuntimeFixtureInput<'a> {
    root: &'a Path,
    output_dir: &'a Path,
    source_ref: &'a str,
    current_dir: &'a Path,
    environment: &'a BTreeMap<String, String>,
    process_runtime: &'a BTreeMap<String, crate::ExecutableReceipt>,
    compile: &'a CompileFixture,
}

pub(super) fn materialize_fixture(defect: RuntimeDefect) -> SimulatorFixture {
    materialize_fixture_with_roots(defect, false)
}

pub(super) fn materialize_cross_root_fixture(defect: RuntimeDefect) -> SimulatorFixture {
    materialize_fixture_with_roots(defect, true)
}

fn materialize_fixture_with_roots(defect: RuntimeDefect, cross_root: bool) -> SimulatorFixture {
    let serial = FIXTURE_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let workspace = fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .expect("canonical workspace root");
    let fixture_id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    let fixture_suffix = format!("{}-{fixture_id}", std::process::id());
    let fixture_base = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/rafter-invariants/tests")
        .join(format!("simulator-provenance-{fixture_suffix}"));
    let producer_root = fixture_base.with_extension("producer-root-a");
    let root = if cross_root {
        fixture_base.with_extension("aggregate-root-b")
    } else {
        producer_root.clone()
    };
    let bundle_path = fixture_base.with_extension("bundle.json");
    let timeout_output_dir = Path::new("target/rafter-invariants/tests")
        .join(format!("simulator-loader-real-timeout-{fixture_suffix}"));
    cleanup_fixture_artifacts(&root, &producer_root, &bundle_path, &timeout_output_dir);
    let pending = PendingSimulatorFixture {
        root: root.clone(),
        producer_root: producer_root.clone(),
        bundle_path: bundle_path.clone(),
        timeout_output_dir: timeout_output_dir.clone(),
        armed: true,
    };
    fs::create_dir_all(&producer_root).expect("create simulator provenance fixture");
    let current_dir = fs::canonicalize(&producer_root).expect("canonical producer fixture root");

    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle");
    copy_fixture_plan_inputs(&workspace, &producer_root, &mut bundle);
    materialize_fixture_checkout(&workspace, &producer_root, defect);
    initialize_fixture_repository(&producer_root);
    let source = crate::producer::source::capture_for_layer_at("simulator", &producer_root)
        .expect("capture clean fixture source identity");
    bundle.source_ref = source.commit.clone();
    bundle.execution.source = source;
    let environment = crate::producer::process::base_environment();
    bundle.execution.invocation.environment = environment.clone();
    bundle.execution.invocation.environment_sha256 =
        crate::provenance::invocation::digest_environment(&environment)
            .expect("digest fixture producer environment");
    let source_ref = bundle.source_ref.clone();
    let compile = materialize_compile_fixture(
        &producer_root,
        &current_dir,
        &source_ref,
        &environment,
        &bundle.execution.source.process_runtime,
    );
    let runtime = materialize_runtime_fixture(
        &RuntimeFixtureInput {
            root: &producer_root,
            output_dir: &timeout_output_dir,
            source_ref: &source_ref,
            current_dir: &current_dir,
            environment: &environment,
            process_runtime: &bundle.execution.source.process_runtime,
            compile: &compile,
        },
        defect,
    );
    bind_fixture_evidence(&mut bundle, &current_dir, &compile, &runtime);
    assert!(
        crate::provenance::source::source_environment_matches_digest(
            &environment,
            &bundle.execution.source.environment_sha256
        )
    );
    fs::write(
        &bundle_path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&bundle).expect("serialize timeout bundle")
        ),
    )
    .expect("write timeout bundle outside fixture checkout");
    if cross_root {
        fs::rename(&producer_root, &root).expect("move producer checkout A to aggregate root B");
    }
    let fixture = SimulatorFixture {
        root,
        producer_root,
        bundle_path,
        timeout_output_dir,
        catalog,
        manifest,
        _serial: serial,
    };
    pending.disarm();
    fixture
}

fn copy_fixture_plan_inputs(workspace: &Path, root: &Path, bundle: &mut crate::ResultBundle) {
    bundle.execution.plan.registry =
        copy_plan_input(workspace, root, "verification/raft-invariants.yaml");
    bundle.execution.plan.manifest =
        copy_plan_input(workspace, root, "verification/raft-invariant-profiles.json");
    bundle.execution.plan.result_schema =
        copy_plan_input(workspace, root, "verification/invariant-result-schema.json");
    bundle.execution.plan.verdict_schema = copy_plan_input(
        workspace,
        root,
        "verification/invariant-verdict-schema.json",
    );
}

fn materialize_compile_fixture(
    root: &Path,
    current_dir: &Path,
    source_ref: &str,
    environment: &BTreeMap<String, String>,
    process_runtime: &BTreeMap<String, crate::ExecutableReceipt>,
) -> CompileFixture {
    let cargo_sha256 = executable_sha256("cargo");
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let target_dir = current_dir
        .join(format!(
            "target/rafter-invariants/simulator-build/{source_prefix}/pr"
        ))
        .to_string_lossy()
        .into_owned();
    let mut compile_environment = environment.clone();
    compile_environment.insert("CARGO_TARGET_DIR".to_owned(), target_dir.clone());
    let arguments = [
        "build",
        "--release",
        "--locked",
        "-p",
        "rafter-sim",
        "--bin",
        "rafter-model-check-fast",
        "--message-format=json-render-diagnostics",
    ];
    let output = Command::new("cargo")
        .args(arguments)
        .env_clear()
        .envs(&compile_environment)
        .current_dir(current_dir)
        .output()
        .expect("execute controlled simulator Cargo compile");
    assert!(
        output.status.success(),
        "controlled simulator Cargo compile failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("Cargo stdout is UTF-8");
    let stderr = String::from_utf8(output.stderr).expect("Cargo stderr is UTF-8");
    let invocation = crate::InvocationReceipt {
        program: "cargo".to_owned(),
        program_sha256: cargo_sha256,
        arguments: arguments.map(str::to_owned).to_vec(),
        current_dir: current_dir.to_string_lossy().into_owned(),
        environment_sha256: crate::provenance::invocation::digest_environment(&compile_environment)
            .expect("valid fixture environment"),
        environment: compile_environment,
        launchers: source_bound_launchers(process_runtime),
    };
    let absolute_target_dir = PathBuf::from(&target_dir);
    let binary_path = simulator_compiler_artifact_executable(
        stdout.as_bytes(),
        current_dir,
        current_dir,
        &absolute_target_dir,
    )
    .expect("controlled Cargo output binds the fixture simulator");
    let binary_bytes = fs::read(&binary_path).expect("read compiled simulator fixture binary");
    let log = framed_process_log("simulator compile", &invocation, false, &stdout, &stderr);
    CompileFixture {
        binary_path,
        binary_artifact: write_fixture_artifact(
            root,
            "artifacts/invariants/rafter-model-check-fast",
            "simulator-binary",
            &binary_bytes,
        ),
        compile_artifact: write_fixture_artifact(
            root,
            "artifacts/invariants/compile.log",
            "compile-log",
            log.as_bytes(),
        ),
    }
}

fn materialize_runtime_fixture(
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
    let (catalog, _) = crate::tests::loaded();
    let (checks, results) = crate::producer::evaluate_model_fixture(&catalog, "pr", &model)
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

fn source_bound_launchers(
    process_runtime: &BTreeMap<String, crate::ExecutableReceipt>,
) -> Vec<crate::LauncherReceipt> {
    crate::receipt::fixture_launchers(false)
        .into_iter()
        .map(|mut launcher| {
            launcher.executable = process_runtime
                .get(&launcher.runtime)
                .unwrap_or_else(|| panic!("missing fixture runtime {}", launcher.runtime))
                .clone();
            launcher
        })
        .collect()
}

fn bind_fixture_evidence(
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
        // The fixture runs the simulator process but deliberately does not run a detector test.
        // Preserve every semantic receipt while binding the detector failures to the real compile
        // artifact, so later process failures cannot erase an earlier counterexample.
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

fn materialize_fixture_checkout(workspace: &Path, root: &Path, defect: RuntimeDefect) {
    fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"crates/rafter\", \"crates/rafter-sim\", \"crates/rafter-invariant-test\", \"crates/rafter-invariant-test-macros\"]\nresolver = \"2\"\n",
    )
    .expect("write fixture workspace manifest");
    let rafter_dir = root.join("crates/rafter");
    fs::create_dir_all(&rafter_dir).expect("create fixture rafter package");
    fs::write(
        rafter_dir.join("Cargo.toml"),
        "[package]\nname = \"rafter\"\nversion = \"0.0.1\"\nedition = \"2021\"\n\n[dev-dependencies]\nrafter-invariant-test = { path = \"../rafter-invariant-test\" }\n",
    )
    .expect("write fixture rafter manifest");
    copy_source_tree(
        &workspace.join("crates/rafter/src"),
        &rafter_dir.join("src"),
    );
    let oracle_dir = root.join("crates/rafter-invariant-test");
    fs::create_dir_all(oracle_dir.join("src")).expect("create fixture oracle package");
    fs::write(
        oracle_dir.join("Cargo.toml"),
        "[package]\nname = \"rafter-invariant-test\"\nversion = \"0.0.1\"\nedition = \"2021\"\n\n[dependencies]\nrafter-invariant-test-macros = { path = \"../rafter-invariant-test-macros\" }\n",
    )
    .expect("write fixture oracle manifest");
    fs::write(oracle_dir.join("src/lib.rs"), "").expect("write fixture oracle library source");
    let macros_dir = root.join("crates/rafter-invariant-test-macros");
    fs::create_dir_all(macros_dir.join("src")).expect("create fixture oracle macros package");
    fs::write(
        macros_dir.join("Cargo.toml"),
        "[package]\nname = \"rafter-invariant-test-macros\"\nversion = \"0.0.1\"\nedition = \"2021\"\n\n[lib]\nproc-macro = true\n",
    )
    .expect("write fixture oracle macros manifest");
    fs::write(macros_dir.join("src/lib.rs"), "").expect("write fixture oracle macros source");
    let package_dir = root.join("crates/rafter-sim");
    fs::create_dir_all(package_dir.join("src/bin")).expect("create fixture package source tree");
    fs::write(
        package_dir.join("Cargo.toml"),
        concat!(
            "[package]\n",
            "name = \"rafter-sim\"\n",
            "version = \"0.0.1\"\n",
            "edition = \"2021\"\n\n",
            "autolib = false\n\n",
            "[[bin]]\n",
            "name = \"rafter-model-check-fast\"\n",
            "path = \"src/bin/rafter-model-check-fast.rs\"\n",
        ),
    )
    .expect("write fixture package manifest");
    copy_source_tree(
        &workspace.join("crates/rafter-sim/src"),
        &package_dir.join("src"),
    );
    fs::write(
        package_dir.join("src/bin/rafter-model-check-fast.rs"),
        simulator_fixture_source(defect),
    )
    .expect("write fixture simulator source");
    fs::write(root.join(".gitignore"), "/artifacts/\n/target/\n")
        .expect("ignore generated fixture evidence");
    let environment = crate::producer::process::base_environment();
    let output = Command::new("cargo")
        .args(["generate-lockfile", "--offline"])
        .env_clear()
        .envs(environment)
        .current_dir(root)
        .output()
        .expect("generate fixture Cargo.lock");
    assert!(
        output.status.success(),
        "generate fixture Cargo.lock failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn copy_source_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("create copied source directory");
    for entry in fs::read_dir(source).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type().expect("read source entry type").is_dir() {
            copy_source_tree(&source_path, &destination_path);
        } else {
            fs::copy(&source_path, &destination_path).expect("copy source fixture file");
        }
    }
}

fn simulator_fixture_source(defect: RuntimeDefect) -> String {
    let event = if matches!(defect, RuntimeDefect::PassExitOne) {
        serde_json::json!({
            "event": "exhaustive-check",
            "check_id": "raft-commit",
            "status": "pass",
            "unique_protocol_states": 20_000,
            "unique_verifier_states": 20_000,
            "observations": {
                "commit_floor_advances": 1,
                "commit_index_within_local_log_bounds_checks": 1,
            },
        })
    } else {
        serde_json::json!({
            "event": "check-failure",
            "event_version": 2,
            "check_id": "raft-commit",
            "status": "fail",
            "classification": "invariant-violation",
            "invariant_id": "CM-02",
            "invariant": "CM-02 commit requires effective quorum",
            "message": if matches!(defect, RuntimeDefect::CounterexampleExitOne) {
                "real exit-one fixture found a counterexample"
            } else {
                "real timeout fixture found a counterexample"
            },
            "unique_protocol_states": 1,
            "unique_verifier_states": 1,
        })
    };
    let malformed = if matches!(defect, RuntimeDefect::MalformedEvent) {
        "    writeln!(stdout, \"{}\", \"RAFTER_EVENT {not-json}\")\n        .expect(\"write malformed event\");"
    } else {
        ""
    };
    let termination_wait = match defect {
        RuntimeDefect::Timeout | RuntimeDefect::MalformedEvent => concat!(
            "    while !TERMINATED.load(Ordering::SeqCst) {\n",
            "        thread::sleep(Duration::from_millis(10));\n",
            "    }",
        ),
        RuntimeDefect::ProvenanceOnly | RuntimeDefect::LaunchFailure => "",
        RuntimeDefect::PassExitOne | RuntimeDefect::CounterexampleExitOne => {
            "    std::process::exit(1);"
        }
    };
    SIMULATOR_FIXTURE_SOURCE
        .replace("__EVENT__", &event.to_string())
        .replace("__MALFORMED_EVENT__", malformed)
        .replace("__TERMINATION_WAIT__", termination_wait)
}

fn copy_plan_input(workspace: &Path, root: &Path, path: &str) -> crate::PlanInput {
    let bytes = fs::read(workspace.join(path)).expect("read plan input");
    let destination = root.join(path);
    fs::create_dir_all(destination.parent().expect("plan input parent"))
        .expect("create plan input parent");
    fs::write(destination, &bytes).expect("write plan input");
    crate::PlanInput {
        path: path.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        size_bytes: bytes.len() as u64,
    }
}
fn executable_sha256(name: &str) -> String {
    let path = env::split_paths(&env::var_os("PATH").expect("PATH is configured"))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| panic!("{name} is present on PATH"));
    format!(
        "{:x}",
        Sha256::digest(fs::read(path).expect("read executable"))
    )
}

fn initialize_fixture_repository(root: &Path) {
    git(root, &["init", "-q"]);
    git(root, &["add", "."]);
    git(
        root,
        &[
            "-c",
            "user.name=Rafter Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            "test: materialize timeout evidence fixture",
        ],
    );
}

fn git(root: &Path, arguments: &[&str]) {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()
        .expect("run git fixture command");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn framed_process_log(
    label: &str,
    invocation: &crate::InvocationReceipt,
    timed_out: bool,
    stdout: &str,
    stderr: &str,
) -> String {
    format!(
        concat!(
            "schema_version: 4\n",
            "label: {label}\n",
            "invocation: {invocation}\n",
            "exit_code: Some(0)\n",
            "timed_out: {timed_out}\n",
            "duration_ms: 1\n",
            "peak_rss_kib: 1\n",
            "stdout_bytes: {stdout_bytes}\n",
            "stderr_bytes: {stderr_bytes}\n\n",
            "{stdout}{stderr}",
        ),
        label = label,
        invocation = serde_json::to_string(invocation).expect("serialize invocation"),
        timed_out = timed_out,
        stdout_bytes = stdout.len(),
        stderr_bytes = stderr.len(),
        stdout = stdout,
        stderr = stderr,
    )
}

fn write_fixture_artifact(root: &Path, path: &str, kind: &str, bytes: &[u8]) -> crate::ArtifactRef {
    let destination = root.join(path);
    fs::create_dir_all(destination.parent().expect("artifact parent"))
        .expect("create artifact parent");
    fs::write(destination, bytes).expect("write simulator fixture artifact");
    crate::ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        size_bytes: bytes.len() as u64,
    }
}
