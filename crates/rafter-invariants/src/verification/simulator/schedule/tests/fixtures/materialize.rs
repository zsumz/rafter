//! Top-level construction of serialized simulator evidence fixtures.

use std::{
    env, fs,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use super::{
    checkout::{initialize_fixture_repository, materialize_fixture_checkout},
    compile::materialize_compile_fixture,
    io::copy_plan_input,
    model::{
        cleanup_fixture_artifacts, PendingSimulatorFixture, RuntimeDefect, RuntimeFixtureInput,
        SimulatorFixture,
    },
    runtime::{bind_fixture_evidence, materialize_runtime_fixture},
};

static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
static FIXTURE_SERIAL: Mutex<()> = Mutex::new(());

pub(in super::super) fn materialize_fixture(defect: RuntimeDefect) -> SimulatorFixture {
    materialize_fixture_with_roots(defect, false)
}

pub(in super::super) fn materialize_cross_root_fixture(defect: RuntimeDefect) -> SimulatorFixture {
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
