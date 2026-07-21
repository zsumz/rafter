//! Controlled Cargo compilation for simulator provenance fixtures.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use super::{
    io::{executable_sha256, framed_process_log, source_bound_launchers, write_fixture_artifact},
    model::CompileFixture,
    simulator_compiler_artifact_executable,
};

pub(super) fn materialize_compile_fixture(
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
