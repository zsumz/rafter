//! Test-only end-to-end detector witness capture and compilation fixtures.

use std::{error::Error, path::Path};

use crate::{
    contract::TestIdentity,
    evidence::format::libtest::{oracle_token, ORACLE_TOKEN_ENV},
};

use super::detector_proof;
use crate::producer::process;

pub(crate) fn capture_detector_witness_fixture_log(
    source_ref: &str,
    fixture: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let identity = fixture_identity(format!("tests::{fixture}"));
    capture_detector_witness_identity_log(source_ref, &identity, true, true)
}

pub(crate) fn capture_fabricated_detector_witness_fixture_log(
    source_ref: &str,
    fixture: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let identity = fixture_identity(format!("tests::{fixture}"));
    capture_detector_witness_identity_log(source_ref, &identity, true, false)
}

pub(crate) fn capture_qualified_helper_forged_transcript_fixture_log(
    source_ref: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let identity =
        fixture_identity("tests::qualified_helper_forged_transcript_subprocess_fixture".to_owned());
    capture_detector_witness_identity_log(source_ref, &identity, true, true)
}

pub(crate) fn capture_hidden_proof_socket_fixture_log(
    source_ref: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let identity = fixture_identity(
        "tests::proof_descriptor_is_hidden_from_fixture_body_subprocess_fixture".to_owned(),
    );
    capture_detector_witness_identity_log(source_ref, &identity, true, true)
}

pub(crate) fn capture_removed_token_detector_fixture_log(
    source_ref: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let identity = fixture_identity(
        "tests::detector_witness_with_removed_token_subprocess_fixture".to_owned(),
    );
    capture_detector_witness_identity_log(source_ref, &identity, true, false)
}

pub(crate) fn capture_registered_detector_fixture_log(
    source_ref: &str,
    identity: &TestIdentity,
) -> Result<(String, String), Box<dyn Error>> {
    capture_detector_witness_identity_log(source_ref, identity, false, true)
}

fn fixture_identity(test_name: String) -> TestIdentity {
    TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name,
    }
}

fn capture_detector_witness_identity_log(
    source_ref: &str,
    identity: &TestIdentity,
    ignored: bool,
    expect_success: bool,
) -> Result<(String, String), Box<dyn Error>> {
    if identity.target_kind != "lib" {
        return Err("detector witness regression helper requires a library target".into());
    }
    let check_id = identity.check_id();
    let fixture = identity
        .test_name
        .rsplit("::")
        .next()
        .ok_or("detector witness fixture has no leaf name")?;
    let target_dir = Path::new("target/rafter-invariants").join(format!(
        "detector-witness-e2e-build-{}-{fixture}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&target_dir);

    let result = (|| {
        let executable = compile_detector_witness_executable(identity, &target_dir)?;
        let program = executable
            .to_str()
            .ok_or("detector witness fixture path is not valid UTF-8")?;
        let mut arguments = vec![
            identity.test_name.clone().into(),
            "--exact".into(),
            "--test-threads=1".into(),
            "--show-output".into(),
            "--color".into(),
            "never".into(),
        ];
        if ignored {
            arguments.push("--ignored".into());
        }
        let mut environment = process::base_environment();
        environment.insert(
            ORACLE_TOKEN_ENV.to_owned(),
            oracle_token(source_ref, &check_id),
        );
        let detector_proof::Execution {
            output: captured,
            challenge,
            channel_error,
        } = detector_proof::execute_for_test(program, &arguments, &mut environment)?;
        if let Some(error) = channel_error {
            return Err(format!("detector witness proof channel failed: {error}").into());
        }
        if captured.status.success() != expect_success {
            return Err(format!(
                "detector witness fixture exact libtest execution success={} expected={expect_success}; stdout={}; stderr={}",
                captured.status.success(),
                String::from_utf8_lossy(&captured.stdout),
                String::from_utf8_lossy(&captured.stderr),
            )
            .into());
        }
        let source = String::from_utf8(process::combined_detector_log(
            "exact libtest execution",
            &captured,
            &challenge,
        )?)?;
        Ok((check_id, source))
    })();

    let _ = std::fs::remove_dir_all(target_dir);
    result
}

fn compile_detector_witness_executable(
    identity: &TestIdentity,
    target_dir: &Path,
) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let compiled = std::process::Command::new("cargo")
        .args([
            "test",
            "--locked",
            "--no-default-features",
            "-p",
            &identity.package,
            "--lib",
            "--no-run",
            "--message-format=json-render-diagnostics",
        ])
        .env("CARGO_TARGET_DIR", target_dir)
        .output()?;
    if !compiled.status.success() {
        return Err(format!(
            "compile detector witness fixture: {}",
            String::from_utf8_lossy(&compiled.stderr)
        )
        .into());
    }
    let executables = String::from_utf8_lossy(&compiled.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|message| message["reason"] == "compiler-artifact")
        .filter(|message| message["target"]["name"] == identity.target)
        .filter(|message| message["target"]["kind"] == serde_json::json!(["lib"]))
        .filter_map(|message| message["executable"].as_str().map(std::path::PathBuf::from))
        .collect::<Vec<_>>();
    let [executable] = executables.as_slice() else {
        return Err(format!(
            "expected one detector witness fixture executable, found {}",
            executables.len()
        )
        .into());
    };
    Ok(std::fs::canonicalize(executable)?)
}
