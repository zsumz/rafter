use std::collections::BTreeMap;

use super::{
    compile::{
        compiler_artifact_executable, target_directory_matches, verify_target_process_binding,
        CargoTargetKey, EmittedTestExecutable,
    },
    test_logs::{
        require_unique_discovery, verify_exact_environment, verify_reconstructed_test_observations,
        verify_runner_test_observations,
    },
};
use crate::{
    producer::process::{LabeledProcess, ProcessMetrics},
    InvocationReceipt,
};

#[test]
fn discovery_counts_are_reconstructed_instead_of_trusting_the_receipt() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut check = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle")
        .execution
        .checks
        .remove(0);
    check.observations = std::collections::BTreeMap::from([
        ("discovered".to_owned(), 1),
        ("executed".to_owned(), 1),
        ("passed".to_owned(), 1),
    ]);
    let exact_name = "module::oracle";
    let valid = passing_test_processes(exact_name);
    verify_reconstructed_test_observations(&check, &valid, exact_name)
        .expect("one discovered identity reconstructs");
    require_unique_discovery(&valid, exact_name).expect("one discovery is unique");
    assert!(verify_reconstructed_test_observations(&check, &valid, "oracle").is_err());
    assert!(require_unique_discovery(&valid, "oracle").is_err());

    let mut zero = valid.clone();
    zero[0].stdout.clear();
    assert!(verify_reconstructed_test_observations(&check, &zero, exact_name).is_err());

    let mut duplicate = valid;
    duplicate[0].stdout = format!("{exact_name}: test\n{exact_name}: test\n");
    assert!(verify_reconstructed_test_observations(&check, &duplicate, exact_name).is_err());
    assert!(require_unique_discovery(&duplicate, exact_name).is_err());
}

#[test]
fn simulator_detector_observations_remain_model_specific() {
    let (catalog, manifest) = crate::tests::loaded();
    let exact_name = "module::oracle";
    let invocations = passing_test_processes(exact_name);
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);
    let tests = bundles
        .iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    let tests_check = &tests.execution.checks[0];
    verify_runner_test_observations(tests, tests_check, &invocations, exact_name)
        .expect("tests runner retains exact transcript observations");

    let simulator = bundles
        .iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle");
    let simulator_check = &simulator.execution.checks[0];
    assert!(
        verify_reconstructed_test_observations(simulator_check, &invocations, exact_name).is_err(),
        "simulator model observations are not a test-runner count map"
    );
    verify_runner_test_observations(simulator, simulator_check, &invocations, exact_name)
        .expect("simulator detector transcript does not replace model observations");

    let mut duplicate = invocations.clone();
    duplicate[0].stdout = format!("{exact_name}: test\n{exact_name}: test\n");
    assert!(
        verify_runner_test_observations(simulator, simulator_check, &duplicate, exact_name,)
            .is_err()
    );

    let mut unsupported = simulator.clone();
    unsupported.runner = "unknown".to_owned();
    assert!(verify_runner_test_observations(
        &unsupported,
        simulator_check,
        &invocations,
        exact_name,
    )
    .is_err());
}

#[test]
fn exact_environment_map_is_rehashed_not_just_compared_by_claimed_digest() {
    let expected = std::collections::BTreeMap::from([("A".to_owned(), "one".to_owned())]);
    let digest = crate::producer::process::digest_environment(&expected);
    let mut exact = process("exact libtest execution", "");
    exact.invocation.environment.clone_from(&expected);
    exact.invocation.environment_sha256.clone_from(&digest);
    verify_exact_environment(&exact, &expected, &digest).expect("exact map verifies");

    exact
        .invocation
        .environment
        .insert("A".to_owned(), "tampered".to_owned());
    assert!(verify_exact_environment(&exact, &expected, &digest).is_err());
}

#[test]
fn compile_target_directory_requires_the_exact_absolute_path() {
    let expected =
        std::path::Path::new("/workspace/rafter/target/rafter-invariants/build/source/pr-tests");
    assert!(target_directory_matches(expected.to_str(), expected,));
    assert!(!target_directory_matches(
        Some("target/rafter-invariants/build/source/pr-tests"),
        expected,
    ));
    assert!(!target_directory_matches(
        Some("/workspace/rafter/target/rafter-invariants/build/source/sibling"),
        expected,
    ));
}

#[test]
fn cross_target_binary_substitution_is_rejected() {
    let target_a_path = "/workspace/target/debug/deps/rafter-a";
    let target_b_path = "/workspace/target/debug/deps/rafter_runtime-b";
    let target_a = CargoTargetKey {
        package: "rafter".to_owned(),
        kind: "lib".to_owned(),
        target: "rafter".to_owned(),
    };
    let target_b = CargoTargetKey {
        package: "rafter-runtime".to_owned(),
        kind: "lib".to_owned(),
        target: "rafter_runtime".to_owned(),
    };
    let emitted_a = EmittedTestExecutable {
        package_id: "path+file:///workspace/crates/rafter#0.0.1".to_owned(),
        target: target_a.clone(),
        executable: target_a_path.into(),
        sha256: "a".repeat(64),
    };
    let emitted_b = EmittedTestExecutable {
        package_id: "path+file:///workspace/crates/rafter-runtime#0.0.1".to_owned(),
        target: target_b.clone(),
        executable: target_b_path.into(),
        sha256: "b".repeat(64),
    };
    let inventory = BTreeMap::from([(target_a.clone(), emitted_a), (target_b, emitted_b)]);
    let expected = inventory.get(&target_a).expect("target A compiler binding");

    // Target B is genuinely present in the inventory, but cannot satisfy target A.
    let mut substituted = process("libtest discovery", "");
    substituted.invocation.program = target_b_path.to_owned();
    substituted.invocation.program_sha256 = "b".repeat(64);
    assert!(verify_target_process_binding(&[substituted], expected, "test.log").is_err());

    let mut exact = process("libtest discovery", "");
    exact.invocation.program = target_a_path.to_owned();
    exact.invocation.program_sha256 = "a".repeat(64);
    verify_target_process_binding(&[exact], expected, "test.log")
        .expect("the exact target-keyed executable verifies");
}

#[test]
fn simulator_compiler_artifact_requires_the_exact_absolute_bin_target() {
    let cargo = |kind: &str, executable: &str| {
        serde_json::json!({
            "reason": "compiler-artifact",
            "target": {"name": "rafter-model-check-fast", "kind": [kind]},
            "fresh": false,
            "executable": executable,
        })
        .to_string()
    };
    compiler_artifact_executable(
        cargo("bin", "/workspace/target/rafter-model-check-fast").as_bytes(),
        "rafter-model-check-fast",
        "bin",
        "simulator compile",
    )
    .expect("exact simulator compiler-artifact verifies");
    assert!(compiler_artifact_executable(
        cargo("lib", "/workspace/target/rafter-model-check-fast").as_bytes(),
        "rafter-model-check-fast",
        "bin",
        "simulator compile",
    )
    .is_err());
    assert!(compiler_artifact_executable(
        cargo("bin", "target/rafter-model-check-fast").as_bytes(),
        "rafter-model-check-fast",
        "bin",
        "simulator compile",
    )
    .is_err());
}

fn process(label: &str, stdout: &str) -> LabeledProcess {
    LabeledProcess {
        label: label.to_owned(),
        invocation: InvocationReceipt {
            program: "/workspace/test".to_owned(),
            program_sha256: "0".repeat(64),
            arguments: vec!["test".to_owned()],
            current_dir: "/workspace".to_owned(),
            environment: std::collections::BTreeMap::new(),
            environment_sha256: crate::producer::process::digest_environment(
                &std::collections::BTreeMap::new(),
            ),
        },
        exit_code: Some(0),
        timed_out: false,
        metrics: ProcessMetrics {
            duration_ms: 1,
            peak_rss_kib: 1,
        },
        stdout: stdout.to_owned(),
        stderr: String::new(),
    }
}

fn passing_test_processes(exact_name: &str) -> Vec<LabeledProcess> {
    let exact_pass = format!(
        "running 1 test\ntest {exact_name} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored\n"
    );
    let mut exact = process("exact libtest execution", &exact_pass);
    exact.invocation.arguments = vec![exact_name.to_owned()];
    vec![
        process("libtest discovery", &format!("{exact_name}: test\n")),
        process("libtest ignored discovery", ""),
        exact,
    ]
}
