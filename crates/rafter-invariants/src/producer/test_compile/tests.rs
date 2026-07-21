//! Cargo compiler-artifact identity and protected-target scenarios.

use super::{executable_from_messages, Target};

#[test]
fn compiler_artifact_binds_package_identity_and_target() {
    let package_dir = std::fs::canonicalize(env!("CARGO_MANIFEST_DIR"))
        .expect("canonical invariant package directory");
    let executable =
        std::fs::canonicalize(std::env::current_exe().expect("resolve invariant test executable"))
            .expect("canonical invariant test executable");
    let target = Target {
        package: "rafter-invariants".to_owned(),
        kind: "lib".to_owned(),
        name: "rafter_invariants".to_owned(),
    };
    let workspace = package_dir
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let protected = |package: &str, name: &str, kind: &str, fresh: bool| {
        let package_path = workspace.join("crates").join(package);
        serde_json::json!({
            "reason": "compiler-artifact",
            "package_id": format!("path+file://{}#0.0.1", package_path.display()),
            "target": {
                "name": name,
                "kind": [kind],
                "src_path": package_path.join("src/lib.rs"),
            },
            "fresh": fresh,
            "executable": null,
        })
        .to_string()
    };
    let protected_artifacts = format!(
        "{}\n{}",
        protected(
            "rafter-invariant-test",
            "rafter_invariant_test",
            "lib",
            false,
        ),
        protected(
            "rafter-invariant-test-macros",
            "rafter_invariant_test_macros",
            "proc-macro",
            true,
        )
    );
    let artifact = |package_path: &std::path::Path| {
        serde_json::json!({
            "reason": "compiler-artifact",
            "package_id": format!("path+file://{}#0.0.1", package_path.display()),
            "target": {
                "name": "rafter_invariants",
                "kind": ["lib"],
                "src_path": package_dir.join("src/lib.rs"),
            },
            "fresh": false,
            "executable": executable,
        })
        .to_string()
    };
    let exact = format!("{protected_artifacts}\n{}", artifact(&package_dir));
    assert_eq!(
        executable_from_messages(exact.as_bytes(), &target).expect("exact Cargo package artifact"),
        executable
    );

    let other_package = package_dir
        .parent()
        .expect("workspace crates directory")
        .join("rafter");
    let substituted = format!("{protected_artifacts}\n{}", artifact(&other_package));
    assert!(executable_from_messages(substituted.as_bytes(), &target).is_err());
    assert!(executable_from_messages(artifact(&package_dir).as_bytes(), &target).is_err());
}
