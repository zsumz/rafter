use std::{collections::BTreeMap, fs, path::Path};

#[derive(Clone, Copy)]
struct ProtectedCompilerTarget {
    name: &'static str,
    package: &'static str,
    relative_root: &'static str,
    kind: &'static str,
}

const PROTECTED_COMPILER_TARGETS: &[ProtectedCompilerTarget] = &[
    ProtectedCompilerTarget {
        name: "rafter_invariant_test",
        package: "rafter-invariant-test",
        relative_root: "crates/rafter-invariant-test",
        kind: "lib",
    },
    ProtectedCompilerTarget {
        name: "rafter_invariant_test_macros",
        package: "rafter-invariant-test-macros",
        relative_root: "crates/rafter-invariant-test-macros",
        kind: "proc-macro",
    },
];

pub(crate) fn verify_protected_compiler_artifacts(
    bytes: &[u8],
    workspace: &Path,
) -> Result<(), String> {
    let workspace = fs::canonicalize(workspace)
        .map_err(|error| format!("canonicalize compiler workspace: {error}"))?;
    let mut counts = PROTECTED_COMPILER_TARGETS
        .iter()
        .map(|target| (target.name, 0_usize))
        .collect::<BTreeMap<_, _>>();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if message.get("reason").and_then(serde_json::Value::as_str) != Some("compiler-artifact") {
            continue;
        }
        let Some(target) = message.get("target") else {
            continue;
        };
        let Some(name) = target.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(expected) = PROTECTED_COMPILER_TARGETS
            .iter()
            .find(|expected| expected.name == name)
        else {
            continue;
        };
        verify_protected_compiler_artifact(&workspace, &message, target, *expected)?;
        *counts
            .get_mut(expected.name)
            .ok_or("protected compiler target counter is missing")? += 1;
    }
    for expected in PROTECTED_COMPILER_TARGETS {
        let count = counts.get(expected.name).copied().unwrap_or_default();
        if count != 1 {
            return Err(format!(
                "compiler output contains {count} canonical artifacts for protected target {}",
                expected.name
            ));
        }
    }
    Ok(())
}

fn verify_protected_compiler_artifact(
    workspace: &Path,
    message: &serde_json::Value,
    target: &serde_json::Value,
    expected: ProtectedCompilerTarget,
) -> Result<(), String> {
    let kinds = target
        .get("kind")
        .and_then(serde_json::Value::as_array)
        .ok_or("protected compiler artifact omitted its target kind")?;
    if !kinds
        .iter()
        .filter_map(serde_json::Value::as_str)
        .eq([expected.kind])
    {
        return Err(format!(
            "protected compiler target {} has the wrong target kind",
            expected.name
        ));
    }
    if !message
        .get("fresh")
        .is_some_and(serde_json::Value::is_boolean)
    {
        return Err(format!(
            "protected compiler target {} omitted its freshness state",
            expected.name
        ));
    }
    let package_id = message
        .get("package_id")
        .and_then(serde_json::Value::as_str)
        .ok_or("protected compiler artifact omitted its package_id")?;
    let encoded = package_id.strip_prefix("path+file://").ok_or_else(|| {
        format!(
            "protected compiler target {} is not an in-tree path package",
            expected.name
        )
    })?;
    let (package_path, version) = encoded.rsplit_once('#').ok_or_else(|| {
        format!(
            "protected compiler target {} has no package version",
            expected.name
        )
    })?;
    if version.is_empty() {
        return Err(format!(
            "protected compiler target {} has an empty package version",
            expected.name
        ));
    }
    let expected_root = workspace.join(expected.relative_root);
    let source = target
        .get("src_path")
        .and_then(serde_json::Value::as_str)
        .ok_or("protected compiler artifact omitted its source path")?;
    if Path::new(package_path) != expected_root
        || fs::canonicalize(package_path).map_err(|error| {
            format!(
                "canonicalize protected package {}: {error}",
                expected.package
            )
        })? != expected_root
        || Path::new(source) != expected_root.join("src/lib.rs")
        || fs::canonicalize(source).map_err(|error| {
            format!(
                "canonicalize protected target source {}: {error}",
                expected.name
            )
        })? != expected_root.join("src/lib.rs")
    {
        return Err(format!(
            "protected compiler target {} does not use canonical package {}",
            expected.name, expected.package
        ));
    }
    Ok(())
}
