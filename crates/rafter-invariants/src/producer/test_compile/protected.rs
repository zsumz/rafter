//! Producer-side acceptance of protected oracle compiler artifacts.

use std::{collections::BTreeMap, fs, path::Path};

use super::cargo_output::CargoCompilerMessage;

#[derive(Clone, Copy)]
struct ProtectedTarget {
    name: &'static str,
    package: &'static str,
    relative_root: &'static str,
    kind: &'static str,
}

const PROTECTED_TARGETS: &[ProtectedTarget] = &[
    ProtectedTarget {
        name: "rafter_invariant_test",
        package: "rafter-invariant-test",
        relative_root: "crates/rafter-invariant-test",
        kind: "lib",
    },
    ProtectedTarget {
        name: "rafter_invariant_test_macros",
        package: "rafter-invariant-test-macros",
        relative_root: "crates/rafter-invariant-test-macros",
        kind: "proc-macro",
    },
];

pub(super) fn verify_protected_compiler_artifacts(
    bytes: &[u8],
    workspace: &Path,
) -> Result<(), String> {
    let workspace = fs::canonicalize(workspace)
        .map_err(|error| format!("canonicalize producer workspace: {error}"))?;
    let mut counts = PROTECTED_TARGETS
        .iter()
        .map(|target| (target.name, 0_usize))
        .collect::<BTreeMap<_, _>>();

    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<CargoCompilerMessage>(line) else {
            continue;
        };
        if message.reason != "compiler-artifact" {
            continue;
        }
        let Some(target) = message.target else {
            continue;
        };
        let Some(expected) = PROTECTED_TARGETS
            .iter()
            .find(|expected| expected.name == target.name)
        else {
            continue;
        };
        if target.kind != [expected.kind] {
            return Err(format!(
                "protected compiler target {} has the wrong target kind",
                expected.name
            ));
        }
        if message.fresh.is_none() {
            return Err(format!(
                "protected compiler target {} omitted its freshness state",
                expected.name
            ));
        }
        let package_id = message.package_id.as_deref().ok_or_else(|| {
            format!(
                "protected compiler target {} omitted its package identity",
                expected.name
            )
        })?;
        verify_package_and_source(&workspace, package_id, &target.src_path, *expected)?;
        *counts
            .get_mut(expected.name)
            .ok_or("protected producer target counter is missing")? += 1;
    }

    for expected in PROTECTED_TARGETS {
        let count = counts.get(expected.name).copied().unwrap_or_default();
        if count != 1 {
            return Err(format!(
                "producer compile output contains {count} canonical artifacts for protected target {}",
                expected.name
            ));
        }
    }
    Ok(())
}

fn verify_package_and_source(
    workspace: &Path,
    package_id: &str,
    source: &Path,
    expected: ProtectedTarget,
) -> Result<(), String> {
    let encoded = package_id.strip_prefix("path+file://").ok_or_else(|| {
        format!(
            "protected producer target {} is not an in-tree path package",
            expected.name
        )
    })?;
    let (package_path, version) = encoded.rsplit_once('#').ok_or_else(|| {
        format!(
            "protected producer target {} has no package version",
            expected.name
        )
    })?;
    if version.is_empty() {
        return Err(format!(
            "protected producer target {} has an empty package version",
            expected.name
        ));
    }

    let expected_root = workspace.join(expected.relative_root);
    let observed_root = fs::canonicalize(package_path).map_err(|error| {
        format!(
            "canonicalize protected producer package {}: {error}",
            expected.package
        )
    })?;
    let observed_source = fs::canonicalize(source).map_err(|error| {
        format!(
            "canonicalize protected producer source {}: {error}",
            expected.name
        )
    })?;
    if Path::new(package_path) != expected_root
        || observed_root != expected_root
        || source != expected_root.join("src/lib.rs")
        || observed_source != expected_root.join("src/lib.rs")
    {
        return Err(format!(
            "protected producer target {} does not use canonical package {}",
            expected.name, expected.package
        ));
    }
    Ok(())
}
