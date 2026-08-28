//! Complete Cargo package inputs required for a sound checkout identity.

use std::{error::Error, fs, path::Path};

mod trusted;

use trusted::{validate_trusted_cargo_dependency, validate_trusted_cargo_package};

#[derive(Clone, Copy)]
struct TrustedCargoPackage {
    rust_crate: &'static str,
    package: &'static str,
    relative_root: &'static str,
    target_kind: &'static str,
    dependency_kind: Option<&'static str>,
}

const TRUSTED_CARGO_PACKAGES: &[TrustedCargoPackage] = &[
    TrustedCargoPackage {
        rust_crate: "rafter_invariant_test",
        package: "rafter-invariant-test",
        relative_root: "crates/rafter-invariant-test",
        target_kind: "lib",
        dependency_kind: Some("dev"),
    },
    TrustedCargoPackage {
        rust_crate: "rafter_invariant_test_macros",
        package: "rafter-invariant-test-macros",
        relative_root: "crates/rafter-invariant-test-macros",
        target_kind: "proc-macro",
        dependency_kind: None,
    },
];

pub(super) fn validate_trusted_cargo_package_metadata(
    root: &Path,
    metadata: &str,
) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let metadata: serde_json::Value = serde_json::from_str(metadata)?;
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or("cargo metadata omitted its package inventory")?;

    validate_local_path_dependency_inventory(packages)?;
    validate_protected_target_names(&root, packages)?;

    for trusted in TRUSTED_CARGO_PACKAGES {
        validate_trusted_cargo_package(&root, packages, *trusted)?;
        let mut matching_edges = 0_usize;
        for package in packages {
            let dependencies = package
                .get("dependencies")
                .and_then(serde_json::Value::as_array)
                .ok_or("cargo metadata package omitted its dependency inventory")?;
            for dependency in dependencies {
                if dependency_effective_crate_name(dependency, packages)? != trusted.rust_crate {
                    continue;
                }
                matching_edges += 1;
                validate_trusted_cargo_dependency(&root, dependency, *trusted)?;
            }
        }
        if matching_edges == 0 {
            return Err(format!(
                "Cargo metadata has no dependency edge for trusted crate {}",
                trusted.rust_crate
            )
            .into());
        }
    }
    Ok(())
}

fn dependency_effective_crate_name(
    dependency: &serde_json::Value,
    packages: &[serde_json::Value],
) -> Result<String, Box<dyn Error>> {
    let package = dependency
        .get("name")
        .and_then(serde_json::Value::as_str)
        .ok_or("Cargo dependency omitted its package name")?;
    let declared = match dependency.get("rename") {
        Some(value) if value.is_null() => {
            if let Some(path) = dependency.get("path").and_then(serde_json::Value::as_str) {
                let referenced = workspace_package_at_path(packages, Path::new(path))?;
                package_library_target_name(referenced)?.ok_or_else(|| {
                    format!("Cargo path dependency {package} has no unique library target")
                })?
            } else {
                package.to_owned()
            }
        }
        Some(value) => value
            .as_str()
            .ok_or("Cargo dependency rename is not a string")?
            .to_owned(),
        None => return Err("Cargo dependency omitted its rename field".into()),
    };
    Ok(declared.replace('-', "_"))
}

fn validate_local_path_dependency_inventory(
    packages: &[serde_json::Value],
) -> Result<(), Box<dyn Error>> {
    for package in packages {
        let dependencies = package
            .get("dependencies")
            .and_then(serde_json::Value::as_array)
            .ok_or("cargo metadata package omitted its dependency inventory")?;
        for dependency in dependencies {
            let Some(path) = dependency.get("path").and_then(serde_json::Value::as_str) else {
                continue;
            };
            workspace_package_at_path(packages, Path::new(path))?;
        }
    }
    Ok(())
}

fn workspace_package_at_path<'a>(
    packages: &'a [serde_json::Value],
    path: &Path,
) -> Result<&'a serde_json::Value, Box<dyn Error>> {
    let path = fs::canonicalize(path)?;
    let matches = packages
        .iter()
        .filter_map(|package| {
            let manifest = package.get("manifest_path")?.as_str()?;
            let package_root = fs::canonicalize(Path::new(manifest).parent()?).ok()?;
            (package_root == path).then_some(package)
        })
        .collect::<Vec<_>>();
    let [package] = matches.as_slice() else {
        return Err(format!(
            "Cargo path dependency {} resolves to {} workspace packages",
            path.display(),
            matches.len()
        )
        .into());
    };
    Ok(*package)
}

fn package_library_target_name(
    package: &serde_json::Value,
) -> Result<Option<String>, Box<dyn Error>> {
    let targets = package
        .get("targets")
        .and_then(serde_json::Value::as_array)
        .ok_or("Cargo package omitted its target inventory")?
        .iter()
        .filter(|target| {
            target
                .get("kind")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|kinds| {
                    kinds
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .any(|kind| matches!(kind, "lib" | "proc-macro"))
                })
        })
        .map(|target| {
            target
                .get("name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| "Cargo library target omitted its name".into())
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    match targets.as_slice() {
        [] => Ok(None),
        [target] => Ok(Some(target.clone())),
        _ => Err("Cargo package has more than one library target".into()),
    }
}

fn validate_protected_target_names(
    root: &Path,
    packages: &[serde_json::Value],
) -> Result<(), Box<dyn Error>> {
    for package in packages {
        let Some(target_name) = package_library_target_name(package)? else {
            continue;
        };
        let Some(trusted) = TRUSTED_CARGO_PACKAGES
            .iter()
            .find(|trusted| trusted.rust_crate == target_name)
        else {
            continue;
        };
        let manifest = package
            .get("manifest_path")
            .and_then(serde_json::Value::as_str)
            .ok_or("Cargo package omitted its manifest_path")?;
        if Path::new(manifest) != root.join(trusted.relative_root).join("Cargo.toml") {
            return Err(format!(
                "noncanonical Cargo package exposes protected target name {}",
                trusted.rust_crate
            )
            .into());
        }
    }
    Ok(())
}
