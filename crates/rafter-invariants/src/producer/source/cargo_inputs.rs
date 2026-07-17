use std::{error::Error, fs, path::Path};

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

fn validate_trusted_cargo_package(
    root: &Path,
    packages: &[serde_json::Value],
    trusted: TrustedCargoPackage,
) -> Result<(), Box<dyn Error>> {
    let expected_root = root.join(trusted.relative_root);
    if fs::canonicalize(&expected_root)? != expected_root {
        return Err(format!(
            "trusted Cargo package {} traverses a filesystem alias or symlink",
            trusted.package
        )
        .into());
    }
    let expected_manifest = expected_root.join("Cargo.toml");
    let named = packages
        .iter()
        .filter(|package| {
            package.get("name").and_then(serde_json::Value::as_str) == Some(trusted.package)
        })
        .collect::<Vec<_>>();
    let [package] = named.as_slice() else {
        return Err(format!(
            "trusted Cargo package {} resolves to {} workspace packages",
            trusted.package,
            named.len()
        )
        .into());
    };
    if !package
        .get("source")
        .is_some_and(serde_json::Value::is_null)
    {
        return Err(format!(
            "trusted Cargo package {} is not an in-tree path package",
            trusted.package
        )
        .into());
    }
    let manifest = package
        .get("manifest_path")
        .and_then(serde_json::Value::as_str)
        .ok_or("trusted Cargo package omitted its manifest_path")?;
    if Path::new(manifest) != expected_manifest || fs::canonicalize(manifest)? != expected_manifest
    {
        return Err(format!(
            "trusted Cargo package {} does not use its canonical workspace manifest",
            trusted.package
        )
        .into());
    }
    let targets = package
        .get("targets")
        .and_then(serde_json::Value::as_array)
        .ok_or("trusted Cargo package omitted its target inventory")?;
    let targets = targets
        .iter()
        .filter(|target| {
            target.get("name").and_then(serde_json::Value::as_str) == Some(trusted.rust_crate)
                && target
                    .get("kind")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|kinds| {
                        kinds
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .eq([trusted.target_kind])
                    })
        })
        .collect::<Vec<_>>();
    let [target] = targets.as_slice() else {
        return Err(format!(
            "trusted Cargo package {} resolves to {} canonical targets",
            trusted.package,
            targets.len()
        )
        .into());
    };
    let source = target
        .get("src_path")
        .and_then(serde_json::Value::as_str)
        .ok_or("trusted Cargo target omitted its src_path")?;
    let expected_source = expected_root.join("src/lib.rs");
    if Path::new(source) != expected_source || fs::canonicalize(source)? != expected_source {
        return Err(format!(
            "trusted Cargo package {} does not use its canonical library source",
            trusted.package
        )
        .into());
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

fn validate_trusted_cargo_dependency(
    root: &Path,
    dependency: &serde_json::Value,
    trusted: TrustedCargoPackage,
) -> Result<(), Box<dyn Error>> {
    let package = dependency
        .get("name")
        .and_then(serde_json::Value::as_str)
        .ok_or("trusted Cargo dependency omitted its package name")?;
    let source_is_path = dependency
        .get("source")
        .is_some_and(serde_json::Value::is_null);
    let path = dependency
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or("trusted Cargo dependency omitted its path")?;
    let optional = dependency
        .get("optional")
        .and_then(serde_json::Value::as_bool)
        .ok_or("trusted Cargo dependency omitted its optional flag")?;
    let target_is_unconditional = dependency
        .get("target")
        .is_some_and(serde_json::Value::is_null);
    let kind = match dependency.get("kind") {
        Some(value) if value.is_null() => None,
        Some(value) => Some(
            value
                .as_str()
                .ok_or("trusted Cargo dependency kind is not a string")?,
        ),
        None => return Err("trusted Cargo dependency omitted its kind".into()),
    };
    let expected_root = root.join(trusted.relative_root);
    if package != trusted.package
        || !source_is_path
        || Path::new(path) != expected_root
        || fs::canonicalize(path)? != expected_root
        || optional
        || !target_is_unconditional
        || kind != trusted.dependency_kind
    {
        return Err(format!(
            "Cargo dependency exposed as {} does not resolve to canonical package {}",
            trusted.rust_crate, trusted.package
        )
        .into());
    }
    Ok(())
}
