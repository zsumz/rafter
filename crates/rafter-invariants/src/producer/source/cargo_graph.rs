use std::error::Error;

pub(super) fn validate_registry_build_script_source_identity(
    metadata: &str,
    cargo_lock: &str,
) -> Result<(), Box<dyn Error>> {
    let metadata: serde_json::Value = serde_json::from_str(metadata)?;
    let lock: toml::Value = cargo_lock.parse()?;
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or("cargo metadata omitted its package inventory")?;
    let locked = lock
        .get("package")
        .and_then(toml::Value::as_array)
        .ok_or("Cargo.lock omitted its package inventory")?;

    // This binds build-script source identity, not a hermetic source-to-binary build. The source
    // receipt separately binds Cargo.lock, while the producer-image receipt binds and reexecutes
    // the exact resulting executable. Host observations not reflected in that executable remain
    // outside this portable provenance contract and are documented explicitly.
    for package in packages {
        let has_build_script = package
            .get("targets")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .any(|target| {
                target
                    .get("kind")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .any(|kind| kind == "custom-build")
            });
        if !has_build_script {
            continue;
        }
        let Some(source) = package.get("source").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if !source.starts_with("registry+") {
            return Err(format!(
                "Cargo custom build package uses an unbound non-registry source: {source}"
            )
            .into());
        }
        let name = package
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or("Cargo custom build package omitted its name")?;
        let version = package
            .get("version")
            .and_then(serde_json::Value::as_str)
            .ok_or("Cargo custom build package omitted its version")?;
        let matching = locked
            .iter()
            .filter(|entry| {
                entry.get("name").and_then(toml::Value::as_str) == Some(name)
                    && entry.get("version").and_then(toml::Value::as_str) == Some(version)
                    && entry.get("source").and_then(toml::Value::as_str) == Some(source)
            })
            .collect::<Vec<_>>();
        let [entry] = matching.as_slice() else {
            return Err(format!(
                "Cargo custom build package {name} {version} has {} matching lock entries",
                matching.len()
            )
            .into());
        };
        let checksum = entry
            .get("checksum")
            .and_then(toml::Value::as_str)
            .ok_or_else(|| {
                format!("Cargo custom build package {name} {version} has no locked checksum")
            })?;
        if checksum.len() != 64 || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!(
                "Cargo custom build package {name} {version} has an invalid locked checksum"
            )
            .into());
        }
    }
    Ok(())
}
