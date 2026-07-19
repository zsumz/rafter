//! Source, toolchain, and clean-checkout provenance capture.

use std::{
    collections::HashSet,
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::{SourceReceipt, ToolReceipt};

use super::process;

mod cargo_graph;
mod cargo_inputs;
mod materialization;
mod path_validation;
mod rust_inputs;

#[cfg(test)]
#[path = "source/cargo_graph_tests.rs"]
mod cargo_graph_tests;

use cargo_graph::validate_registry_build_script_source_identity;
use cargo_inputs::validate_trusted_cargo_package_metadata;
use materialization::capture_materialization;
use path_validation::validate_tracked_source_path;
use rust_inputs::validate_resolved_tracked_rust_inputs;

#[derive(Clone, Copy)]
struct LayerSourceContract {
    build_profile: &'static str,
    features: &'static [&'static str],
    tools: &'static [&'static str],
    script_runtime: bool,
}

#[derive(Clone, Copy)]
enum CaptureBudget {
    Execution,
    Total,
}

const TOOL_IDENTITY_PROBES: &[(&str, &[&str])] = &[
    ("java", &["-version"]),
    ("maelstrom", &["serve", "--help"]),
    ("dot", &["-V"]),
    ("gnuplot", &["--version"]),
];

pub(super) fn capture_for_layer(layer: &str) -> Result<SourceReceipt, Box<dyn Error>> {
    capture(layer_contract(layer)?)
}

#[cfg(test)]
pub(crate) fn capture_for_layer_at(
    layer: &str,
    root: &Path,
) -> Result<SourceReceipt, Box<dyn Error>> {
    capture_at(layer_contract(layer)?, root, CaptureBudget::Execution)
}

pub(crate) fn head_commit() -> Result<String, Box<dyn Error>> {
    git(&["rev-parse", "HEAD"])
}

fn capture(contract: LayerSourceContract) -> Result<SourceReceipt, Box<dyn Error>> {
    capture_at(contract, Path::new("."), CaptureBudget::Execution)
}

fn capture_at(
    contract: LayerSourceContract,
    root: &Path,
    budget: CaptureBudget,
) -> Result<SourceReceipt, Box<dyn Error>> {
    let receipt = capture_identity_at(contract, root, budget)?;
    validate_resolved_path_packages(root, budget)?;
    Ok(receipt)
}

fn capture_identity_at(
    contract: LayerSourceContract,
    root: &Path,
    budget: CaptureBudget,
) -> Result<SourceReceipt, Box<dyn Error>> {
    let status = command_output_at(
        "git",
        &["status", "--porcelain=v1", "--untracked-files=all"],
        true,
        root,
        budget,
    )?;
    if !status.trim().is_empty() {
        return Err("evidence producers require a clean tracked and untracked worktree".into());
    }
    let materialized = capture_materialization(root, budget)?;
    let cargo = command_output_at("cargo", &["-vV"], false, root, budget)?;
    let rustc = command_output_at("rustc", &["-vV"], false, root, budget)?;
    let target = rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or_else(|| format!("rustc -vV omitted host target from output: {rustc:?}"))?
        .to_owned();
    let cargo_lock = fs::read(root.join("Cargo.lock"))?;
    let cargo_config_sha256 = cargo_config_sha256(root, &cargo)?;
    let environment = process::base_environment();
    let process_runtime = process::capture_runtime_receipts(&environment, contract.script_runtime)?;
    let tools = contract
        .tools
        .iter()
        .map(|name| Ok(((*name).to_owned(), capture_tool(name, root, budget)?)))
        .collect::<Result<_, Box<dyn Error>>>()?;
    let environment_sha256 = crate::provenance::invocation::digest_environment(&environment)?;
    Ok(SourceReceipt {
        commit: materialized.commit,
        tree: materialized.tree,
        materialization: materialized.receipt,
        cargo_lock_sha256: format!("{:x}", Sha256::digest(cargo_lock)),
        cargo,
        cargo_sha256: executable_sha256("cargo")?,
        cargo_config_sha256,
        rustc,
        rustc_sha256: executable_sha256("rustc")?,
        target,
        build_profile: contract.build_profile.to_owned(),
        features: contract
            .features
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        tools,
        process_runtime,
        environment_sha256,
        clean: true,
    })
}

pub(super) fn verify(expected: &SourceReceipt) -> Result<(), Box<dyn Error>> {
    let contract = contract_for_receipt(expected)?;
    let observed = capture_identity_at(contract, Path::new("."), CaptureBudget::Total)?;
    if &observed != expected {
        return Err("source or toolchain identity changed during evidence execution".into());
    }
    Ok(())
}

pub(crate) fn verify_checkout_at(
    expected: &SourceReceipt,
    root: &Path,
) -> Result<(), Box<dyn Error>> {
    let contract = contract_for_receipt(expected)?;
    let observed = capture_at(
        LayerSourceContract {
            tools: &[],
            ..contract
        },
        root,
        CaptureBudget::Total,
    )?;
    if observed.commit != expected.commit
        || observed.tree != expected.tree
        || observed.materialization != expected.materialization
        || observed.cargo_lock_sha256 != expected.cargo_lock_sha256
        || observed.cargo != expected.cargo
        || observed.cargo_sha256 != expected.cargo_sha256
        || observed.cargo_config_sha256 != expected.cargo_config_sha256
        || observed.rustc != expected.rustc
        || observed.rustc_sha256 != expected.rustc_sha256
        || observed.target != expected.target
    {
        return Err("evidence source identity does not match the active checkout".into());
    }
    Ok(())
}

pub(crate) fn verify_layer_contract(
    layer: &str,
    receipt: &SourceReceipt,
) -> Result<(), Box<dyn Error>> {
    let expected = layer_contract(layer)?;
    let expected_features = expected
        .features
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    let expected_tools = expected
        .tools
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let observed_tools = receipt
        .tools
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let expected_runtime = if expected.script_runtime {
        ["bash", "perl", "ps", "time"].as_slice()
    } else {
        ["perl", "ps", "time"].as_slice()
    };
    let observed_runtime = receipt
        .process_runtime
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    if receipt.build_profile != expected.build_profile
        || receipt.features != expected_features
        || observed_tools != expected_tools
        || observed_runtime != expected_runtime.iter().copied().collect()
    {
        return Err(format!(
            "{layer} source receipt does not match its exact build profile, features, and tools contract"
        )
        .into());
    }
    Ok(())
}

fn contract_for_receipt(receipt: &SourceReceipt) -> Result<LayerSourceContract, Box<dyn Error>> {
    ["tests", "simulator", "tla", "maelstrom"]
        .into_iter()
        .find_map(|layer| {
            let contract = layer_contract(layer).ok()?;
            verify_layer_contract(layer, receipt).ok().map(|()| contract)
        })
        .ok_or_else(|| {
            "source receipt does not match any reviewed layer build profile, features, and tools contract"
                .into()
        })
}

fn layer_contract(layer: &str) -> Result<LayerSourceContract, Box<dyn Error>> {
    match layer {
        "tests" => Ok(LayerSourceContract {
            build_profile: "test",
            features: &["no-default-features"],
            tools: &[],
            script_runtime: false,
        }),
        "simulator" => Ok(LayerSourceContract {
            build_profile: "release-and-test",
            features: &["internal-test-hooks"],
            tools: &[],
            script_runtime: false,
        }),
        "tla" => Ok(LayerSourceContract {
            build_profile: "tla",
            features: &[],
            tools: &["java"],
            script_runtime: false,
        }),
        "maelstrom" => Ok(LayerSourceContract {
            build_profile: "maelstrom-debug",
            features: &[],
            tools: &["java", "maelstrom", "dot", "gnuplot"],
            script_runtime: true,
        }),
        _ => Err(format!("unsupported source profile for layer {layer}").into()),
    }
}

fn git(arguments: &[&str]) -> Result<String, Box<dyn Error>> {
    command_output("git", arguments, false)
}

fn command_output(
    program: &str,
    arguments: &[&str],
    allow_empty: bool,
) -> Result<String, Box<dyn Error>> {
    command_output_at(
        program,
        arguments,
        allow_empty,
        Path::new("."),
        CaptureBudget::Execution,
    )
}

fn command_output_at(
    program: &str,
    arguments: &[&str],
    allow_empty: bool,
    root: &Path,
    budget: CaptureBudget,
) -> Result<String, Box<dyn Error>> {
    let stdout = command_stdout_at(program, arguments, root, budget)?;
    let value = stdout.trim().to_owned();
    if value.is_empty() && !allow_empty {
        return Err(format!("{program} produced empty identity output").into());
    }
    Ok(value)
}

fn command_output_raw_at(
    program: &str,
    arguments: &[&str],
    allow_empty: bool,
    root: &Path,
    budget: CaptureBudget,
) -> Result<String, Box<dyn Error>> {
    let value = command_stdout_at(program, arguments, root, budget)?;
    if value.is_empty() && !allow_empty {
        return Err(format!("{program} produced empty identity output").into());
    }
    Ok(value)
}

fn command_stdout_at(
    program: &str,
    arguments: &[&str],
    root: &Path,
    budget: CaptureBudget,
) -> Result<String, Box<dyn Error>> {
    let output = match budget {
        CaptureBudget::Execution => process::identity_command_in(program, arguments, root)?,
        CaptureBudget::Total => {
            process::identity_command_in_total_budget(program, arguments, root)?
        }
    };
    Ok(output.stdout)
}

fn validate_resolved_path_packages(
    root: &Path,
    budget: CaptureBudget,
) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let tracked = command_output_raw_at("git", &["ls-files", "-z"], true, &root, budget)?;
    let tracked = parse_tracked_source_paths(&tracked)?;
    validate_manifest_path_overrides(&root, &tracked)?;
    let metadata = command_output_at(
        "cargo",
        &["metadata", "--format-version", "1", "--locked", "--offline"],
        false,
        &root,
        budget,
    )?;
    validate_resolved_path_package_metadata(&root, &metadata, &tracked)?;
    validate_registry_build_script_source_identity(
        &metadata,
        &fs::read_to_string(root.join("Cargo.lock"))?,
    )?;
    validate_resolved_tracked_rust_inputs(&root, &tracked, &metadata)?;
    validate_trusted_cargo_package_metadata(&root, &metadata)
}

fn validate_resolved_path_package_metadata(
    root: &Path,
    metadata: &str,
    tracked: &HashSet<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let metadata: serde_json::Value = serde_json::from_str(metadata)?;
    let workspace_root = metadata
        .get("workspace_root")
        .and_then(serde_json::Value::as_str)
        .ok_or("cargo metadata omitted its workspace_root")?;
    if fs::canonicalize(workspace_root)? != root {
        return Err("cargo metadata resolved a different workspace root".into());
    }
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or("cargo metadata omitted its package inventory")?;
    for package in packages {
        match package.get("source") {
            Some(source) if source.is_null() => {}
            Some(_) => continue,
            None => return Err("cargo metadata package omitted its source field".into()),
        }
        let manifest = package
            .get("manifest_path")
            .and_then(serde_json::Value::as_str)
            .ok_or("path package omitted its manifest_path")?;
        validate_tracked_source_path(&root, Path::new(manifest), tracked, "package manifest")?;

        let dependencies = package
            .get("dependencies")
            .and_then(serde_json::Value::as_array)
            .ok_or("cargo metadata package omitted its dependency inventory")?;
        for dependency in dependencies {
            let Some(path) = dependency.get("path").and_then(serde_json::Value::as_str) else {
                continue;
            };
            validate_tracked_source_path(
                &root,
                &Path::new(path).join("Cargo.toml"),
                tracked,
                "dependency manifest",
            )?;
        }

        let targets = package
            .get("targets")
            .and_then(serde_json::Value::as_array)
            .ok_or("cargo metadata package omitted its target inventory")?;
        for target in targets {
            if target
                .get("kind")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .any(|kind| kind == "custom-build")
            {
                return Err(
                    "Cargo custom build targets are outside the source binding contract".into(),
                );
            }
            let source = target
                .get("src_path")
                .and_then(serde_json::Value::as_str)
                .ok_or("cargo metadata target omitted its src_path")?;
            validate_tracked_source_path(&root, Path::new(source), tracked, "target source")?;
        }
    }
    Ok(())
}

fn parse_tracked_source_paths(output: &str) -> Result<HashSet<PathBuf>, Box<dyn Error>> {
    output
        .split('\0')
        .filter(|value| !value.is_empty())
        .map(|value| {
            let path = PathBuf::from(value);
            if path.is_absolute()
                || path
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir))
            {
                return Err(format!("git reported a non-relative tracked path: {value:?}").into());
            }
            Ok(path)
        })
        .collect()
}

pub(crate) fn tracked_source_paths_at(root: &Path) -> Result<HashSet<PathBuf>, String> {
    let root = fs::canonicalize(root)
        .map_err(|error| format!("canonicalize source root {}: {error}", root.display()))?;
    // This inventory only constrains the Rust source analyzer; source acceptance is independently
    // proven from raw HEAD-tree bytes. Use the fixed system Git so catalog guards remain portable.
    let output = std::process::Command::new("/usr/bin/git")
        .args(["--no-replace-objects", "ls-files", "-z"])
        .env_clear()
        .envs(process::base_environment())
        .current_dir(&root)
        .output()
        .map_err(|error| format!("enumerate tracked source paths: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "enumerate tracked source paths: git exited with {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let output = String::from_utf8(output.stdout)
        .map_err(|error| format!("enumerate tracked source paths: {error}"))?;
    parse_tracked_source_paths(&output)
        .map_err(|error| format!("parse tracked source paths: {error}"))
}

fn validate_manifest_path_overrides(
    root: &Path,
    tracked: &HashSet<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let manifest_path = root.join("Cargo.toml");
    validate_tracked_source_path(&root, &manifest_path, tracked, "workspace manifest")?;
    let manifest: toml::Value = fs::read_to_string(&manifest_path)?.parse()?;
    for section in ["patch", "replace"] {
        if manifest.get(section).is_some() {
            return Err(format!(
                "Cargo manifest [{section}] overrides are outside the source binding contract"
            )
            .into());
        }
    }
    Ok(())
}

fn executable_sha256(name: &str) -> Result<String, Box<dyn Error>> {
    let path = find_tool(name).ok_or_else(|| format!("{name} is not present on PATH"))?;
    file_sha256(&path)
}

fn file_sha256(path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

fn capture_tool(
    name: &str,
    root: &Path,
    budget: CaptureBudget,
) -> Result<ToolReceipt, Box<dyn Error>> {
    let executable = find_tool(name).ok_or_else(|| format!("{name} is not present on PATH"))?;
    let version = bind_adjacent_tool_inputs(name, tool_version(name, root, budget)?, &executable)?;
    Ok(ToolReceipt {
        version,
        sha256: file_sha256(&executable)?,
    })
}

fn bind_adjacent_tool_inputs(
    name: &str,
    version: String,
    executable: &Path,
) -> Result<String, Box<dyn Error>> {
    if name != "maelstrom" {
        return Ok(version);
    }
    // `sha256` remains the profile-pinned launcher digest; the probe identity
    // carries the adjacent JAR digest so the complete executed tool is bound.
    let executable = fs::canonicalize(executable)?;
    let jar = maelstrom_jar_path(&executable)?;
    let jar_sha256 = file_sha256(&jar).map_err(|error| {
        format!(
            "bind Maelstrom launcher {} to adjacent {}: {error}",
            executable.display(),
            jar.display()
        )
    })?;
    Ok(format!(
        "{version}\nrafter-adjacent-lib/maelstrom.jar-sha256: {jar_sha256}"
    ))
}

pub(super) fn maelstrom_jar_path(executable: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let executable = fs::canonicalize(executable)?;
    Ok(executable
        .parent()
        .ok_or("Maelstrom launcher has no installation directory")?
        .join("lib/maelstrom.jar"))
}

fn tool_version(name: &str, root: &Path, budget: CaptureBudget) -> Result<String, Box<dyn Error>> {
    let arguments = tool_identity_arguments(name)?;
    let output = match budget {
        CaptureBudget::Execution => process::identity_command_in(name, arguments, root)?,
        CaptureBudget::Total => process::identity_command_in_total_budget(name, arguments, root)?,
    };
    tool_version_output(name, &output.stdout, &output.stderr)
}

fn tool_identity_arguments(name: &str) -> Result<&'static [&'static str], Box<dyn Error>> {
    TOOL_IDENTITY_PROBES
        .iter()
        .find_map(|(tool, arguments)| (*tool == name).then_some(*arguments))
        .ok_or_else(|| format!("no reviewed identity probe is registered for {name}").into())
}

fn tool_version_output(name: &str, stdout: &str, stderr: &str) -> Result<String, Box<dyn Error>> {
    let value = format!("{stdout}{stderr}").trim().to_owned();
    if value.is_empty() {
        return Err(format!("{name} produced empty identity output").into());
    }
    Ok(value)
}

fn find_tool(name: &str) -> Option<PathBuf> {
    env::split_paths(&env::var_os("PATH")?)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
}

pub(super) fn tool_path(name: &str) -> Option<PathBuf> {
    find_tool(name)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CargoRelease {
    major: u64,
    minor: u64,
}

impl CargoRelease {
    // Cargo releases before 1.94 hash the active config but do not load its
    // `include` targets.
    const CONFIG_INCLUDE: Self = Self {
        major: 1,
        minor: 94,
    };

    const fn new(major: u64, minor: u64) -> Self {
        Self { major, minor }
    }

    fn from_verbose_identity(identity: &str) -> Result<Self, Box<dyn Error>> {
        let mut releases = identity
            .lines()
            .filter_map(|line| line.strip_prefix("release: "));
        let release = releases.next().ok_or("cargo -vV omitted its release")?;
        if releases.next().is_some() {
            return Err("cargo -vV reported more than one release".into());
        }
        let mut components = release.split('.');
        let major = components
            .next()
            .ok_or("cargo -vV release omitted its major version")?
            .parse()?;
        let minor = components
            .next()
            .ok_or("cargo -vV release omitted its minor version")?
            .parse()?;
        Ok(Self::new(major, minor))
    }

    fn follows_config_includes(self) -> bool {
        self >= Self::CONFIG_INCLUDE
    }
}

fn cargo_config_sha256(root: &Path, cargo_identity: &str) -> Result<String, Box<dyn Error>> {
    let cargo_home = env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")));
    cargo_config_sha256_with_home(
        root,
        cargo_home.as_deref(),
        CargoRelease::from_verbose_identity(cargo_identity)?,
    )
}

fn cargo_config_sha256_with_home(
    root: &Path,
    cargo_home: Option<&Path>,
    cargo_release: CargoRelease,
) -> Result<String, Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let cargo_home_config = if let Some(home) = cargo_home {
        let home = if home.is_absolute() {
            home.to_owned()
        } else {
            root.join(home)
        };
        cargo_config_in(&home)?
            .map(|path| fs::canonicalize(&path).map(|canonical| (path, canonical)))
            .transpose()?
    } else {
        None
    };
    let mut ancestor_configs = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for ancestor in root.ancestors() {
        if let Some(path) = cargo_config_in(&ancestor.join(".cargo"))? {
            let canonical = fs::canonicalize(&path)?;
            if cargo_home_config
                .as_ref()
                .is_some_and(|(_, home)| home == &canonical)
            {
                continue;
            }
            if seen.insert(canonical) {
                ancestor_configs.push(path);
            }
        }
    }
    let mut paths = ancestor_configs
        .into_iter()
        .enumerate()
        .map(|(precedence, path)| {
            Ok((
                format!(
                    "ancestor:{precedence}:{}",
                    path.file_name()
                        .and_then(std::ffi::OsStr::to_str)
                        .ok_or("Cargo configuration filename is not valid UTF-8")?
                ),
                path,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if let Some((path, canonical)) = cargo_home_config {
        if seen.insert(canonical) {
            paths.push((
                format!(
                    "cargo-home:{}",
                    path.file_name()
                        .and_then(std::ffi::OsStr::to_str)
                        .ok_or("Cargo configuration filename is not valid UTF-8")?
                ),
                path,
            ));
        }
    }

    let mut hasher = Sha256::new();
    for (identity, path) in paths {
        hash_cargo_config_tree(
            &mut hasher,
            &identity,
            &path,
            cargo_release,
            &mut std::collections::BTreeSet::new(),
        )?;
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[derive(Debug)]
struct CargoConfigInclude {
    path: PathBuf,
    optional: bool,
}

fn hash_cargo_config_tree(
    hasher: &mut Sha256,
    identity: &str,
    path: &Path,
    cargo_release: CargoRelease,
    active: &mut std::collections::BTreeSet<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let canonical = fs::canonicalize(path)?;
    if !active.insert(canonical.clone()) {
        return Err(format!(
            "Cargo configuration include cycle reaches {}",
            path.display()
        )
        .into());
    }
    let contents = fs::read(path)?;
    let parsed = std::str::from_utf8(&contents)?
        .parse::<toml::Value>()
        .map_err(|error| format!("parse Cargo configuration {}: {error}", path.display()))?;
    validate_bound_cargo_config(&parsed, path)?;
    if cargo_release.follows_config_includes() {
        for (position, include) in cargo_config_includes(&parsed)?.into_iter().enumerate() {
            let include_identity =
                format!("{identity}:include:{position}:{}", include.path.display());
            let include_path = if include.path.is_absolute() {
                include.path
            } else {
                path.parent()
                    .ok_or_else(|| {
                        format!(
                            "Cargo configuration has no parent directory: {}",
                            path.display()
                        )
                    })?
                    .join(include.path)
            };
            match fs::metadata(&include_path) {
                Ok(metadata) if metadata.is_file() => {
                    hash_cargo_config_tree(
                        hasher,
                        &include_identity,
                        &include_path,
                        cargo_release,
                        active,
                    )?;
                }
                Ok(_) => {
                    return Err(format!(
                        "Cargo configuration include is not a file: {}",
                        include_path.display()
                    )
                    .into());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound && include.optional => {
                    hasher.update(include_identity.as_bytes());
                    hasher.update(b"\0optional-missing\0");
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(format!(
                        "required Cargo configuration include is missing: {}",
                        include_path.display()
                    )
                    .into());
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    hasher.update(identity.as_bytes());
    hasher.update([0]);
    hasher.update(contents);
    hasher.update([0]);
    active.remove(&canonical);
    Ok(())
}

fn validate_bound_cargo_config(
    configuration: &toml::Value,
    path: &Path,
) -> Result<(), Box<dyn Error>> {
    // These settings can name executables or filesystem inputs whose bytes are
    // outside the source receipt. Reject them instead of recording a false bind.
    let table = configuration.as_table().ok_or_else(|| {
        format!(
            "Cargo configuration root must be a table: {}",
            path.display()
        )
    })?;
    for key in [
        "paths",
        "path-bases",
        "patch",
        "source",
        "env",
        "resolver",
        "unstable",
    ] {
        if table.contains_key(key) {
            return unbound_cargo_config_error(path, key);
        }
    }
    if let Some(target) = table.get("target") {
        if let Some(setting) = nested_unbound_target_setting(target, "target") {
            return unbound_cargo_config_error(path, &setting);
        }
        return unbound_cargo_config_error(path, "target");
    }
    let Some(build) = table.get("build") else {
        return Ok(());
    };
    let build = build.as_table().ok_or_else(|| {
        format!(
            "Cargo configuration build setting must be a table: {}",
            path.display()
        )
    })?;
    for key in [
        "rustc",
        "rustc-wrapper",
        "rustc-workspace-wrapper",
        "rustdoc",
        "target",
        "target-dir",
        "rustflags",
        "rustdocflags",
    ] {
        if build.contains_key(key) {
            return unbound_cargo_config_error(path, &format!("build.{key}"));
        }
    }
    Ok(())
}

fn nested_unbound_target_setting(value: &toml::Value, prefix: &str) -> Option<String> {
    let table = value.as_table()?;
    for (key, value) in table {
        let setting = format!("{prefix}.{key}");
        if matches!(
            key.as_str(),
            "linker" | "runner" | "rustflags" | "rustdocflags"
        ) {
            return Some(setting);
        }
        if let Some(setting) = nested_unbound_target_setting(value, &setting) {
            return Some(setting);
        }
    }
    None
}

fn unbound_cargo_config_error<T>(path: &Path, setting: &str) -> Result<T, Box<dyn Error>> {
    Err(format!(
        "Cargo configuration {} uses unbound build input setting {setting}",
        path.display()
    )
    .into())
}

fn cargo_config_includes(
    configuration: &toml::Value,
) -> Result<Vec<CargoConfigInclude>, Box<dyn Error>> {
    let Some(include) = configuration.get("include") else {
        return Ok(Vec::new());
    };
    let entries = include
        .as_array()
        .ok_or("Cargo configuration include must be an array")?;
    entries
        .iter()
        .map(|entry| {
            let (path, optional) = if let Some(path) = entry.as_str() {
                (path, false)
            } else {
                let table = entry
                    .as_table()
                    .ok_or("Cargo configuration include entry must be a path or table")?;
                if table.keys().any(|key| key != "path" && key != "optional") {
                    return Err("Cargo configuration include table has an unknown field".into());
                }
                let path = table
                    .get("path")
                    .and_then(toml::Value::as_str)
                    .ok_or("Cargo configuration include table requires a string path")?;
                let optional = table
                    .get("optional")
                    .map(|value| {
                        value
                            .as_bool()
                            .ok_or("Cargo configuration include optional must be a boolean")
                    })
                    .transpose()?
                    .unwrap_or(false);
                (path, optional)
            };
            if Path::new(path)
                .extension()
                .and_then(std::ffi::OsStr::to_str)
                != Some("toml")
            {
                return Err(
                    format!("Cargo configuration include path must end in .toml: {path}").into(),
                );
            }
            Ok(CargoConfigInclude {
                path: PathBuf::from(path),
                optional,
            })
        })
        .collect()
}

fn cargo_config_in(directory: &Path) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let config = directory.join("config");
    if cargo_config_file_exists(&config)? {
        return Ok(Some(config));
    }
    let config_toml = directory.join("config.toml");
    if cargo_config_file_exists(&config_toml)? {
        Ok(Some(config_toml))
    } else {
        Ok(None)
    }
}

fn cargo_config_file_exists(path: &Path) -> Result<bool, Box<dyn Error>> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(format!("Cargo configuration path is not a file: {}", path.display()).into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
#[path = "source_identity_tests.rs"]
mod identity_tests;

#[cfg(test)]
#[path = "source_config_tests.rs"]
mod config_tests;
