//! Source, toolchain, and clean-checkout observation without acceptance policy.

use std::{
    collections::HashSet,
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::provenance::source::parse_tracked_source_paths;

mod cargo_graph;
mod cargo_inputs;
mod environment;
mod materialization;
mod path_validation;
mod rust_inputs;

#[cfg(test)]
#[path = "checkout/cargo_graph_tests.rs"]
mod cargo_graph_tests;
#[cfg(test)]
#[path = "checkout/observation_tests.rs"]
mod observation_tests;

use cargo_graph::validate_registry_build_script_source_identity;
use cargo_inputs::validate_trusted_cargo_package_metadata;
#[cfg(test)]
pub(crate) use environment::source_environment_matches_digest;
pub(crate) use environment::source_environment_sha256;
use materialization::capture_materialization;
pub(crate) use materialization::CapturedSourceFile;
pub(crate) use materialization::MaterializationObservation;
use path_validation::validate_tracked_source_path;
use rust_inputs::validate_resolved_tracked_rust_inputs;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckoutObservation {
    pub(crate) commit: String,
    pub(crate) tree: String,
    pub(crate) materialization: MaterializationObservation,
    pub(crate) cargo_lock_sha256: String,
    pub(crate) cargo: String,
    pub(crate) cargo_sha256: String,
    pub(crate) cargo_config_sha256: String,
    pub(crate) rustc: String,
    pub(crate) rustc_sha256: String,
    pub(crate) target: String,
}

#[derive(Debug)]
pub(crate) struct CapturedCheckout {
    pub(crate) observation: CheckoutObservation,
    pub(crate) files: Vec<CapturedSourceFile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandOutput {
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

pub(crate) trait CheckoutCommandRunner {
    fn run(
        &self,
        program: &str,
        arguments: &[&str],
        current_dir: &Path,
    ) -> Result<CommandOutput, Box<dyn Error>>;
}

pub(crate) trait GeneratedOutputPolicy {
    fn permits(&self, path: &Path) -> bool;
}

struct StandaloneCommandRunner;

impl CheckoutCommandRunner for StandaloneCommandRunner {
    fn run(
        &self,
        program: &str,
        arguments: &[&str],
        current_dir: &Path,
    ) -> Result<CommandOutput, Box<dyn Error>> {
        let output =
            crate::execution::process::run_identity_command_in(program, arguments, current_dir)?;
        Ok(CommandOutput {
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

pub(crate) fn observe_checkout_at(
    root: &Path,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<CheckoutObservation, Box<dyn Error>> {
    observe_checkout_with(root, &StandaloneCommandRunner, generated_outputs)
}

pub(crate) fn capture_checkout_at(
    root: &Path,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<CapturedCheckout, Box<dyn Error>> {
    capture_checkout_with(root, &StandaloneCommandRunner, generated_outputs)
}

pub(crate) fn head_commit_at(root: &Path) -> Result<String, Box<dyn Error>> {
    command_output_at(
        &StandaloneCommandRunner,
        "git",
        &["rev-parse", "HEAD"],
        false,
        root,
    )
}

pub(crate) fn identity_probe_at(
    program: &str,
    arguments: &[&str],
    root: &Path,
) -> Result<CommandOutput, Box<dyn Error>> {
    StandaloneCommandRunner.run(program, arguments, root)
}

pub(crate) fn observe_checkout_with(
    root: &Path,
    runner: &impl CheckoutCommandRunner,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<CheckoutObservation, Box<dyn Error>> {
    capture_checkout_with(root, runner, generated_outputs).map(|capture| capture.observation)
}

fn capture_checkout_with(
    root: &Path,
    runner: &impl CheckoutCommandRunner,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<CapturedCheckout, Box<dyn Error>> {
    require_clean_worktree(root, runner)?;
    let materialized = capture_materialization(root, runner, generated_outputs)?;
    let cargo = command_output_at(runner, "cargo", &["-vV"], false, root)?;
    let rustc = command_output_at(runner, "rustc", &["-vV"], false, root)?;
    let target = rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or_else(|| format!("rustc -vV omitted host target from output: {rustc:?}"))?
        .to_owned();
    let cargo_lock = fs::read(root.join("Cargo.lock"))?;
    let cargo_config_sha256 = cargo_config_sha256(root, &cargo)?;
    validate_resolved_path_packages(root, runner)?;
    require_clean_worktree(root, runner)?;
    Ok(CapturedCheckout {
        observation: CheckoutObservation {
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
        },
        files: materialized.files,
    })
}

fn require_clean_worktree(
    root: &Path,
    runner: &impl CheckoutCommandRunner,
) -> Result<(), Box<dyn Error>> {
    let status = command_output_at(
        runner,
        "git",
        &["status", "--porcelain=v1", "--untracked-files=all"],
        true,
        root,
    )?;
    if status.trim().is_empty() {
        Ok(())
    } else {
        Err("source observation requires a clean tracked and untracked worktree".into())
    }
}

fn command_output_at(
    runner: &impl CheckoutCommandRunner,
    program: &str,
    arguments: &[&str],
    allow_empty: bool,
    root: &Path,
) -> Result<String, Box<dyn Error>> {
    let stdout = command_stdout_at(runner, program, arguments, root)?;
    let value = stdout.trim().to_owned();
    if value.is_empty() && !allow_empty {
        return Err(format!("{program} produced empty identity output").into());
    }
    Ok(value)
}

fn command_output_raw_at(
    runner: &impl CheckoutCommandRunner,
    program: &str,
    arguments: &[&str],
    allow_empty: bool,
    root: &Path,
) -> Result<String, Box<dyn Error>> {
    let value = command_stdout_at(runner, program, arguments, root)?;
    if value.is_empty() && !allow_empty {
        return Err(format!("{program} produced empty identity output").into());
    }
    Ok(value)
}

fn command_stdout_at(
    runner: &impl CheckoutCommandRunner,
    program: &str,
    arguments: &[&str],
    root: &Path,
) -> Result<String, Box<dyn Error>> {
    let output = runner.run(program, arguments, root)?;
    Ok(output.stdout)
}

fn validate_resolved_path_packages(
    root: &Path,
    runner: &impl CheckoutCommandRunner,
) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let tracked = command_output_raw_at(runner, "git", &["ls-files", "-z"], true, &root)?;
    let tracked = parse_tracked_source_paths(&tracked)?;
    validate_manifest_path_overrides(&root, &tracked)?;
    let metadata = command_output_at(
        runner,
        "cargo",
        &["metadata", "--format-version", "1", "--locked", "--offline"],
        false,
        &root,
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
    let path = find_executable(name).ok_or_else(|| format!("{name} is not present on PATH"))?;
    file_sha256(&path)
}

pub(crate) fn file_sha256(path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

pub(crate) fn find_executable(name: &str) -> Option<PathBuf> {
    env::split_paths(&env::var_os("PATH")?)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
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
#[path = "checkout/config_tests.rs"]
mod config_tests;
