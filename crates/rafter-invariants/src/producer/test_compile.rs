use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
    fs,
    path::{Component, Path, PathBuf},
};

use serde::Deserialize;

use crate::{ArtifactRef, TestIdentity};

use super::{
    artifact,
    filesystem::{self as producer_fs, HeldDirectory, HeldFile, OperationDeadline, TREE_LIMITS},
    process,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct Target {
    package: String,
    kind: String,
    name: String,
}

pub(super) struct CompiledTarget {
    pub executable: Option<PathBuf>,
    pub executable_handle: Option<HeldFile>,
    pub binary_artifact: Option<ArtifactRef>,
    pub artifact: ArtifactRef,
    pub error: Option<String>,
    pub peak_rss_kib: u64,
    pub duration_ms: u64,
}

pub(super) struct PreparedTargetDir {
    handle: HeldDirectory,
}

impl PreparedTargetDir {
    pub(super) fn external_path(&self) -> PathBuf {
        self.handle.external_path()
    }

    pub(super) fn verify(&self) -> Result<(), Box<dyn Error>> {
        self.handle.verify_path_binding()
    }
}

#[derive(Debug, Deserialize)]
struct CargoCompilerMessage {
    reason: String,
    package_id: Option<String>,
    target: Option<CargoMessageTarget>,
    fresh: Option<bool>,
    executable: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct CargoMessageTarget {
    kind: Vec<String>,
    name: String,
    src_path: PathBuf,
}

pub(super) fn compile(
    target: &Target,
    profile: &str,
    source_ref: &str,
    environment: &BTreeMap<String, String>,
    output_dir: &Path,
) -> Result<CompiledTarget, Box<dyn Error>> {
    let mut arguments = vec![
        "test".into(),
        "--locked".into(),
        "--no-default-features".into(),
        "-p".into(),
        target.package.clone().into(),
    ];
    arguments.extend(target.selector()?);
    arguments.extend([
        "--no-run".into(),
        "--message-format=json-render-diagnostics".into(),
    ]);
    let output = process::timed_for(
        process::ProcessKind::Compile,
        "cargo",
        &arguments,
        environment,
        Path::new("."),
    )?;
    let artifact_id = artifact::stable_id(
        "compile",
        &format!("{profile}\0{source_ref}\0{}", target.key()),
    );
    let log = artifact::write(
        output_dir,
        Path::new(&format!("{profile}-tests/compile/{artifact_id}.log")),
        "compile-log",
        &process::combined_log(&target.key(), &output)?,
    )?;
    let (executable, error) = compile_result(&output, target);
    let executable_handle = executable
        .as_deref()
        .map(producer_fs::hold_file)
        .transpose()?;
    let binary_artifact = executable
        .as_deref()
        .map(|path| {
            artifact::capture(
                output_dir,
                Path::new(&format!("{profile}-tests/inputs")),
                path,
                "test-binary",
            )
        })
        .transpose()?;
    Ok(CompiledTarget {
        executable,
        executable_handle,
        binary_artifact,
        artifact: log,
        error,
        peak_rss_kib: output.peak_rss_kib,
        duration_ms: process::duration_ms(output.duration),
    })
}

fn compile_result(
    output: &process::ProcessOutput,
    target: &Target,
) -> (Option<PathBuf>, Option<String>) {
    if output.timed_out {
        return (
            None,
            Some(format!(
                "cargo test --no-run timed out for {}",
                target.key()
            )),
        );
    }
    if output.status.success() {
        match executable_from_messages(&output.stdout, target) {
            Ok(executable) => (Some(executable), None),
            Err(error) => (None, Some(error)),
        }
    } else {
        (
            None,
            Some(format!("cargo test --no-run failed for {}", target.key())),
        )
    }
}

fn executable_from_messages(bytes: &[u8], target: &Target) -> Result<PathBuf, String> {
    let mut executables = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<CargoCompilerMessage>(line) else {
            continue;
        };
        if message.reason != "compiler-artifact" {
            continue;
        }
        let Some(message_target) = message.target else {
            continue;
        };
        if message_target.name != target.name || message_target.kind != [target.kind.as_str()] {
            continue;
        }
        if message.fresh == Some(true) {
            return Err(format!(
                "fresh cached executable is forbidden for {}",
                target.key()
            ));
        }
        let package_id = message.package_id.ok_or_else(|| {
            format!(
                "compiler-artifact omitted Cargo package_id for {}",
                target.key()
            )
        })?;
        verify_package_identity(&package_id, &message_target.src_path, target)?;
        let executable = message
            .executable
            .ok_or_else(|| format!("compiler-artifact omitted executable for {}", target.key()))?;
        executables.push(canonical_test_executable(&executable, target)?);
    }
    if executables.len() == 1 {
        Ok(executables.remove(0))
    } else {
        Err(format!(
            "expected one executable for {}, found {}",
            target.key(),
            executables.len()
        ))
    }
}

fn verify_package_identity(
    package_id: &str,
    src_path: &Path,
    target: &Target,
) -> Result<(), String> {
    let current =
        fs::canonicalize(".").map_err(|error| format!("canonicalize workspace: {error}"))?;
    let expected_package_dir = current
        .ancestors()
        .map(|ancestor| ancestor.join("crates").join(&target.package))
        .find(|candidate| candidate.join("Cargo.toml").is_file())
        .ok_or_else(|| format!("workspace package {} is not present", target.package))?;
    let encoded = package_id.strip_prefix("path+file://").ok_or_else(|| {
        format!(
            "Cargo package_id for {} is not a workspace path package",
            target.package
        )
    })?;
    let (package_path, version) = encoded
        .rsplit_once('#')
        .ok_or_else(|| format!("Cargo package_id for {} has no version", target.package))?;
    if version.is_empty() {
        return Err(format!(
            "Cargo package_id for {} has an empty version",
            target.package
        ));
    }
    let observed_package_dir = fs::canonicalize(package_path).map_err(|error| {
        format!(
            "canonicalize Cargo package_id for {}: {error}",
            target.package
        )
    })?;
    let observed_source = fs::canonicalize(src_path).map_err(|error| {
        format!(
            "canonicalize Cargo target source for {}: {error}",
            target.key()
        )
    })?;
    if observed_package_dir != expected_package_dir
        || !observed_source.starts_with(&expected_package_dir)
    {
        return Err(format!(
            "Cargo package_id or source path does not match workspace package {}",
            target.package
        ));
    }
    Ok(())
}

fn canonical_test_executable(executable: &Path, target: &Target) -> Result<PathBuf, String> {
    if !executable.is_absolute()
        || executable
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(format!(
            "Cargo emitted a non-canonical executable for {}",
            target.key()
        ));
    }
    let canonical = fs::canonicalize(executable).map_err(|error| {
        format!(
            "canonicalize Cargo executable for {}: {error}",
            target.key()
        )
    })?;
    let expected_prefix = target.name.replace('-', "_");
    let file_name = canonical
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("Cargo executable for {} is not UTF-8", target.key()))?;
    if file_name != expected_prefix
        && !file_name
            .strip_prefix(&expected_prefix)
            .is_some_and(|suffix| suffix.starts_with('-'))
    {
        return Err(format!(
            "Cargo executable {file_name} does not match target {}",
            target.name
        ));
    }
    Ok(canonical)
}

pub(super) fn prepare_target_dir(
    profile: &str,
    source_ref: &str,
    deadline: std::time::Instant,
) -> Result<PreparedTargetDir, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let directory = Path::new("target/rafter-invariants/build")
        .join(source_prefix)
        .join(format!("{profile}-tests"));
    let directory_guard = HeldDirectory::replace_tree(
        &directory,
        TREE_LIMITS,
        OperationDeadline::at(deadline, "test compile scratch cleanup"),
    )?;
    directory_guard.verify_path_binding()?;
    Ok(PreparedTargetDir {
        handle: directory_guard,
    })
}

impl From<&TestIdentity> for Target {
    fn from(identity: &TestIdentity) -> Self {
        Self {
            package: identity.package.clone(),
            kind: identity.target_kind.clone(),
            name: identity.target.clone(),
        }
    }
}

impl Target {
    fn key(&self) -> String {
        format!("{}/{}/{}", self.package, self.kind, self.name)
    }

    fn selector(&self) -> Result<Vec<OsString>, Box<dyn Error>> {
        match self.kind.as_str() {
            "lib" => Ok(vec!["--lib".into()]),
            "test" => Ok(vec!["--test".into(), self.name.clone().into()]),
            "bin" => Ok(vec!["--bin".into(), self.name.clone().into()]),
            kind => Err(format!("unsupported Cargo target kind {kind}").into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{executable_from_messages, Target};

    #[test]
    fn compiler_artifact_binds_package_identity_and_target() {
        let package_dir = std::fs::canonicalize(env!("CARGO_MANIFEST_DIR"))
            .expect("canonical invariant package directory");
        let executable = std::fs::canonicalize(
            std::env::current_exe().expect("resolve invariant test executable"),
        )
        .expect("canonical invariant test executable");
        let target = Target {
            package: "rafter-invariants".to_owned(),
            kind: "lib".to_owned(),
            name: "rafter_invariants".to_owned(),
        };
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
        let exact = artifact(&package_dir);
        assert_eq!(
            executable_from_messages(exact.as_bytes(), &target)
                .expect("exact Cargo package artifact"),
            executable
        );

        let other_package = package_dir
            .parent()
            .expect("workspace crates directory")
            .join("rafter");
        assert!(executable_from_messages(artifact(&other_package).as_bytes(), &target).is_err());
    }
}
