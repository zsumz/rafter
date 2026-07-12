use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;

use crate::{ArtifactRef, TestIdentity};

use super::{artifact, process};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct Target {
    package: String,
    kind: String,
    name: String,
}

pub(super) struct CompiledTarget {
    pub executable: Option<PathBuf>,
    pub binary_artifact: Option<ArtifactRef>,
    pub artifact: ArtifactRef,
    pub error: Option<String>,
    pub peak_rss_kib: u64,
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
    let output = process::timed("cargo", &arguments, environment, Path::new("."))?;
    let artifact_id = artifact::stable_id(
        "compile",
        &format!("{profile}\0{source_ref}\0{}", target.key()),
    );
    let log = artifact::write(
        output_dir,
        Path::new(&format!("{profile}-tests/compile/{artifact_id}.log")),
        "compile-log",
        &process::combined_log(&target.key(), &output),
    )?;
    let (executable, error) = compile_result(&output, target);
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
        binary_artifact,
        artifact: log,
        error,
        peak_rss_kib: output.peak_rss_kib,
    })
}

fn compile_result(
    output: &process::ProcessOutput,
    target: &Target,
) -> (Option<PathBuf>, Option<String>) {
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
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if message["reason"] == "compiler-artifact"
            && message["target"]["name"] == target.name
            && message["target"]["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind == &target.kind))
        {
            if message["fresh"] == true {
                return Err(format!(
                    "fresh cached executable is forbidden for {}",
                    target.key()
                ));
            }
            if let Some(executable) = message["executable"].as_str() {
                executables.push(PathBuf::from(executable));
            }
        }
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

pub(super) fn prepare_target_dir(
    profile: &str,
    source_ref: &str,
) -> Result<PathBuf, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let directory = Path::new("target/rafter-invariants/build")
        .join(source_prefix)
        .join(format!("{profile}-tests"));
    if directory.exists() {
        fs::remove_dir_all(&directory)?;
    }
    fs::create_dir_all(&directory)?;
    Ok(directory)
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
