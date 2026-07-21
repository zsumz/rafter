//! Strict Cargo JSON message admission and replay-target candidate collection.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use super::super::{metadata::CompilationGraph, ReplayTarget};

#[derive(Deserialize)]
struct CompilerArtifact {
    package_id: String,
    target: MessageTarget,
    profile: MessageProfile,
    fresh: bool,
    executable: Option<PathBuf>,
}

#[derive(Deserialize)]
struct CompilerMessage {
    package_id: String,
    target: MessageTarget,
}

#[derive(Deserialize)]
struct BuildScriptExecuted {
    package_id: String,
    out_dir: PathBuf,
}

#[derive(Deserialize)]
struct BuildFinished {
    success: bool,
}

#[derive(Deserialize)]
struct MessageTarget {
    name: String,
    kind: Vec<String>,
    src_path: PathBuf,
}

#[derive(Deserialize)]
struct MessageProfile {
    test: bool,
}

struct CompilerTranscript {
    candidates: BTreeMap<ReplayTarget, Vec<PathBuf>>,
    build_finished: usize,
    messages: usize,
}

pub(super) fn parse(
    bytes: &[u8],
    graph: &CompilationGraph,
    target_root: &Path,
    expected: &BTreeSet<ReplayTarget>,
) -> Result<BTreeMap<ReplayTarget, Vec<PathBuf>>, String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| "Cargo compiler transcript is not UTF-8".to_owned())?;
    let mut transcript = CompilerTranscript {
        candidates: BTreeMap::new(),
        build_finished: 0,
        messages: 0,
    };
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        transcript.messages += 1;
        let value: serde_json::Value = serde_json::from_str(line).map_err(|error| {
            format!(
                "Cargo compiler transcript line {} is not JSON: {error}",
                index + 1
            )
        })?;
        transcript.accept(value, index + 1, graph, target_root, expected)?;
    }
    if transcript.messages == 0 || transcript.build_finished != 1 {
        return Err(format!(
            "Cargo compiler transcript requires messages and one successful build-finished record; messages={} build_finished={}",
            transcript.messages, transcript.build_finished
        ));
    }
    Ok(transcript.candidates)
}

impl CompilerTranscript {
    fn accept(
        &mut self,
        value: serde_json::Value,
        line: usize,
        graph: &CompilationGraph,
        target_root: &Path,
        expected: &BTreeSet<ReplayTarget>,
    ) -> Result<(), String> {
        let reason = value
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("Cargo compiler transcript line {line} omitted reason"))?;
        match reason {
            "compiler-artifact" => self.accept_artifact(value, line, graph, expected),
            "compiler-message" => {
                let message: CompilerMessage = serde_json::from_value(value).map_err(|error| {
                    format!("parse Cargo compiler-message line {line}: {error}")
                })?;
                verify_target(graph, &message.package_id, &message.target)
            }
            "build-script-executed" => {
                let script: BuildScriptExecuted = serde_json::from_value(value)
                    .map_err(|error| format!("parse Cargo build-script line {line}: {error}"))?;
                verify_build_script(graph, target_root, &script)
            }
            "build-finished" => {
                let finished: BuildFinished = serde_json::from_value(value)
                    .map_err(|error| format!("parse Cargo build-finished line {line}: {error}"))?;
                if !finished.success {
                    return Err("Cargo compiler transcript reports a failed build".to_owned());
                }
                self.build_finished += 1;
                Ok(())
            }
            other => Err(format!(
                "Cargo compiler transcript line {line} uses unknown reason {other:?}"
            )),
        }
    }

    fn accept_artifact(
        &mut self,
        value: serde_json::Value,
        line: usize,
        graph: &CompilationGraph,
        expected: &BTreeSet<ReplayTarget>,
    ) -> Result<(), String> {
        let artifact: CompilerArtifact = serde_json::from_value(value)
            .map_err(|error| format!("parse Cargo compiler-artifact line {line}: {error}"))?;
        verify_target(graph, &artifact.package_id, &artifact.target)?;
        if artifact.fresh {
            return Err(format!(
                "Cargo reused a cached compiler artifact for {}",
                artifact.package_id
            ));
        }
        let key = ReplayTarget {
            package: graph.package_name(&artifact.package_id)?.to_owned(),
            kind: single_kind(&artifact.target.kind)?.to_owned(),
            name: artifact.target.name,
        };
        if artifact.profile.test && expected.contains(&key) {
            let executable = artifact.executable.ok_or_else(|| {
                format!("selected replay target {key:?} omitted its test executable")
            })?;
            self.candidates.entry(key).or_default().push(executable);
        }
        Ok(())
    }
}

fn verify_build_script(
    graph: &CompilationGraph,
    target_root: &Path,
    script: &BuildScriptExecuted,
) -> Result<(), String> {
    if !graph.contains(&script.package_id) {
        return Err(format!(
            "Cargo build script uses unresolved package ID {}",
            script.package_id
        ));
    }
    let out_dir = fs::canonicalize(&script.out_dir)
        .map_err(|error| format!("canonicalize Cargo build-script output directory: {error}"))?;
    if !out_dir.starts_with(target_root) {
        return Err("Cargo build script escaped the private target directory".to_owned());
    }
    Ok(())
}

fn verify_target(
    graph: &CompilationGraph,
    package_id: &str,
    target: &MessageTarget,
) -> Result<(), String> {
    if target.name.is_empty() || target.kind.is_empty() {
        return Err("Cargo compiler target omitted its name or kind".to_owned());
    }
    graph.verify_target(package_id, &target.name, &target.kind, &target.src_path)
}

fn single_kind(kinds: &[String]) -> Result<&str, String> {
    let [kind] = kinds else {
        return Err("selected Cargo replay target does not have exactly one kind".to_owned());
    };
    Ok(kind)
}
