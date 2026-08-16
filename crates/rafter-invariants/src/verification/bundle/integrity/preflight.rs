//! Declaration validation and trusted runner resource policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
};

use crate::{
    evidence::{
        limits::{MAX_ARTIFACT_BYTES, MAX_ARTIFACT_KIND_BYTES, MAX_EVIDENCE_PATH_BYTES},
        ArtifactRef, PlanInput, ResultBundle,
    },
    verification::AggregateError,
};

use super::super::budget::{
    BundleBudget, MAX_PLAN_INPUT_BYTES, MAX_PLAN_INPUT_TOTAL_BYTES, MAX_RETAINED_ARTIFACT_BYTES,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeclaredArtifacts {
    pub(crate) references: usize,
    pub(crate) bytes: u64,
}

pub(super) fn plan_inputs(bundle: &ResultBundle) -> Result<Vec<&PlanInput>, AggregateError> {
    let inputs = Vec::from([
        &bundle.execution.plan.registry,
        &bundle.execution.plan.manifest,
        &bundle.execution.plan.result_schema,
        &bundle.execution.plan.verdict_schema,
    ]);
    let mut paths = BTreeSet::new();
    for input in &inputs {
        validate_declared_path(&input.path, "plan input")?;
        if input.size_bytes > MAX_PLAN_INPUT_BYTES {
            return Err(AggregateError::new(format!(
                "plan input {} declares {} bytes, exceeding the {MAX_PLAN_INPUT_BYTES}-byte limit",
                input.path, input.size_bytes
            )));
        }
        if !paths.insert(input.path.as_str()) {
            return Err(AggregateError::new(format!(
                "plan repeats input path {}",
                input.path
            )));
        }
    }
    let total = checked_sum(
        inputs.iter().map(|input| input.size_bytes),
        "plan input size",
    )?;
    if total > MAX_PLAN_INPUT_TOTAL_BYTES {
        return Err(AggregateError::new(format!(
            "plan inputs declare {total} bytes, exceeding the {MAX_PLAN_INPUT_TOTAL_BYTES}-byte limit"
        )));
    }
    Ok(inputs)
}

pub(super) fn artifacts<'a>(
    bundle: &'a ResultBundle,
    budget: BundleBudget,
    trusted_runner: &str,
) -> Result<BTreeMap<String, &'a ArtifactRef>, AggregateError> {
    let declarations = bundle
        .execution
        .artifacts
        .iter()
        .chain(std::iter::once(&bundle.execution.producer.executable))
        .chain(
            bundle
                .execution
                .checks
                .iter()
                .flat_map(|check| check.artifacts.iter()),
        )
        .chain(
            bundle
                .results
                .iter()
                .flat_map(|result| result.artifacts.iter()),
        )
        .collect::<Vec<_>>();
    if declarations.len() > budget.declarations {
        return Err(AggregateError::new(format!(
            "{} receipt declares {} artifact references, exceeding the {}-declaration limit",
            bundle.runner,
            declarations.len(),
            budget.declarations
        )));
    }

    let mut artifacts = BTreeMap::new();
    for artifact in declarations {
        validate_artifact_ref(artifact)?;
        retain_semantic_bytes(trusted_runner, &artifact.kind)?;
        if let Some(existing) = artifacts.insert(artifact.path.clone(), artifact) {
            if existing != artifact {
                return Err(AggregateError::new(format!(
                    "artifact path has conflicting declarations: {}",
                    artifact.path
                )));
            }
        }
    }
    if artifacts.len() > budget.references {
        return Err(AggregateError::new(format!(
            "{} receipt declares {} distinct artifact references, exceeding the {}-reference limit",
            bundle.runner,
            artifacts.len(),
            budget.references
        )));
    }
    let artifact_bytes = checked_sum(
        artifacts.values().map(|artifact| artifact.size_bytes),
        "artifact bundle size",
    )?;
    if artifact_bytes > budget.bytes {
        return Err(AggregateError::new(format!(
            "{} artifact bundle declares {artifact_bytes} bytes, exceeding the {}-byte limit",
            bundle.runner, budget.bytes
        )));
    }

    let mut retained_content = BTreeSet::new();
    for artifact in artifacts.values() {
        if retain_semantic_bytes(trusted_runner, &artifact.kind)? {
            retained_content.insert((artifact.sha256.as_str(), artifact.size_bytes));
        }
    }
    let retained_bytes = checked_sum(
        retained_content
            .into_iter()
            .map(|(_, size_bytes)| size_bytes),
        "retained artifact size",
    )?;
    if retained_bytes > MAX_RETAINED_ARTIFACT_BYTES {
        return Err(AggregateError::new(format!(
            "{} semantic snapshot declares {retained_bytes} bytes, exceeding the {MAX_RETAINED_ARTIFACT_BYTES}-byte limit",
            bundle.runner
        )));
    }
    Ok(artifacts)
}

pub(crate) fn declared_artifacts(
    bundle: &ResultBundle,
    budget: BundleBudget,
    trusted_runner: &str,
) -> Result<DeclaredArtifacts, AggregateError> {
    let artifacts = artifacts(bundle, budget, trusted_runner)?;
    Ok(DeclaredArtifacts {
        references: artifacts.len(),
        bytes: checked_sum(
            artifacts.values().map(|artifact| artifact.size_bytes),
            "artifact bundle size",
        )?,
    })
}

pub(super) fn validate_artifact_ref(artifact: &ArtifactRef) -> Result<(), AggregateError> {
    validate_declared_path(&artifact.path, "artifact")?;
    if artifact.kind.len() > MAX_ARTIFACT_KIND_BYTES {
        return Err(AggregateError::new(format!(
            "artifact kind exceeds the {MAX_ARTIFACT_KIND_BYTES}-byte limit"
        )));
    }
    if artifact.size_bytes > MAX_ARTIFACT_BYTES {
        return Err(AggregateError::new(format!(
            "artifact {} declares {} bytes, exceeding the {MAX_ARTIFACT_BYTES}-byte limit",
            artifact.path, artifact.size_bytes
        )));
    }
    Ok(())
}

fn validate_declared_path(path: &str, label: &str) -> Result<(), AggregateError> {
    if path.len() > MAX_EVIDENCE_PATH_BYTES {
        return Err(AggregateError::new(format!(
            "{label} path exceeds the {MAX_EVIDENCE_PATH_BYTES}-byte limit"
        )));
    }
    let path = Path::new(path);
    let normalized = path.components().collect::<PathBuf>();
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || normalized.as_os_str() != path.as_os_str()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(AggregateError::new(format!(
            "{label} path must be repository-relative: {}",
            path.display()
        )));
    }
    Ok(())
}

fn checked_sum(values: impl IntoIterator<Item = u64>, label: &str) -> Result<u64, AggregateError> {
    values.into_iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| AggregateError::new(format!("{label} overflowed u64")))
    })
}

pub(super) fn retain_semantic_bytes(runner: &str, kind: &str) -> Result<bool, AggregateError> {
    retention_policy(runner, kind).ok_or_else(|| {
        AggregateError::new(format!(
            "runner {runner} declared unreviewed artifact kind {kind}"
        ))
    })
}

fn retention_policy(runner: &str, kind: &str) -> Option<bool> {
    if matches!(kind, "producer-binary" | "summary") {
        return Some(false);
    }
    match runner {
        "tests" => match kind {
            "test-binary" => Some(false),
            "compile-log" | "test-log" => Some(true),
            _ => None,
        },
        "simulator" => match kind {
            "test-binary" | "simulator-binary" => Some(false),
            "compile-log" | "test-log" | "simulator-log" => Some(true),
            _ => None,
        },
        "tla" => match kind {
            "tla-tool" => Some(false),
            "tla-log"
            | "tla-trace-log"
            | "tla-mutation-log"
            | "tla-spec"
            | "tla-trace-spec"
            | "tla-detector-spec"
            | "tla-runner"
            | "tla-tool-asset-id"
            | "tla-tool-checksums"
            | "tla-config"
            | "tla-trace-config"
            | "tla-detector-config"
            | "tla-checkpoint-contract"
            | "tla-checkpoint-inventory"
            | "tla-checkpoint-recovered-contract"
            | "tla-checkpoint-recovered-inventory"
            | "tla-checkpoint-recovery-report" => Some(true),
            // Per-obligation logs and configurations are text the verifier
            // reads back and re-parses, exactly like the detector pair, so
            // their semantic bytes are retained. They are named dynamically
            // because the reviewed obligation set is profile data.
            dynamic
                if dynamic.starts_with("tla-detector-log:")
                    || dynamic.starts_with("tla-detector-config:")
                    || dynamic.starts_with("tla-obligation-log:")
                    || dynamic.starts_with("tla-obligation-config:") =>
            {
                Some(true)
            }
            _ => None,
        },
        "maelstrom" => match kind {
            "maelstrom-tool-jar" | "maelstrom-durable-file" => Some(false),
            "maelstrom-results"
            | "maelstrom-process-log"
            | "maelstrom-runner"
            | "maelstrom-binary"
            | "maelstrom-proxy-binary"
            | "maelstrom-node-log"
            | "maelstrom-store-file" => Some(true),
            _ => None,
        },
        _ => None,
    }
}
