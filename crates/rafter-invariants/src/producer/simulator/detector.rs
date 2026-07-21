//! Detector inventory, compilation, and qualified execution contract.

use std::{collections::BTreeMap, error::Error, path::Path, time::Instant};

use crate::{
    contract::{catalog::EvidenceDescriptor, TestIdentity},
    evidence::ArtifactRef,
};

use crate::producer::{
    artifact, process,
    test_compile::{compile, prepare_target_dir, CompiledTarget, Target},
    test_exec::{self, TestOutcome},
};

pub(super) struct DetectorRun {
    pub(super) outcomes: BTreeMap<String, TestOutcome>,
    pub(super) artifacts: Vec<ArtifactRef>,
    pub(super) peak_rss_kib: u64,
    pub(super) duration_ms: u64,
    pub(super) harness_error: Option<String>,
}

pub(super) fn run_detectors(
    descriptors: &[EvidenceDescriptor],
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<DetectorRun, Box<dyn Error>> {
    let identities = unique_detector_identities(
        descriptors
            .iter()
            .filter_map(|descriptor| descriptor.simulator.as_ref()?.negative_test.clone())
            .collect(),
    )?;
    if identities.is_empty() {
        return Err("simulator detector inventory is empty".into());
    }
    let detector_profile = format!("{profile}-simulator-detectors");
    let (execution_deadline, _) = process::active_layer_deadlines(profile, "simulator")?;
    let target_dir = prepare_target_dir(&detector_profile, source_ref, execution_deadline)?;
    let mut environment = process::base_environment();
    environment.insert(
        "CARGO_TARGET_DIR".to_owned(),
        target_dir.external_path().to_string_lossy().into_owned(),
    );
    let targets = identities
        .iter()
        .map(Target::from)
        .collect::<std::collections::BTreeSet<_>>();
    let mut compiled = BTreeMap::new();
    for target in targets {
        target_dir.verify()?;
        let outcome = compile(
            &target,
            &detector_profile,
            source_ref,
            &environment,
            output_dir,
        )?;
        compiled.insert(target, outcome);
    }
    evaluate_detectors(
        identities,
        &compiled,
        &detector_profile,
        source_ref,
        output_dir,
        execution_deadline,
    )
}

pub(super) fn unique_detector_identities(
    identities: Vec<TestIdentity>,
) -> Result<Vec<TestIdentity>, Box<dyn Error>> {
    let mut unique = BTreeMap::new();
    for identity in identities {
        let check_id = identity.check_id();
        if let Some(previous) = unique.insert(check_id.clone(), identity.clone()) {
            if previous != identity {
                return Err(
                    format!("colliding simulator detector check identity {check_id}").into(),
                );
            }
        }
    }
    Ok(unique.into_values().collect())
}

fn evaluate_detectors(
    identities: Vec<TestIdentity>,
    compiled: &BTreeMap<Target, CompiledTarget>,
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
    scratch_deadline: Instant,
) -> Result<DetectorRun, Box<dyn Error>> {
    let mut outcomes = BTreeMap::new();
    let mut peak_rss_kib = compiled
        .values()
        .map(|target| target.peak_rss_kib)
        .max()
        .unwrap_or_default();
    let mut artifacts = Vec::new();
    let mut duration_ms = compiled
        .values()
        .map(|target| target.duration_ms)
        .sum::<u64>();
    for target in compiled.values() {
        artifacts.push(target.artifact.clone());
        if let Some(binary) = &target.binary_artifact {
            artifacts.push(binary.clone());
        }
    }
    for identity in identities {
        let target = Target::from(&identity);
        let compiled_target = compiled
            .get(&target)
            .ok_or("compiled simulator detector target inventory changed")?;
        let execution_id = artifact::stable_id("detector", &identity.check_id());
        let mut outcome = test_exec::evaluate_detector(
            &identity,
            compiled_target,
            profile,
            source_ref,
            &execution_id,
            output_dir,
            scratch_deadline,
        )?;
        if let Some(binary) = &compiled_target.binary_artifact {
            outcome.artifacts.push(binary.clone());
        }
        peak_rss_kib = peak_rss_kib.max(outcome.peak_rss_kib);
        if !outcome
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "compile-log")
        {
            duration_ms = duration_ms.saturating_add(outcome.duration_ms);
        }
        if outcomes.insert(identity.check_id(), outcome).is_some() {
            return Err("duplicate simulator detector execution outcome".into());
        }
    }
    Ok(DetectorRun {
        outcomes,
        artifacts,
        peak_rss_kib,
        duration_ms,
        harness_error: None,
    })
}
