//! Synthetic observation frames and artifact sets, one shape per layer.
//!
//! Counters sit exactly at the floors the reviewed profile pins -- the weakest
//! run each contract accepts -- so a floor regression cannot hide behind a
//! generous fixture and a recalibration needs no fixture edit.

use super::maelstrom::{
    maelstrom_scenario, synthetic_maelstrom_artifacts, synthetic_maelstrom_observations,
};
use super::receipts::{artifact, artifact_kind};
use super::scenario::{synthetic_obligation_summary, tla_obligations, tla_policy};
use crate::*;

pub(super) fn synthetic_check_id(descriptor: &EvidenceDescriptor) -> String {
    if descriptor.layer == "simulator" {
        return format!("simulator/{}", descriptor.evidence_id());
    }
    if descriptor.layer == "tla" {
        return "tla/RaftCi.cfg#Spec".to_owned();
    }
    if descriptor.layer == "maelstrom" {
        return format!(
            "maelstrom/{}",
            maelstrom_scenario(&descriptor.path).expect("reviewed Maelstrom evidence path")
        );
    }
    descriptor
        .test
        .as_ref()
        .map_or_else(|| descriptor.evidence_id(), TestIdentity::check_id)
}

/// The TLA+ layer's synthetic observation frame.
///
/// Split out because it is the one layer whose frame depends on the profile's
/// continuation policy: a gating profile publishes the terminal counters of a
/// drained monolith, a reporting one the progress frame of a continuation that
/// spent its budget with the frontier still open.
pub(super) fn synthetic_tla_observations(
    descriptors: &[EvidenceDescriptor],
    manifest: &ProfileManifest,
    profile: &str,
) -> std::collections::BTreeMap<String, u64> {
    let mut observations = std::collections::BTreeMap::from([
        ("configured_invariants".to_owned(), 9),
        ("tool_pin_verified".to_owned(), 1),
        ("trace_sample_passed".to_owned(), 1),
    ]);
    // A gating profile publishes the terminal frame of a drained
    // monolith. A reporting profile publishes the progress frame of a
    // continuation that spent its budget with the frontier still open,
    // which is the shape its lane actually produces.
    if tla_policy(manifest, profile).gates() {
        // Counters sit exactly at the pinned floors -- the weakest run
        // the contract accepts -- so a floor regression cannot hide
        // behind a generous fixture, and a floor recalibration needs
        // no fixture edit.
        let configuration = &manifest.profiles[profile].runners["tla"].configuration;
        let floor = |key: &str| -> u64 {
            configuration[key]
                .parse()
                .expect("profile pins a numeric state floor")
        };
        observations.extend([
            (
                "generated_states".to_owned(),
                floor("minimum_generated_states"),
            ),
            (
                "distinct_states".to_owned(),
                floor("minimum_distinct_states"),
            ),
            ("states_left_on_queue".to_owned(), 0),
            ("search_depth".to_owned(), 1),
        ]);
    } else {
        observations.extend([
            ("progress_generated_states".to_owned(), 23_784_130),
            ("progress_distinct_states".to_owned(), 6_246_309),
            ("progress_states_left".to_owned(), 3_294_097),
            ("progress_depth".to_owned(), 21),
        ]);
    }
    observations.extend(
        crate::producer::tla_output::REGISTERED_PREDICATES
            .into_iter()
            .map(|predicate| (format!("detector_qualified:{predicate}"), 1)),
    );
    observations.extend(
        descriptors
            .iter()
            .map(|descriptor| (format!("checked:{}", descriptor.symbol), 1)),
    );
    observations.extend(
        crate::producer::tla_output::REQUIRED_MODEL_TRANSITIONS
            .into_iter()
            .map(|transition| (format!("transition_covered:{transition}"), 1)),
    );
    for obligation in tla_obligations(manifest, profile) {
        observations.extend(crate::producer::tla_output::obligation_observations(
            &obligation.id,
            &synthetic_obligation_summary(obligation),
            true,
        ));
    }
    observations
}

pub(super) fn synthetic_observations(
    descriptors: &[EvidenceDescriptor],
    manifest: &ProfileManifest,
    profile: &str,
) -> std::collections::BTreeMap<String, u64> {
    let descriptor = &descriptors[0];
    if descriptor.layer == "tests" {
        return std::collections::BTreeMap::from([
            ("discovered".to_owned(), 1),
            ("executed".to_owned(), 1),
            ("passed".to_owned(), 1),
        ]);
    }
    if descriptor.layer == "maelstrom" {
        return synthetic_maelstrom_observations(descriptor, manifest, profile);
    }
    let Some(identity) = &descriptor.simulator else {
        if descriptor.layer == "tla" {
            return synthetic_tla_observations(descriptors, manifest, profile);
        }
        return std::collections::BTreeMap::new();
    };
    let liveness_reports = identity.liveness_report.as_ref().map(|_| {
        identity.checks.len() as u64 * identity.minimum_runs_per_check.unwrap_or_default() as u64
    });
    let mut observations = std::collections::BTreeMap::from([
        ("detector_qualified".to_owned(), 1),
        (
            identity.required_observation.clone(),
            liveness_reports.unwrap_or(identity.minimum_observation as u64),
        ),
    ]);
    if let (Some(protocol), Some(verifier)) = (
        identity.minimum_protocol_states,
        identity.minimum_verifier_states,
    ) {
        observations.insert("unique_protocol_states".to_owned(), protocol as u64);
        observations.insert("unique_verifier_states".to_owned(), verifier as u64);
    }
    for check in &identity.checks {
        let runs = identity.minimum_runs_per_check.unwrap_or(1) as u64;
        observations.insert(format!("passes:{check}"), runs);
        observations.insert(format!("runs:{check}"), runs);
        if let Some(steps) = identity.minimum_steps {
            observations.insert(format!("steps:{check}"), steps as u64);
        }
        if let Some(contract) = manifest.profiles[profile].runners["simulator"]
            .simulator_checks
            .get(check)
        {
            observations.insert(
                crate::contract::profile::per_check_protocol_states_key(check),
                contract.minimum_protocol_states,
            );
            observations.insert(
                crate::contract::profile::per_check_verifier_states_key(check),
                contract.minimum_verifier_states,
            );
            observations.extend(contract.required_observations.iter().map(|observation| {
                (
                    crate::contract::profile::per_check_observation_key(check, observation),
                    1,
                )
            }));
        }
    }
    observations
}

pub(super) fn synthetic_liveness_binding(
    descriptor: &EvidenceDescriptor,
    profile: &str,
) -> Option<crate::evidence::SimulatorLivenessBinding> {
    let identity = descriptor.simulator.as_ref()?;
    let contract = identity.liveness_report.clone()?;
    let runs = identity.minimum_runs_per_check?;
    let mut reports = identity
        .checks
        .iter()
        .flat_map(|check_id| {
            let execution_contract =
                crate::contract::profile::expected_execution_contract(profile, check_id)
                    .expect("synthetic execution contract");
            (0..runs).map(
                move |index| crate::evidence::SimulatorLivenessReportBinding {
                    check_id: check_id.clone(),
                    seed: index as u64 + 1,
                    execution_contract_sha256: crate::evidence::execution_contract_digest(
                        &execution_contract,
                    ),
                    execution_contract: execution_contract.clone(),
                    report_sha256: format!("{:064x}", index + 1),
                    round_limit: 1,
                    rounds_used: 1,
                },
            )
        })
        .collect::<Vec<_>>();
    reports.sort();
    Some(crate::evidence::SimulatorLivenessBinding {
        schema_version: 1,
        contract_sha256: crate::evidence::liveness_contract_digest(&contract),
        reports_sha256: crate::evidence::liveness_reports_digest(&reports),
        contract,
        reports,
    })
}

pub(super) fn synthetic_artifacts(
    descriptor: &EvidenceDescriptor,
    manifest: &ProfileManifest,
    profile: &str,
) -> Vec<ArtifactRef> {
    match descriptor.layer.as_str() {
        "tests" => vec![
            artifact_kind("artifacts/tests.log", "test-log"),
            artifact_kind("artifacts/tests.bin", "test-binary"),
        ],
        "simulator" => {
            let mut artifacts = vec![
                artifact_kind("artifacts/simulator.log", "simulator-log"),
                artifact_kind("artifacts/simulator.bin", "simulator-binary"),
            ];
            if descriptor
                .simulator
                .as_ref()
                .is_some_and(|identity| identity.negative_test.is_some())
            {
                artifacts.extend([
                    artifact_kind("artifacts/detector.log", "test-log"),
                    artifact_kind("artifacts/detector.bin", "test-binary"),
                ]);
            }
            artifacts
        }
        "tla" => {
            let mut kinds = [
                "tla-log",
                "tla-trace-log",
                "tla-tool",
                "tla-spec",
                "tla-trace-spec",
                "tla-detector-spec",
                "tla-runner",
                "tla-tool-asset-id",
                "tla-tool-checksums",
                "tla-config",
                "tla-trace-config",
                "tla-detector-config",
                crate::producer::tla_output::MUTATION_SUITE_ARTIFACT_KIND,
            ]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
            for probe in crate::producer::tla_output::DETECTOR_PROBES {
                kinds.push(
                    crate::producer::tla_output::detector_log_kind(probe)
                        .expect("registered detector probe"),
                );
                kinds.push(
                    crate::producer::tla_output::detector_config_kind(probe)
                        .expect("registered detector probe"),
                );
            }
            // Two artifacts per obligation, and only two: the configuration TLC
            // read and the log it produced. Obligations never checkpoint, so
            // there is no recovery vocabulary to synthesize.
            for obligation in tla_obligations(manifest, profile) {
                kinds.push(crate::producer::tla_output::obligation_log_kind(
                    &obligation.id,
                ));
                kinds.push(crate::producer::tla_output::obligation_config_kind(
                    &obligation.id,
                ));
            }
            kinds
                .into_iter()
                .map(|kind| artifact_kind(&format!("artifacts/{kind}"), &kind))
                .collect()
        }
        "maelstrom" => synthetic_maelstrom_artifacts(descriptor, manifest, profile),
        runner => vec![artifact(&format!("artifacts/{runner}.log"))],
    }
}
