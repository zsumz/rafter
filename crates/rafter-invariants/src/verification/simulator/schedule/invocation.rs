//! Exact simulator runtime invocation and captured-binary binding.

use std::path::Path;

use super::{compiler::emitted_simulator_executable, paths::simulator_roots};
use crate::{
    contract::profile::SimulatorRunnerConfiguration,
    evidence::{InvocationReceipt, ResultBundle},
    verification::{process_invocation_matches_source, AggregateError, AuthenticatedArtifacts},
};

pub(super) struct InvocationVerification {
    pub(super) diagnostics: Vec<String>,
    pub(super) complete: bool,
}

pub(super) fn verify_simulator_invocations(
    bundle: &ResultBundle,
    root: &Path,
    configuration: &SimulatorRunnerConfiguration,
    sources: &[&str],
    authenticated: &AuthenticatedArtifacts,
) -> Result<InvocationVerification, AggregateError> {
    let roots = simulator_roots(bundle, root)?;
    let binaries = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "simulator-binary")
        .collect::<Vec<_>>();
    let [binary] = binaries.as_slice() else {
        return Err(AggregateError::new(format!(
            "simulator execution must capture exactly one binary artifact, found {}",
            binaries.len()
        )));
    };
    let emitted = emitted_simulator_executable(bundle, &roots, authenticated)?;
    let environment = &bundle.execution.invocation.environment;
    let environment_sha256 = bundle.execution.invocation.environment_sha256.as_str();
    let expected = expected_simulator_invocations(bundle, configuration)?;
    if sources.len() > expected.len() {
        return Err(AggregateError::new(
            "simulator log count exceeds the execution plan".to_owned(),
        ));
    }
    let mut diagnostics = Vec::new();
    let expected_count = expected.len();
    let mut matched = 0_usize;
    for (label, arguments) in expected {
        let Some(source) = sources
            .iter()
            .find(|source| source.lines().any(|line| line == format!("label: {label}")))
        else {
            diagnostics.push(format!(
                "simulator execution plan did not run required profile {label}"
            ));
            continue;
        };
        matched += 1;
        let processes = crate::evidence::format::process::parse_combined_v4(source)
            .map_err(|error| AggregateError::new(format!("parse simulator invocation: {error}")))?;
        let [observed] = processes.as_slice() else {
            return Err(AggregateError::new(format!(
                "simulator log {label} must contain exactly one invocation"
            )));
        };
        if let Err(error) =
            verify_simulator_invocation_outcome(&label, observed.exit_code, observed.timed_out)
        {
            diagnostics.push(error.to_string());
        }
        if observed.label != label
            || observed.invocation.arguments != arguments
            || !simulator_program_matches(&observed.invocation, &emitted, &binary.sha256)
            || !process_invocation_matches_source(&observed.invocation, &bundle.execution.source)
            || Path::new(&observed.invocation.current_dir) != roots.producer
            || !invocation_environment_matches(
                &observed.invocation,
                environment,
                environment_sha256,
            )
        {
            return Err(AggregateError::new(format!(
                "simulator log {label} does not match the exact invocation plan"
            )));
        }
    }
    if matched != sources.len() {
        return Err(AggregateError::new(
            "simulator logs contain an unexpected or duplicate invocation".to_owned(),
        ));
    }
    Ok(InvocationVerification {
        diagnostics,
        complete: matched == expected_count,
    })
}

fn expected_simulator_invocations(
    bundle: &ResultBundle,
    configuration: &SimulatorRunnerConfiguration,
) -> Result<Vec<(String, Vec<String>)>, AggregateError> {
    match bundle.profile.as_str() {
        "pr" => Ok(vec![
            (
                "fast".to_owned(),
                vec!["--profile".to_owned(), "fast".to_owned()],
            ),
            (
                "raft-soak".to_owned(),
                vec!["--profile".to_owned(), "raft-soak".to_owned()],
            ),
        ]),
        profile @ ("nightly" | "weekly") => {
            // The invocation names the model profile the lane runs, which is
            // not the lane name once a lane runs a sibling's profile.
            let label = crate::contract::profile::scheduled_model_profile(profile)
                .ok_or_else(|| AggregateError::new(format!("unknown simulator profile {profile}")))?
                .to_owned();
            let seed_count = configuration
                .seed_count
                .and_then(|count| usize::try_from(count).ok())
                .ok_or_else(|| AggregateError::new("scheduled seed count is missing".to_owned()))?;
            let seeds = crate::contract::profile::scheduled_simulator_seeds(
                profile,
                &bundle.source_ref,
                seed_count,
            )
            .ok_or_else(|| AggregateError::new("scheduled seeds are missing".to_owned()))?;
            Ok(vec![(
                label.clone(),
                vec!["--profile".to_owned(), label, "--seed".to_owned(), seeds],
            )])
        }
        profile => Err(AggregateError::new(format!(
            "unknown simulator profile {profile}"
        ))),
    }
}

fn invocation_environment_matches(
    invocation: &InvocationReceipt,
    expected: &std::collections::BTreeMap<String, String>,
    expected_digest: &str,
) -> bool {
    invocation.environment == *expected
        && invocation.environment_sha256 == expected_digest
        && crate::provenance::invocation::environment_matches_digest(
            &invocation.environment,
            expected_digest,
        )
}

pub(crate) fn verify_simulator_invocation_outcome(
    label: &str,
    exit_code: Option<i32>,
    timed_out: bool,
) -> Result<(), AggregateError> {
    if exit_code != Some(0) || timed_out {
        return Err(AggregateError::new(format!(
            "simulator log {label} requires a zero-exit invocation that did not time out"
        )));
    }
    Ok(())
}

pub(crate) fn simulator_program_matches(
    invocation: &InvocationReceipt,
    emitted: &Path,
    captured_sha256: &str,
) -> bool {
    Path::new(&invocation.program) == emitted
        && emitted.is_absolute()
        && invocation.program_sha256 == captured_sha256
}
