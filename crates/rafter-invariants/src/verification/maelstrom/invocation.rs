//! Exact Maelstrom trial input and process-invocation binding.

use std::{collections::BTreeMap, path::Path};

use crate::{
    evidence::{ArtifactRef, InvocationReceipt, ResultBundle},
    verification::{script_invocation_matches_source, AggregateError, AuthenticatedArtifacts},
};

use super::{artifact::unique, configuration, scenario::Scenario};

pub(super) fn verify_process(
    bundle: &ResultBundle,
    scenario: Scenario,
    trial: u64,
    artifacts: &[&ArtifactRef],
    observed: &InvocationReceipt,
    root: &Path,
) -> Result<(), AggregateError> {
    if !script_invocation_matches_source(observed, &bundle.execution.source) {
        return Err(error(
            "Maelstrom process log has wrong schema, label, or exact invocation",
        ));
    }
    let source_prefix = bundle.source_ref.get(..12).unwrap_or(&bundle.source_ref);
    let suffix = Path::new("target/rafter-invariants/maelstrom")
        .join(source_prefix)
        .join(&bundle.profile)
        .join(scenario.name())
        .join(format!("trial-{trial}"));
    let repository = std::fs::canonicalize(root).map_err(|canonicalize_error| {
        error(format!("canonicalize Maelstrom root: {canonicalize_error}"))
    })?;
    let state_dir = repository.join(suffix);
    let mut base_environment = observed.environment.clone();
    remove_trial_environment(&mut base_environment);
    let expected_environment =
        expected_environment(bundle, scenario, &repository, &state_dir, &base_environment)?;
    let runner = unique(artifacts, "maelstrom-runner")?;
    let expected_program = std::fs::canonicalize(repository.join(scenario.script())).map_err(
        |canonicalize_error| {
            error(format!(
                "canonicalize Maelstrom script: {canonicalize_error}"
            ))
        },
    )?;
    if observed.program != expected_program.to_string_lossy() {
        return Err(error(
            "Maelstrom process program path does not match its scenario",
        ));
    }
    if observed.program_sha256 != runner.sha256 {
        return Err(error(
            "Maelstrom process program digest does not match its runner artifact",
        ));
    }
    if observed.arguments != ["--test-count", "1"] {
        return Err(error(
            "Maelstrom process arguments do not match the exact invocation plan",
        ));
    }
    if observed.current_dir != state_dir.to_string_lossy() {
        return Err(error(
            "Maelstrom invocation working directory does not match its trial",
        ));
    }
    if observed.environment != expected_environment {
        return Err(error(
            "Maelstrom process environment does not match the exact invocation plan",
        ));
    }
    if base_environment != bundle.execution.invocation.environment {
        return Err(error(
            "Maelstrom base environment does not match producer invocation provenance",
        ));
    }
    Ok(())
}

pub(super) fn verify_trial_inputs(
    bundle: &ResultBundle,
    scenario: Scenario,
    artifacts: &[&ArtifactRef],
    root: &Path,
    source_root: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    super::artifact::verify_matches_file(
        unique(artifacts, "maelstrom-runner")?,
        source_root.join(scenario.script()),
        authenticated,
    )?;
    super::artifact::verify_matches_file(
        unique(artifacts, "maelstrom-binary")?,
        root.join("target/debug/rafter-maelstrom"),
        authenticated,
    )?;
    if unique(artifacts, "maelstrom-tool-jar")?.sha256
        != configuration::value(bundle, "maelstrom_jar_sha256")?
    {
        return Err(error("Maelstrom tool jar does not match the profile pin"));
    }
    if scenario.requires_proxy() {
        super::artifact::verify_matches_file(
            unique(artifacts, "maelstrom-proxy-binary")?,
            root.join("target/debug/rafter-maelstrom-leader-restart-proxy"),
            authenticated,
        )?;
    }
    Ok(())
}

fn expected_environment(
    bundle: &ResultBundle,
    scenario: Scenario,
    repository: &Path,
    state_dir: &Path,
    base: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, AggregateError> {
    let mut expected = base.clone();
    expected.extend([
        (
            "RAFTER_MAELSTROM_ROOT".to_owned(),
            state_dir.join("durable").to_string_lossy().into_owned(),
        ),
        (
            "RAFTER_MAELSTROM_SCRIPT_DIR".to_owned(),
            repository.join("scripts").to_string_lossy().into_owned(),
        ),
        (
            "RAFTER_MAELSTROM_TIME_LIMIT".to_owned(),
            configuration::value(bundle, "duration_seconds")?.to_owned(),
        ),
        (
            "RAFTER_MAELSTROM_RATE".to_owned(),
            configuration::value(bundle, "rate")?.to_owned(),
        ),
        (
            "RAFTER_MAELSTROM_CONCURRENCY".to_owned(),
            scenario.concurrency().to_owned(),
        ),
    ]);
    if scenario == Scenario::LeaseIsolation {
        expected.extend([
            (
                "RAFTER_MAELSTROM_RESTART_MODE".to_owned(),
                "lease-isolation".to_owned(),
            ),
            ("RAFTER_MAELSTROM_LEASE_EVIDENCE".to_owned(), "1".to_owned()),
            configured(
                bundle,
                "RAFTER_MAELSTROM_TICK_INTERVAL_MS",
                "lease_tick_interval_ms",
            )?,
            configured(
                bundle,
                "RAFTER_MAELSTROM_ELECTION_TIMEOUT_TICKS",
                "lease_election_timeout_ticks",
            )?,
            configured(
                bundle,
                "RAFTER_MAELSTROM_HEARTBEAT_INTERVAL_TICKS",
                "lease_heartbeat_interval_ticks",
            )?,
        ]);
    }
    Ok(expected)
}

fn configured(
    bundle: &ResultBundle,
    environment: &str,
    configuration: &str,
) -> Result<(String, String), AggregateError> {
    Ok((
        environment.to_owned(),
        super::configuration::value(bundle, configuration)?.to_owned(),
    ))
}

fn remove_trial_environment(environment: &mut BTreeMap<String, String>) {
    for name in [
        "RAFTER_MAELSTROM_ROOT",
        "RAFTER_MAELSTROM_SCRIPT_DIR",
        "RAFTER_MAELSTROM_TIME_LIMIT",
        "RAFTER_MAELSTROM_RATE",
        "RAFTER_MAELSTROM_CONCURRENCY",
        "RAFTER_MAELSTROM_RESTART_MODE",
        "RAFTER_MAELSTROM_LEASE_EVIDENCE",
        "RAFTER_MAELSTROM_TICK_INTERVAL_MS",
        "RAFTER_MAELSTROM_ELECTION_TIMEOUT_TICKS",
        "RAFTER_MAELSTROM_HEARTBEAT_INTERVAL_TICKS",
    ] {
        environment.remove(name);
    }
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}
