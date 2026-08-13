//! Exact Maelstrom trial input and process-invocation binding.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::{
    evidence::{ArtifactRef, InvocationReceipt, ResultBundle},
    verification::{
        script_invocation_matches_source, AggregateError, AuthenticatedArtifacts,
        VerificationContext,
    },
};

use super::{artifact::unique, configuration, scenario::Scenario};

pub(super) fn verify_process(
    bundle: &ResultBundle,
    scenario: Scenario,
    trial: u64,
    runner: &ArtifactRef,
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

/// How a captured shared input is independently re-derived.
///
/// The two classes differ in what a later job can honestly reconstruct, not in
/// how much they are trusted.
pub(super) enum InputBinding {
    /// A version-controlled file. Any checkout of the reviewed commit holds
    /// the same bytes, so byte-equality against the checkout is a real
    /// independent derivation in every context.
    Checkout(PathBuf),
    /// A build output. Only the job that built it has the file; a later job
    /// has the repository but not the artifacts of someone else's `cargo
    /// build`, and cannot reproduce them byte-for-byte either -- the invariant
    /// jobs each set their own `CARGO_HOME` and `CARGO_TARGET_DIR`, whose
    /// paths debug binaries embed.
    BuildOutput(PathBuf),
}

/// Verifies one shared input against the strongest claim its context supports.
///
/// A build output binds by byte-equality where the build happened, which is
/// where that comparison means something, and by artifact integrity everywhere
/// else: the published bytes are the ones the receipt names, digest and length
/// both. That is not a weaker acceptance of the same claim, it is the claim
/// that is actually available -- the provenance of those bytes is carried by
/// the source receipt and the producing job's own verification, which runs
/// this same function against the real file and still fails closed.
///
/// This binding had been aggregate-unsatisfiable since it was written. It went
/// unnoticed because no aggregate run had ever reached it: the scheduled lanes
/// failed earlier, for other reasons, until this branch made every layer green
/// at once and the aggregate got far enough to read a file that could not
/// exist.
#[cfg(test)]
pub(super) fn verify_input_binding_for_test(
    artifact: &ArtifactRef,
    binding: &InputBinding,
    context: VerificationContext,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    verify_input_binding(artifact, binding, context, authenticated)
}

fn verify_input_binding(
    artifact: &ArtifactRef,
    binding: &InputBinding,
    context: VerificationContext,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    match (binding, context) {
        (InputBinding::Checkout(path), _)
        | (InputBinding::BuildOutput(path), VerificationContext::ProducingJob) => {
            super::artifact::verify_matches_file(artifact, path, authenticated)
        }
        (InputBinding::BuildOutput(_), VerificationContext::Aggregate) => {
            verify_artifact_integrity(artifact, authenticated)
        }
    }
}

/// Re-derives the published bytes' identity from the bytes themselves.
fn verify_artifact_integrity(
    artifact: &ArtifactRef,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    let bytes = authenticated.bytes(artifact)?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    if digest != artifact.sha256 || bytes.len() as u64 != artifact.size_bytes {
        return Err(error(format!(
            "published {} does not match the identity its receipt claims",
            artifact.kind
        )));
    }
    Ok(())
}

pub(super) fn verify_shared_inputs(
    bundle: &ResultBundle,
    scenario: Scenario,
    artifacts: &[&ArtifactRef],
    root: &Path,
    source_root: &Path,
    authenticated: &AuthenticatedArtifacts,
    context: VerificationContext,
) -> Result<(), AggregateError> {
    // The runner script is version controlled, so every context can re-derive
    // it from the checkout. The node binary is a build output: see
    // `verify_input_binding` for why byte-equality against a checkout path is
    // a producer-local claim.
    verify_input_binding(
        unique(artifacts, "maelstrom-runner")?,
        &InputBinding::Checkout(source_root.join(scenario.script())),
        context,
        authenticated,
    )?;
    verify_input_binding(
        unique(artifacts, "maelstrom-binary")?,
        &InputBinding::BuildOutput(root.join("target/debug/rafter-maelstrom")),
        context,
        authenticated,
    )?;
    if unique(artifacts, "maelstrom-tool-jar")?.sha256
        != configuration::value(bundle, "maelstrom_jar_sha256")?
    {
        return Err(error("Maelstrom tool jar does not match the profile pin"));
    }
    let proxies = artifacts
        .iter()
        .filter(|artifact| artifact.kind == "maelstrom-proxy-binary")
        .count();
    if scenario.requires_proxy() {
        verify_input_binding(
            unique(artifacts, "maelstrom-proxy-binary")?,
            &InputBinding::BuildOutput(
                root.join("target/debug/rafter-maelstrom-leader-restart-proxy"),
            ),
            context,
            authenticated,
        )?;
    } else if proxies != 0 {
        return Err(error(
            "Maelstrom proxy binary is not an input of this scenario",
        ));
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
