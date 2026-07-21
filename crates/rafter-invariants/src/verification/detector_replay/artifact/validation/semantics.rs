//! Cross-field semantics for complete and failed replay reports.

use std::collections::BTreeSet;

use super::{
    super::model::{CompilationStatus, FixtureReport, ReplayReport, REPORT_SCHEMA_VERSION},
    inventory, process, value,
};

pub(super) fn validate(report: &ReplayReport) -> Result<(), String> {
    if report.schema_version != REPORT_SCHEMA_VERSION {
        return Err(format!(
            "verifier replay report schema is {}, expected {REPORT_SCHEMA_VERSION}",
            report.schema_version
        ));
    }
    value::require_nonempty(&report.profile, "profile")?;
    value::require_nonempty(&report.source_ref, "source reference")?;
    value::validate_source(report)?;
    validate_contract(report)?;
    validate_registry(report)?;
    super::runtime::validate(report)?;
    match report.compilation.status {
        CompilationStatus::Passed => validate_completed(report),
        CompilationStatus::HarnessError => validate_failed(report),
    }
}

fn validate_contract(report: &ReplayReport) -> Result<(), String> {
    let contract = &report.contract;
    value::require_digest(
        &contract.required_inventory_sha256,
        "reviewed replay inventory",
    )?;
    if contract.required_unique_fixtures == 0
        || contract.required_evidence_bindings == 0
        || contract.required_targets == 0
        || contract.required_registry_packages == 0
        || contract.compile_timeout_seconds == 0
        || contract.fixture_timeout_seconds == 0
        || contract.total_timeout_seconds == 0
    {
        return Err("verifier replay contract contains a zero required bound".to_owned());
    }
    if let Some(sha256) = &report.inventory.sha256 {
        value::require_digest(sha256, "observed replay inventory")?;
        if sha256 != &contract.required_inventory_sha256
            || report.inventory.fixtures != contract.required_unique_fixtures
            || report.inventory.evidence_bindings != contract.required_evidence_bindings
            || report.inventory.targets != contract.required_targets
        {
            return Err("observed replay inventory differs from reviewed contract".to_owned());
        }
    } else if report.inventory.fixtures != 0 || report.inventory.targets != 0 {
        return Err("unhashed replay inventory claims fixture or target coverage".to_owned());
    }
    Ok(())
}

fn validate_registry(report: &ReplayReport) -> Result<(), String> {
    let Some(registry) = &report.registry else {
        return Ok(());
    };
    value::require_digest(&registry.lock_sha256, "registry lock")?;
    if registry.lock_sha256 != report.source.cargo_lock_sha256 {
        return Err("authenticated registry lock differs from source Cargo.lock".to_owned());
    }
    value::require_digest(&registry.materialization_sha256, "registry materialization")?;
    if registry.package_count == 0
        || registry.archive_bytes == 0
        || registry.expanded_bytes == 0
        || registry.entries == 0
    {
        return Err("authenticated registry receipt contains an empty bound".to_owned());
    }
    Ok(())
}

fn validate_completed(report: &ReplayReport) -> Result<(), String> {
    if report.compilation.message.is_some() {
        return Err("passed replay compilation carries an error message".to_owned());
    }
    let metadata_sha256 = report
        .compilation
        .metadata_sha256
        .as_deref()
        .ok_or_else(|| "passed replay compilation has no metadata digest".to_owned())?;
    value::require_digest(metadata_sha256, "Cargo metadata")?;
    let registry = report
        .registry
        .as_ref()
        .ok_or_else(|| "passed replay compilation has no registry receipt".to_owned())?;
    if registry.package_count != report.contract.required_registry_packages {
        return Err("passed replay registry package count differs from contract".to_owned());
    }
    if report.inventory.sha256.is_none() {
        return Err("passed replay compilation has no observed inventory digest".to_owned());
    }
    if report.compilation.targets.len() != report.inventory.targets
        || report.fixtures.len() != report.inventory.fixtures
    {
        return Err("passed replay report counts differ from observed inventory".to_owned());
    }
    inventory::validate(report)?;
    process::validate_all(&report.compilation.processes, true)?;
    let compilation_limit = successful_process_limit(report.contract.compile_timeout_seconds)?;
    for process in &report.compilation.processes {
        process::require_duration_at_most(process, compilation_limit)?;
    }
    let roles = report
        .compilation
        .processes
        .iter()
        .map(process::role)
        .collect::<BTreeSet<_>>();
    if roles != BTreeSet::from(["cargo-metadata", "cargo-test-no-run"]) {
        return Err(
            "passed replay compilation does not contain the exact required process roles"
                .to_owned(),
        );
    }
    if report
        .compilation
        .processes
        .iter()
        .any(|process| process::execution_id(process) != process::role(process))
    {
        return Err("replay compilation process identity differs from its role".to_owned());
    }
    let targets = report
        .compilation
        .targets
        .iter()
        .map(|target| {
            value::require_nonempty(&target.package, "compiled target package")?;
            value::require_nonempty(&target.kind, "compiled target kind")?;
            value::require_nonempty(&target.name, "compiled target name")?;
            Ok(target.clone())
        })
        .collect::<Result<BTreeSet<_>, String>>()?;
    if targets.len() != report.compilation.targets.len() {
        return Err("passed replay report repeats a compiled target".to_owned());
    }
    validate_fixtures(report, &targets)
}

fn validate_failed(report: &ReplayReport) -> Result<(), String> {
    value::require_nonempty(
        report
            .compilation
            .message
            .as_deref()
            .ok_or_else(|| "failed replay compilation has no message".to_owned())?,
        "failed replay compilation message",
    )?;
    if report.compilation.metadata_sha256.is_some()
        || !report.compilation.targets.is_empty()
        || !report.fixtures.is_empty()
    {
        return Err("failed replay compilation claims completed output".to_owned());
    }
    process::validate_all(&report.compilation.processes, false)
}

fn validate_fixtures(
    report: &ReplayReport,
    targets: &BTreeSet<super::super::model::TargetReport>,
) -> Result<(), String> {
    let mut identities = BTreeSet::new();
    let mut evidence_ids = BTreeSet::new();
    let fixture_duration_limit = successful_process_limit(report.contract.fixture_timeout_seconds)?;
    for fixture in &report.fixtures {
        if !targets.contains(&fixture.target) {
            return Err("replayed fixture refers to an uncompiled target".to_owned());
        }
        value::require_nonempty(&fixture.test_name, "fixture test name")?;
        if !identities.insert((fixture.target.clone(), fixture.test_name.clone())) {
            return Err("replay report repeats a fixture identity".to_owned());
        }
        validate_fixture_source(fixture)?;
        if fixture.evidence.is_empty() {
            return Err("replayed fixture has no evidence binding".to_owned());
        }
        for evidence in &fixture.evidence {
            value::require_nonempty(&evidence.invariant_id, "fixture invariant ID")?;
            value::require_nonempty(&evidence.evidence_id, "fixture evidence ID")?;
            if !evidence_ids.insert(evidence.evidence_id.clone()) {
                return Err("replay report repeats an evidence binding".to_owned());
            }
        }
        validate_fixture_outcome(fixture, fixture_duration_limit)?;
    }
    if evidence_ids.len() != report.inventory.evidence_bindings {
        return Err("replayed fixture evidence count differs from inventory".to_owned());
    }
    Ok(())
}

fn validate_fixture_source(fixture: &FixtureReport) -> Result<(), String> {
    let source = &fixture.source;
    value::require_nonempty(&source.fixture_symbol, "fixture symbol")?;
    value::require_relative_path(&source.fixture_path, "fixture")?;
    value::require_digest(&source.fixture_sha256, "fixture source")?;
    value::require_nonempty(&source.detector_symbol, "detector symbol")?;
    value::require_relative_path(&source.detector_path, "detector")?;
    value::require_digest(&source.detector_sha256, "detector source")?;
    value::require_digest(&source.source_graph_sha256, "target source graph")?;
    value::require_nonempty(&source.registered_identity, "registered detector identity")?;
    if source.expected_witnesses.is_empty()
        || !source
            .expected_witnesses
            .contains_key(&format!("expect-err:{}", source.registered_identity))
        || source.expected_witnesses.values().any(|count| *count == 0)
    {
        return Err("fixture source has no invocation-bound rejecting witness".to_owned());
    }
    Ok(())
}

fn validate_fixture_outcome(
    fixture: &FixtureReport,
    fixture_duration_limit: u64,
) -> Result<(), String> {
    if fixture.token.is_some() != fixture.challenge.is_some() {
        return Err("replayed fixture carries an incomplete transcript identity".to_owned());
    }
    if let Some(token) = &fixture.token {
        let challenge = fixture
            .challenge
            .as_deref()
            .ok_or_else(|| "replayed fixture token has no paired challenge".to_owned())?;
        let encoded = token
            .strip_prefix("replay-")
            .ok_or_else(|| "replayed fixture token is not canonical".to_owned())?;
        if encoded.len() != 32
            || encoded
                .bytes()
                .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
        {
            return Err("replayed fixture token is not canonical".to_owned());
        }
        crate::evidence::detector_proof::validate_challenge(challenge)?;
    }
    if let Some(process) = &fixture.process {
        let expected =
            super::super::process::fixture_execution_id(&fixture.target, &fixture.test_name);
        if process::role(process) != "detector-fixture"
            || process::execution_id(process) != expected
        {
            return Err("replayed fixture process identity does not match its fixture".to_owned());
        }
    }
    match fixture.status {
        crate::verification::detector_replay::result::FixtureReplayStatus::Passed => {
            if fixture.message.is_some() {
                return Err("passed fixture replay carries an error message".to_owned());
            }
            let process = fixture
                .process
                .as_ref()
                .ok_or_else(|| "passed fixture replay has no process report".to_owned())?;
            if fixture.token.is_none() {
                return Err("passed fixture replay has no transcript identity".to_owned());
            }
            process::validate(process, true)?;
            process::require_duration_at_most(process, fixture_duration_limit)
        }
        crate::verification::detector_replay::result::FixtureReplayStatus::HarnessError => {
            value::require_nonempty(
                fixture
                    .message
                    .as_deref()
                    .ok_or_else(|| "failed fixture replay has no message".to_owned())?,
                "failed fixture message",
            )?;
            if let Some(process) = &fixture.process {
                process::validate(process, false)?;
            }
            Ok(())
        }
    }
}

fn successful_process_limit(timeout_seconds: u64) -> Result<u64, String> {
    timeout_seconds
        .checked_mul(1_000)
        .and_then(|timeout| {
            timeout.checked_add(super::super::model::SUCCESSFUL_PROCESS_LIFECYCLE_ALLOWANCE_MS)
        })
        .ok_or_else(|| "replay process phase budget overflow".to_owned())
}
