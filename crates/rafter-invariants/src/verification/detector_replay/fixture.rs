//! Exact authenticated execution of detector-level negative fixtures.

use std::{
    collections::BTreeMap,
    error::Error,
    fs::File,
    io::Read,
    os::fd::AsRawFd,
    time::{Duration, Instant},
};

use crate::{
    contract::profile::DetectorReplayContract,
    evidence::{detector_proof::PROOF_DESCRIPTOR_ENV, format::libtest::ORACLE_TOKEN_ENV},
    execution::{
        detector_proof::{ChallengeExchange, ChallengeGate},
        filesystem::ChildDirectory,
    },
    verification::{qualify_detector_execution, source::AuthenticatedCompilationSource},
};

use super::{
    execution::CompiledReplay,
    process::{self, ReplayCommand, ReplayProcessBudget, ReplayProcessOutput},
    result::{FixtureReplayResult, FixtureReplayStatus},
    DetectorReplayPlan, ReplayFixture, ReplayTarget,
};

const HEX: &[u8; 16] = b"0123456789abcdef";

pub(super) fn execute(
    replay: &DetectorReplayPlan,
    compiled: &CompiledReplay,
    source: &AuthenticatedCompilationSource<'_>,
    contract: &DetectorReplayContract,
    total_deadline: Instant,
) -> Vec<FixtureReplayResult> {
    // The compiled replay executes only descriptor-bound binaries and fixture scratch paths.
    // Authenticate the immutable source/archive inventory at both phase boundaries instead of
    // re-hashing the complete Cargo registry before and after every fixture.
    if let Err(error) = source.revalidate() {
        return harness_errors(
            replay,
            &format!("detector replay source authentication failed before execution: {error}"),
        );
    }
    let mut results = Vec::with_capacity(replay.fixture_count());
    for (target, fixtures) in replay.targets() {
        for fixture in fixtures {
            let result = if Instant::now() >= total_deadline {
                harness_error(
                    target,
                    fixture,
                    "detector replay exhausted its total budget",
                )
            } else {
                execute_one(target, fixture, compiled, source, contract, total_deadline)
            };
            results.push(result);
        }
    }
    match source.revalidate() {
        Ok(()) => results,
        Err(error) => harness_errors(
            replay,
            &format!("detector replay source authentication failed after execution: {error}"),
        ),
    }
}

fn harness_errors(replay: &DetectorReplayPlan, message: &str) -> Vec<FixtureReplayResult> {
    replay
        .targets()
        .iter()
        .flat_map(|(target, fixtures)| {
            fixtures
                .iter()
                .map(move |fixture| harness_error(target, fixture, message))
        })
        .collect()
}

fn execute_one(
    target: &ReplayTarget,
    fixture: &ReplayFixture,
    compiled: &CompiledReplay,
    source: &AuthenticatedCompilationSource<'_>,
    contract: &DetectorReplayContract,
    total_deadline: Instant,
) -> FixtureReplayResult {
    let Some(binary) = compiled.targets.get(target) else {
        return harness_error(target, fixture, "compiled replay target is missing");
    };
    let attempt = (|| -> Result<FixtureAttempt, Box<dyn Error>> {
        compiled.workspace.verify()?;
        binary.revalidate()?;
        let temporary = compiled.workspace.create_fixture_temporary()?;
        let temporary_binding = temporary.bind_for_child()?;
        let gate = ChallengeGate::open()?;
        let token = random_token()?;
        let challenge = gate.challenge().encoded();
        let process = (|| -> Result<ReplayProcessOutput, Box<dyn Error>> {
            let environment = environment(&temporary_binding, &gate, &token)?;
            let arguments = [
                fixture.identity.test_name.clone().into(),
                "--exact".into(),
                "--show-output".into(),
                "--color".into(),
                "never".into(),
                "--test-threads=1".into(),
            ];
            let command = ReplayCommand::bind(
                binary
                    .executable()
                    .to_str()
                    .ok_or("compiled replay executable path is not UTF-8")?,
                &arguments,
                &environment,
                source.workspace(),
            )?;
            let timeout = Duration::from_secs(contract.fixture_timeout_seconds).min(
                total_deadline
                    .checked_duration_since(Instant::now())
                    .ok_or("detector replay total deadline expired")?,
            );
            if timeout.is_zero() {
                return Err("detector replay total deadline expired".into());
            }
            process::run(
                &command,
                &environment,
                ReplayProcessBudget::new(timeout, total_deadline),
                &[gate.child_descriptor(), temporary_binding.descriptor()],
            )
        })();
        let exchange = gate.finish();
        let output = process?;
        let qualification = (|| -> Result<(), Box<dyn Error>> {
            compiled.workspace.verify()?;
            temporary.verify()?;
            binary.revalidate()?;
            qualify(&output, fixture, &token, &challenge, &exchange)?;
            Ok(())
        })();
        Ok(FixtureAttempt {
            output,
            token,
            challenge,
            error: qualification.err().map(|error| error.to_string()),
        })
    })();
    match attempt {
        Ok(attempt) => attempt.into_result(target, fixture),
        Err(error) => {
            let diagnostics = process::retained_diagnostics(error.as_ref());
            harness_error_with_diagnostics(target, fixture, &error.to_string(), diagnostics)
        }
    }
}

struct FixtureAttempt {
    output: ReplayProcessOutput,
    token: String,
    challenge: String,
    error: Option<String>,
}

impl FixtureAttempt {
    fn into_result(self, target: &ReplayTarget, fixture: &ReplayFixture) -> FixtureReplayResult {
        FixtureReplayResult {
            target: target.clone(),
            test_name: fixture.identity.test_name.clone(),
            evidence: fixture.evidence.clone(),
            status: if self.error.is_some() {
                FixtureReplayStatus::HarnessError
            } else {
                FixtureReplayStatus::Passed
            },
            token: Some(self.token),
            challenge: Some(self.challenge),
            message: self.error,
            output: Some(self.output),
            retained_diagnostics: None,
        }
    }
}

fn qualify(
    output: &ReplayProcessOutput,
    fixture: &ReplayFixture,
    token: &str,
    challenge: &str,
    exchange: &ChallengeExchange,
) -> Result<(), Box<dyn Error>> {
    if output.timed_out {
        return Err("detector fixture timed out".into());
    }
    if !output.status.success() {
        return Err(format!("detector fixture exited with status {}", output.status).into());
    }
    if exchange != &ChallengeExchange::Completed {
        return Err(format!("detector challenge exchange did not complete: {exchange:?}").into());
    }
    qualify_detector_execution(
        &output.stdout,
        &output.stderr,
        &fixture.identity.test_name,
        token,
        challenge,
        &fixture.expected_witnesses,
    )?;
    Ok(())
}

fn environment(
    temporary: &ChildDirectory,
    gate: &ChallengeGate,
    token: &str,
) -> Result<BTreeMap<String, String>, Box<dyn Error>> {
    let mut environment = process::environment();
    environment.remove("CARGO_HOME");
    environment.remove("HOME");
    environment.extend([
        ("RUST_BACKTRACE".to_owned(), "1".to_owned()),
        (
            "TMPDIR".to_owned(),
            temporary
                .path()
                .to_str()
                .ok_or("detector replay temporary path is not UTF-8")?
                .to_owned(),
        ),
        (ORACLE_TOKEN_ENV.to_owned(), token.to_owned()),
        (
            PROOF_DESCRIPTOR_ENV.to_owned(),
            gate.child_descriptor().as_raw_fd().to_string(),
        ),
    ]);
    Ok(environment)
}

fn random_token() -> Result<String, Box<dyn Error>> {
    let mut bytes = [0_u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let mut token = String::from("replay-");
    for byte in bytes {
        token.push(char::from(HEX[usize::from(byte >> 4)]));
        token.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(token)
}

fn harness_error(
    target: &ReplayTarget,
    fixture: &ReplayFixture,
    message: &str,
) -> FixtureReplayResult {
    harness_error_with_diagnostics(target, fixture, message, None)
}

fn harness_error_with_diagnostics(
    target: &ReplayTarget,
    fixture: &ReplayFixture,
    message: &str,
    retained_diagnostics: Option<process::RetainedProcessDiagnostics>,
) -> FixtureReplayResult {
    FixtureReplayResult {
        target: target.clone(),
        test_name: fixture.identity.test_name.clone(),
        evidence: fixture.evidence.clone(),
        status: FixtureReplayStatus::HarnessError,
        token: None,
        challenge: None,
        message: Some(message.to_owned()),
        output: None,
        retained_diagnostics,
    }
}
