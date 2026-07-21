//! Producer-side detector challenge transport and process adaptation.

use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path};

#[cfg(all(test, unix))]
use command_fds::{CommandFdExt, FdMapping};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(all(test, unix))]
use std::time::Instant;

use super::super::process;
#[cfg(unix)]
use crate::{
    evidence::detector_proof as proof_wire,
    execution::detector_proof::{ChallengeExchange, ChallengeGate},
};

pub(super) struct Execution {
    pub output: process::ProcessOutput,
    pub challenge: String,
    pub channel_error: Option<String>,
}

#[cfg(unix)]
pub(super) fn execute(
    program: &str,
    arguments: &[OsString],
    environment: &mut BTreeMap<String, String>,
) -> Result<Execution, Box<dyn Error>> {
    validate_protocol_contract()?;
    let gate = ChallengeGate::open()?;
    let descriptor = gate.child_descriptor();
    environment.insert(
        proof_wire::PROOF_DESCRIPTOR_ENV.to_owned(),
        descriptor.as_raw_fd().to_string(),
    );
    let challenge = gate.challenge().encoded();
    let output = process::timed_for_with_cap_and_descriptors(
        process::ProcessKind::TestExecution,
        program,
        arguments,
        environment,
        Path::new("."),
        None,
        &[descriptor],
    );
    complete_execution(output, gate.finish(), challenge)
}

#[cfg(not(unix))]
pub(super) fn execute(
    _program: &str,
    _arguments: &[OsString],
    _environment: &mut BTreeMap<String, String>,
) -> Result<Execution, Box<dyn Error>> {
    Err("detector proof requires Unix domain sockets".into())
}

#[cfg(all(test, unix))]
pub(super) fn execute_for_test(
    program: &str,
    arguments: &[OsString],
    environment: &mut BTreeMap<String, String>,
) -> Result<Execution, Box<dyn Error>> {
    validate_protocol_contract()?;
    let gate = ChallengeGate::open()?;
    let descriptor = gate.child_descriptor();
    environment.insert(
        proof_wire::PROOF_DESCRIPTOR_ENV.to_owned(),
        descriptor.as_raw_fd().to_string(),
    );
    environment.insert(
        "RAFTER_INVARIANT_TEST_DISCLOSED_PROOF_FD".to_owned(),
        descriptor.as_raw_fd().to_string(),
    );
    let challenge = gate.challenge().encoded();
    let invocation = process::expected_invocation(program, arguments, environment, Path::new("."))?;
    let started = Instant::now();
    let mut command = std::process::Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .envs(&*environment)
        .current_dir(".");
    command.fd_mappings(vec![FdMapping {
        parent_fd: descriptor.try_clone_to_owned()?,
        child_fd: descriptor.as_raw_fd(),
    }])?;
    let captured = command.output()?;
    let output = process::ProcessOutput {
        invocation,
        status: captured.status,
        stdout: captured.stdout,
        stderr: captured.stderr,
        duration: started.elapsed(),
        peak_rss_kib: 1,
        timed_out: false,
        termination: None,
    };
    complete_execution(Ok(output), gate.finish(), challenge)
}

#[cfg(all(test, not(unix)))]
pub(super) fn execute_for_test(
    _program: &str,
    _arguments: &[OsString],
    _environment: &mut BTreeMap<String, String>,
) -> Result<Execution, Box<dyn Error>> {
    Err("detector proof requires Unix domain sockets".into())
}

#[cfg(unix)]
fn complete_execution(
    output: Result<process::ProcessOutput, Box<dyn Error>>,
    exchange: ChallengeExchange,
    challenge: String,
) -> Result<Execution, Box<dyn Error>> {
    let channel_error = exchange_error(exchange);
    match output {
        Ok(output) => Ok(Execution {
            output,
            challenge,
            channel_error,
        }),
        Err(process_error) => match channel_error {
            None => Err(process_error),
            Some(channel_error) => Err(format!(
                "{process_error}; detector proof channel also failed: {channel_error}"
            )
            .into()),
        },
    }
}

#[cfg(unix)]
fn exchange_error(exchange: ChallengeExchange) -> Option<String> {
    match exchange {
        ChallengeExchange::Completed | ChallengeExchange::Disconnected => None,
        ChallengeExchange::MalformedRequest => {
            Some("detector proof request is malformed".to_owned())
        }
        ChallengeExchange::TransportError(error) => Some(error.to_string()),
    }
}

#[cfg(unix)]
fn validate_protocol_contract() -> Result<(), Box<dyn Error>> {
    let protocol = ChallengeGate::protocol();
    let evidence_challenge = [0; proof_wire::CHALLENGE_BYTES];
    let compatible = protocol.descriptor_environment == proof_wire::PROOF_DESCRIPTOR_ENV
        && protocol.challenge_bytes == proof_wire::CHALLENGE_BYTES
        && protocol.proof_request == proof_wire::PROOF_REQUEST
        && protocol.zero_challenge_encoding == proof_wire::encode_challenge(&evidence_challenge);
    if compatible {
        Ok(())
    } else {
        Err("detector challenge transport and evidence wire contracts disagree".into())
    }
}

#[cfg(all(test, unix))]
#[path = "detector_proof_tests.rs"]
mod tests;
