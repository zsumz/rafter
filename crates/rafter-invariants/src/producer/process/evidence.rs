//! Invocation binding and process evidence format adapters.

use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path};

use super::{duration_ms, ProcessOutput};

#[cfg(unix)]
use std::os::fd::BorrowedFd;

use crate::evidence::format::process::{
    encode_combined_v4, encode_detector_v5, encode_maelstrom_v3, encode_tla_v4, ProcessFormatError,
    ProcessObservation,
};
use crate::{
    evidence::{ExecutableReceipt, InvocationReceipt, LauncherReceipt},
    execution::process::{
        BoundCommand, FinalizationPolicy, PendingProcessOutput, ProcessDeadlines, TerminationPolicy,
    },
};

pub(super) struct BoundInvocation {
    receipt: InvocationReceipt,
    command: BoundCommand,
}

impl BoundInvocation {
    pub(super) fn receipt(&self) -> &InvocationReceipt {
        &self.receipt
    }

    #[cfg(test)]
    pub(super) fn into_receipt(self) -> InvocationReceipt {
        self.receipt
    }

    pub(super) fn verify_path_bindings(&self) -> Result<(), Box<dyn Error>> {
        self.command.verify_path_bindings()
    }

    pub(super) fn run(
        &self,
        environment: &BTreeMap<String, String>,
        deadlines: ProcessDeadlines,
        termination: TerminationPolicy,
        finalization: FinalizationPolicy,
        inherited_descriptors: &[BorrowedFd<'_>],
    ) -> Result<PendingProcessOutput, Box<dyn Error>> {
        self.command.run(
            environment,
            deadlines,
            termination,
            finalization,
            inherited_descriptors,
        )
    }
}

#[cfg(test)]
pub(crate) fn expected_invocation(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
) -> Result<InvocationReceipt, Box<dyn Error>> {
    Ok(bind_invocation(program, arguments, environment, current_dir)?.into_receipt())
}

pub(super) fn bind_invocation(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
) -> Result<BoundInvocation, Box<dyn Error>> {
    let command = BoundCommand::bind(program, arguments, environment, current_dir)?;
    let target = command.target_identity();
    let receipt = InvocationReceipt {
        program: command.requested_program().to_owned(),
        program_sha256: target.sha256,
        arguments: command.arguments().to_vec(),
        current_dir: command.current_dir().to_owned(),
        environment: environment.clone(),
        environment_sha256: crate::provenance::invocation::digest_environment(environment)?,
        launchers: command
            .launcher_identities()
            .into_iter()
            .map(|launcher| LauncherReceipt {
                role: launcher.role,
                runtime: launcher.runtime,
                executable: ExecutableReceipt {
                    program: launcher.executable.path.to_string_lossy().into_owned(),
                    sha256: launcher.executable.sha256,
                },
            })
            .collect(),
    };
    Ok(BoundInvocation { receipt, command })
}

pub(in crate::producer) fn combined_log(
    label: &str,
    output: &ProcessOutput,
) -> Result<Vec<u8>, ProcessFormatError> {
    encode_combined_v4(label, observation_without_termination(output))
}

pub(in crate::producer) fn combined_detector_log(
    label: &str,
    output: &ProcessOutput,
    detector_challenge: &str,
) -> Result<Vec<u8>, ProcessFormatError> {
    encode_detector_v5(
        label,
        observation_without_termination(output),
        detector_challenge,
    )
}

pub(in crate::producer) fn json_log(
    label: &str,
    output: &ProcessOutput,
) -> Result<Vec<u8>, ProcessFormatError> {
    encode_maelstrom_v3(label, observation_without_termination(output))
}

pub(in crate::producer) fn tla_json_log(
    label: &str,
    output: &ProcessOutput,
) -> Result<Vec<u8>, ProcessFormatError> {
    encode_tla_v4(label, observation(output))
}

fn observation(output: &ProcessOutput) -> ProcessObservation<'_> {
    ProcessObservation {
        invocation: &output.invocation,
        exit_code: output.status.code(),
        timed_out: output.timed_out,
        termination: output.termination.as_ref(),
        duration_ms: duration_ms(output.duration),
        peak_rss_kib: output.peak_rss_kib,
        stdout: &output.stdout,
        stderr: &output.stderr,
    }
}

fn observation_without_termination(output: &ProcessOutput) -> ProcessObservation<'_> {
    ProcessObservation {
        termination: None,
        ..observation(output)
    }
}
