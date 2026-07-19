//! Invocation binding and process evidence format adapters.

use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path};

use super::{
    duration_ms,
    runtime::{BoundExecutable, BoundInterpreter, BoundProcessRuntime},
    ProcessOutput,
};

#[cfg(unix)]
use std::os::fd::BorrowedFd;

use crate::evidence::format::process::{
    encode_combined_v4, encode_detector_v5, encode_maelstrom_v3, encode_tla_v4, ProcessFormatError,
    ProcessObservation,
};
use crate::evidence::InvocationReceipt;
use crate::execution::filesystem::{ChildDirectory, HeldDirectory};

pub(super) struct BoundInvocation {
    receipt: InvocationReceipt,
    target: BoundExecutable,
    interpreter: Option<BoundInterpreter>,
    runtime: BoundProcessRuntime,
    launch_program: String,
    launch_arguments: Vec<OsString>,
    current_dir: HeldDirectory,
    child_current_dir: ChildDirectory,
}

impl BoundInvocation {
    pub(super) fn receipt(&self) -> &InvocationReceipt {
        &self.receipt
    }

    #[cfg(test)]
    pub(super) fn into_receipt(self) -> InvocationReceipt {
        self.receipt
    }

    pub(super) fn executable_path(&self) -> &Path {
        self.launch_executable().execution_path()
    }

    pub(super) fn logical_program(&self) -> &str {
        &self.launch_program
    }

    pub(super) fn launch_arguments(&self) -> &[OsString] {
        &self.launch_arguments
    }

    pub(super) fn runtime(&self) -> &BoundProcessRuntime {
        &self.runtime
    }

    #[cfg(unix)]
    pub(super) fn executable_descriptor(&self) -> BorrowedFd<'_> {
        self.launch_executable().descriptor()
    }

    #[cfg(unix)]
    pub(super) fn target_descriptor(&self) -> BorrowedFd<'_> {
        self.target.descriptor()
    }

    #[cfg(unix)]
    pub(super) fn current_dir_descriptor(&self) -> BorrowedFd<'_> {
        self.child_current_dir.descriptor()
    }

    pub(super) fn verify_path_bindings(&self) -> Result<(), Box<dyn Error>> {
        self.target.verify_path_binding()?;
        if let Some(interpreter) = &self.interpreter {
            interpreter.executable().verify_path_binding()?;
        }
        self.runtime.verify_path_bindings()?;
        self.current_dir.verify_path_binding()?;
        Ok(())
    }

    fn launch_executable(&self) -> &BoundExecutable {
        self.interpreter
            .as_ref()
            .map_or(&self.target, BoundInterpreter::executable)
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
    let arguments = arguments
        .iter()
        .map(|argument| {
            argument
                .to_str()
                .map(str::to_owned)
                .ok_or("subprocess argument is not UTF-8")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let current_dir = HeldDirectory::open(current_dir)?;
    let current_dir_receipt = current_dir
        .external_path()
        .into_os_string()
        .into_string()
        .map_err(|_| "subprocess working directory is not UTF-8")?;
    let mut target = BoundExecutable::bind_program(program, environment)?;
    let interpreter = BoundInterpreter::bind_for_script(&mut target, environment)?;
    let runtime = BoundProcessRuntime::bind()?;
    let launch_program = interpreter.as_ref().map_or_else(
        || Ok(program.to_owned()),
        |interpreter| {
            interpreter
                .executable()
                .logical_program()
                .to_str()
                .map(str::to_owned)
                .ok_or("script interpreter path is not UTF-8")
        },
    )?;
    let mut launch_arguments = Vec::new();
    if let Some(interpreter) = &interpreter {
        launch_arguments.extend(interpreter.arguments().iter().map(OsString::from));
        launch_arguments.push(target.execution_path().as_os_str().to_owned());
    }
    launch_arguments.extend(arguments.iter().map(OsString::from));
    let receipt = InvocationReceipt {
        program: program.to_owned(),
        program_sha256: target.receipt().sha256,
        arguments,
        current_dir: current_dir_receipt,
        environment: environment.clone(),
        environment_sha256: crate::provenance::invocation::digest_environment(environment)?,
        launchers: runtime.launcher_receipts(interpreter.as_ref()),
    };
    let child_current_dir = current_dir.bind_for_child()?;
    Ok(BoundInvocation {
        receipt,
        target,
        interpreter,
        runtime,
        launch_program,
        launch_arguments,
        current_dir,
        child_current_dir,
    })
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
