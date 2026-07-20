//! Descriptor-bound commands shared by evidence production and verification.

use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path};

#[cfg(unix)]
use std::os::fd::BorrowedFd;

use crate::execution::filesystem::{ChildDirectory, HeldDirectory};

use super::{
    binding::{BoundExecutable, BoundInterpreter, BoundProcessRuntime},
    FinalizationPolicy, LauncherIdentity, PendingProcessOutput, ProcessDeadlines, ProcessRequest,
    ProcessRuntime, RuntimeExecutable, TerminationPolicy,
};

pub(crate) struct BoundCommand {
    requested_program: String,
    arguments: Vec<String>,
    current_dir_text: String,
    target: BoundExecutable,
    interpreter: Option<BoundInterpreter>,
    runtime: BoundProcessRuntime,
    launch_program: String,
    launch_arguments: Vec<OsString>,
    current_dir: HeldDirectory,
    child_current_dir: ChildDirectory,
}

impl BoundCommand {
    pub(crate) fn bind(
        program: &str,
        arguments: &[OsString],
        environment: &BTreeMap<String, String>,
        current_dir: &Path,
    ) -> Result<Self, Box<dyn Error>> {
        let arguments = utf8_arguments(arguments)?;
        let current_dir = HeldDirectory::open(current_dir)?;
        let current_dir_text = current_dir
            .external_path()
            .into_os_string()
            .into_string()
            .map_err(|_| "subprocess working directory is not UTF-8")?;
        let mut target = BoundExecutable::bind_program(program, environment)?;
        let interpreter = BoundInterpreter::bind_for_script(&mut target, environment)?;
        let runtime = BoundProcessRuntime::bind()?;
        let launch_program = launch_program(program, interpreter.as_ref())?;
        let launch_arguments = launch_arguments(&target, interpreter.as_ref(), &arguments);
        let child_current_dir = current_dir.bind_for_child()?;
        Ok(Self {
            requested_program: program.to_owned(),
            arguments,
            current_dir_text,
            target,
            interpreter,
            runtime,
            launch_program,
            launch_arguments,
            current_dir,
            child_current_dir,
        })
    }

    pub(crate) fn requested_program(&self) -> &str {
        &self.requested_program
    }

    pub(crate) fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub(crate) fn current_dir(&self) -> &str {
        &self.current_dir_text
    }

    pub(crate) fn target_identity(&self) -> super::ExecutableIdentity {
        self.target.identity()
    }

    pub(crate) fn launcher_identities(&self) -> Vec<LauncherIdentity> {
        self.runtime.launcher_identities(self.interpreter.as_ref())
    }

    pub(crate) fn verify_path_bindings(&self) -> Result<(), Box<dyn Error>> {
        self.target.verify_path_binding()?;
        if let Some(interpreter) = &self.interpreter {
            interpreter.executable().verify_path_binding()?;
        }
        self.runtime.verify_path_bindings()?;
        self.current_dir.verify_path_binding()?;
        Ok(())
    }

    pub(crate) fn run(
        &self,
        environment: &BTreeMap<String, String>,
        deadlines: ProcessDeadlines,
        termination: TerminationPolicy,
        finalization: FinalizationPolicy,
        inherited_descriptors: &[BorrowedFd<'_>],
    ) -> Result<PendingProcessOutput, Box<dyn Error>> {
        super::run(&ProcessRequest {
            program: &self.launch_program,
            executable_path: self.launch_executable().execution_path(),
            arguments: &self.launch_arguments,
            environment,
            deadlines,
            termination,
            finalization,
            runtime: ProcessRuntime {
                perl: runtime_executable(self.runtime.perl()),
                time: runtime_executable(self.runtime.time()),
                observer: runtime_executable(self.runtime.ps()),
            },
            #[cfg(unix)]
            executable_descriptor: self.launch_executable().descriptor(),
            #[cfg(unix)]
            target_descriptor: self.target.descriptor(),
            #[cfg(unix)]
            working_directory_descriptor: self.child_current_dir.descriptor(),
            #[cfg(unix)]
            inherited_descriptors,
        })
    }

    fn launch_executable(&self) -> &BoundExecutable {
        self.interpreter
            .as_ref()
            .map_or(&self.target, BoundInterpreter::executable)
    }
}

fn utf8_arguments(arguments: &[OsString]) -> Result<Vec<String>, &'static str> {
    arguments
        .iter()
        .map(|argument| {
            argument
                .to_str()
                .map(str::to_owned)
                .ok_or("subprocess argument is not UTF-8")
        })
        .collect()
}

fn launch_program(
    requested: &str,
    interpreter: Option<&BoundInterpreter>,
) -> Result<String, &'static str> {
    interpreter.map_or_else(
        || Ok(requested.to_owned()),
        |interpreter| {
            interpreter
                .executable()
                .logical_program()
                .to_str()
                .map(str::to_owned)
                .ok_or("script interpreter path is not UTF-8")
        },
    )
}

fn launch_arguments(
    target: &BoundExecutable,
    interpreter: Option<&BoundInterpreter>,
    arguments: &[String],
) -> Vec<OsString> {
    let mut launch = Vec::new();
    if let Some(interpreter) = interpreter {
        launch.extend(interpreter.arguments().iter().map(OsString::from));
        launch.push(target.execution_path().as_os_str().to_owned());
    }
    launch.extend(arguments.iter().map(OsString::from));
    launch
}

fn runtime_executable(executable: &BoundExecutable) -> RuntimeExecutable<'_> {
    RuntimeExecutable {
        path: executable.execution_path(),
        #[cfg(unix)]
        descriptor: executable.descriptor(),
    }
}
