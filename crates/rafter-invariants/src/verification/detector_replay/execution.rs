//! Authenticated metadata and fresh compilation under one bounded capability set.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

mod build_plan;
mod command;
mod failure;

use crate::{
    contract::profile::DetectorReplayContract, execution::filesystem::ChildDirectory,
    verification::source::AuthenticatedCompilationSource,
};

use super::{
    compiler::{self, CompiledReplayTarget},
    metadata::CompilationGraph,
    process::{ReplayProcessBudget, ReplayProcessOutput},
    toolchain::ReplayToolchain,
    workspace::{ReplayBindings, ReplayWorkspace},
    DetectorReplayPlan, ReplayTarget,
};
use build_plan::ReplayBuildPlan;
use command::{
    child_path, environment, remaining, require_success, source_replacement, strings,
    CargoExecution,
};
pub(super) use failure::CompilationFailure;

pub(super) struct CompiledReplay {
    pub(super) workspace: ReplayWorkspace,
    pub(super) targets: BTreeMap<ReplayTarget, CompiledReplayTarget>,
    pub(super) metadata_output: ReplayProcessOutput,
    pub(super) compiler_output: ReplayProcessOutput,
    pub(super) metadata_sha256: String,
}

struct PreparedCompilation {
    compile_deadline: Instant,
    workspace: ReplayWorkspace,
    bindings: ReplayBindings,
    vendor: ChildDirectory,
    environment: BTreeMap<String, String>,
    config: [String; 2],
    build: ReplayBuildPlan,
    toolchain: ReplayToolchain,
}

pub(super) fn compile(
    replay: &DetectorReplayPlan,
    source: &AuthenticatedCompilationSource<'_>,
    contract: &DetectorReplayContract,
    profile: &str,
    source_ref: &str,
    total_deadline: Instant,
) -> Result<CompiledReplay, Box<CompilationFailure>> {
    let build = ReplayBuildPlan::derive(replay)
        .map_err(CompilationFailure::setup)
        .map_err(Box::new)?;
    let prepared =
        PreparedCompilation::new(source, contract, profile, source_ref, total_deadline, build)?;
    let metadata_output = prepared
        .run_metadata(source)
        .map_err(|error| CompilationFailure::setup_error(error.as_ref()))
        .map_err(Box::new)?;
    if let Err(error) = require_success("Cargo metadata", &metadata_output) {
        return Err(Box::new(CompilationFailure::after_metadata(
            error,
            metadata_output,
        )));
    }
    let graph = match CompilationGraph::parse(&metadata_output.stdout, source) {
        Ok(graph) => graph,
        Err(error) => {
            return Err(Box::new(CompilationFailure::after_metadata(
                error,
                metadata_output,
            )));
        }
    };
    let compiler_output = match prepared.run_compiler(source) {
        Ok(output) => output,
        Err(error) => {
            return Err(Box::new(CompilationFailure::after_metadata_error(
                error.as_ref(),
                metadata_output,
            )));
        }
    };
    if let Err(error) = require_success("Cargo compilation", &compiler_output) {
        return Err(Box::new(CompilationFailure::after_compiler(
            error,
            metadata_output,
            compiler_output,
        )));
    }
    let targets = match bind_and_revalidate(
        replay,
        source,
        &graph,
        &prepared.workspace,
        &compiler_output,
    ) {
        Ok(targets) => targets,
        Err(error) => {
            return Err(Box::new(CompilationFailure::after_compiler(
                error,
                metadata_output,
                compiler_output,
            )));
        }
    };
    Ok(CompiledReplay {
        workspace: prepared.workspace,
        targets,
        metadata_output,
        compiler_output,
        metadata_sha256: graph.sha256,
    })
}

impl PreparedCompilation {
    fn new(
        source: &AuthenticatedCompilationSource<'_>,
        contract: &DetectorReplayContract,
        profile: &str,
        source_ref: &str,
        total_deadline: Instant,
        build: ReplayBuildPlan,
    ) -> Result<Self, Box<CompilationFailure>> {
        let compile_deadline = Instant::now()
            .checked_add(Duration::from_secs(contract.compile_timeout_seconds))
            .ok_or("detector replay compile deadline overflow")
            .map_err(|error| Box::new(CompilationFailure::setup(error)))?
            .min(total_deadline);
        let workspace = ReplayWorkspace::create(profile, source_ref, total_deadline)
            .map_err(|error| Box::new(CompilationFailure::setup(error)))?;
        let bindings = workspace
            .bind_for_child()
            .map_err(|error| Box::new(CompilationFailure::setup(error)))?;
        let vendor = source
            .bind_vendor_for_child()
            .map_err(|error| Box::new(CompilationFailure::setup(error)))?;
        let toolchain = ReplayToolchain::bind(source)
            .map_err(|error| Box::new(CompilationFailure::setup(error)))?;
        let environment = environment(&bindings, &toolchain)
            .map_err(|error| Box::new(CompilationFailure::setup(error)))?;
        let config = source_replacement(&vendor)
            .map_err(|error| Box::new(CompilationFailure::setup(error)))?;
        Ok(Self {
            compile_deadline,
            workspace,
            bindings,
            vendor,
            environment,
            config,
            build,
            toolchain,
        })
    }

    fn run_metadata(
        &self,
        source: &AuthenticatedCompilationSource<'_>,
    ) -> Result<ReplayProcessOutput, Box<dyn std::error::Error>> {
        let arguments = strings([
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--offline",
            "--manifest-path",
            "Cargo.toml",
            "--config",
            &self.config[0],
            "--config",
            &self.config[1],
        ]);
        self.cargo_execution(source).run(
            &arguments,
            ReplayProcessBudget::new(
                remaining(self.compile_deadline, "Cargo metadata")?,
                self.compile_deadline,
            ),
        )
    }

    fn cargo_execution<'run, 'source>(
        &'run self,
        source: &'run AuthenticatedCompilationSource<'source>,
    ) -> CargoExecution<'run, 'source> {
        CargoExecution {
            environment: &self.environment,
            source,
            workspace: &self.workspace,
            bindings: &self.bindings,
            vendor: &self.vendor,
            toolchain: &self.toolchain,
        }
    }

    fn run_compiler(
        &self,
        source: &AuthenticatedCompilationSource<'_>,
    ) -> Result<ReplayProcessOutput, Box<dyn std::error::Error>> {
        let target_path = child_path(&self.bindings.target)?;
        let mut arguments = strings(["test", "--locked", "--offline", "--no-default-features"]);
        arguments.extend(self.build.cargo_arguments());
        arguments.extend(strings([
            "--no-run",
            "--target-dir",
            target_path,
            "--message-format=json-render-diagnostics",
            "--config",
            &self.config[0],
            "--config",
            &self.config[1],
        ]));
        self.cargo_execution(source).run(
            &arguments,
            ReplayProcessBudget::new(
                remaining(self.compile_deadline, "Cargo compilation")?,
                self.compile_deadline,
            ),
        )
    }
}

fn bind_and_revalidate(
    replay: &DetectorReplayPlan,
    source: &AuthenticatedCompilationSource<'_>,
    graph: &CompilationGraph,
    workspace: &ReplayWorkspace,
    compiler_output: &ReplayProcessOutput,
) -> Result<BTreeMap<ReplayTarget, CompiledReplayTarget>, String> {
    let targets = compiler::bind_fresh_executables(
        &compiler_output.stdout,
        graph,
        source.workspace(),
        workspace.target(),
        replay.targets().keys().cloned(),
    )?;
    source.revalidate()?;
    workspace.verify().map_err(|error| error.to_string())?;
    for target in targets.values() {
        target.revalidate()?;
    }
    Ok(targets)
}
