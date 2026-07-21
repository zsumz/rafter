//! Held-directory TLC command construction and bounded process execution.

use std::{error::Error, ffi::OsString, fs, path::Path, time::Duration};

#[cfg(target_os = "linux")]
use std::os::fd::BorrowedFd;

use crate::execution::filesystem::{HeldDirectory, OperationDeadline, TREE_LIMITS};

use super::{
    super::{artifact, process},
    model::TlcRun,
};

const JAR: &str = "tools/cache/tla2tools.jar";

#[derive(Clone, Copy)]
pub(super) enum TlcState<'a> {
    Ephemeral,
    Checkpoint {
        state_dir: &'a HeldDirectory,
        recover_from: Option<&'a HeldDirectory>,
        checkpoint_minutes: &'a str,
    },
}

#[derive(Clone, Copy)]
pub(super) struct TlcRequest<'a> {
    pub(super) profile: &'a str,
    pub(super) source_ref: &'a str,
    pub(super) config: &'a str,
    pub(super) module: &'a str,
    pub(super) workers: &'a str,
    pub(super) seed: &'a str,
    pub(super) timeout: Duration,
    pub(super) output_dir: &'a Path,
    pub(super) label: &'a str,
    pub(super) artifact_kind: &'a str,
    pub(super) max_heap: Option<&'a str>,
    pub(super) fp_mem: Option<&'a str>,
    pub(super) state: TlcState<'a>,
}

pub(super) fn run_tlc(request: TlcRequest<'_>) -> Result<TlcRun, Box<dyn Error>> {
    #[cfg(not(target_os = "linux"))]
    require_sound_tlc_state_binding()?;
    let source_prefix = request.source_ref.get(..12).unwrap_or(request.source_ref);
    process::ensure_execution_deadline(
        request.profile,
        "tla",
        &format!("{} TLC state preparation", request.label),
    )?;
    let ephemeral_state = prepare_ephemeral_state(request, source_prefix)?;
    let (state_handle, recover_handle) = state_handles(request.state, ephemeral_state.as_ref())?;
    process::ensure_execution_deadline(
        request.profile,
        "tla",
        &format!("{} TLC process launch", request.label),
    )?;
    verify_tlc_state_binding(state_handle, recover_handle)?;
    let state_binding = state_handle.bind_for_child()?;
    let recover_binding = recover_handle
        .map(HeldDirectory::bind_for_child)
        .transpose()?;
    let arguments = tlc_arguments(
        request,
        state_binding.path(),
        recover_binding
            .as_ref()
            .map(crate::execution::filesystem::ChildDirectory::path),
    )?;
    verify_tlc_state_binding(state_handle, recover_handle)?;
    let environment = process::base_environment();
    #[cfg(target_os = "linux")]
    let descriptors = tlc_directory_descriptors(&state_binding, recover_binding.as_ref());
    #[cfg(target_os = "linux")]
    let output = process::timed_for_with_cap_and_descriptors(
        process::ProcessKind::TlaExecution,
        "java",
        &arguments,
        &environment,
        Path::new("specs/tla/raft"),
        Some(request.timeout),
        &descriptors,
    )?;
    #[cfg(not(target_os = "linux"))]
    let output = process::timed_for_with_cap(
        process::ProcessKind::TlaExecution,
        "java",
        &arguments,
        &environment,
        Path::new("specs/tla/raft"),
        Some(request.timeout),
    )?;
    verify_tlc_state_binding(state_handle, recover_handle)?;
    let artifact = artifact::write(
        request.output_dir,
        Path::new(&format!(
            "{}-tla/{source_prefix}/{}.log",
            request.profile, request.label
        )),
        request.artifact_kind,
        &process::tla_json_log(request.label, &output)?,
    )?;
    Ok(TlcRun { output, artifact })
}

#[cfg(not(target_os = "linux"))]
fn require_sound_tlc_state_binding() -> Result<(), Box<dyn Error>> {
    Err("TLC execution requires Linux descriptor-relative state directories; this host cannot soundly expose a held directory tree to Java".into())
}

fn prepare_ephemeral_state(
    request: TlcRequest<'_>,
    source_prefix: &str,
) -> Result<Option<HeldDirectory>, Box<dyn Error>> {
    if !matches!(request.state, TlcState::Ephemeral) {
        return Ok(None);
    }
    let state_dir = Path::new("target/rafter-invariants/tla")
        .join(source_prefix)
        .join(request.profile)
        .join(request.label);
    let (execution_deadline, _) = process::active_layer_deadlines(request.profile, "tla")?;
    Ok(Some(HeldDirectory::replace_tree(
        &state_dir,
        TREE_LIMITS,
        OperationDeadline::at(execution_deadline, "stale TLC state cleanup"),
    )?))
}

fn state_handles<'a>(
    state: TlcState<'a>,
    ephemeral_state: Option<&'a HeldDirectory>,
) -> Result<(&'a HeldDirectory, Option<&'a HeldDirectory>), Box<dyn Error>> {
    match state {
        TlcState::Ephemeral => Ok((
            ephemeral_state.ok_or("ephemeral TLC state handle was not initialized")?,
            None,
        )),
        TlcState::Checkpoint {
            state_dir,
            recover_from,
            ..
        } => Ok((state_dir, recover_from)),
    }
}

fn tlc_arguments(
    request: TlcRequest<'_>,
    state_dir: &Path,
    recover_from: Option<&Path>,
) -> Result<Vec<OsString>, Box<dyn Error>> {
    let jar = fs::canonicalize(JAR)?;
    let mut arguments = Vec::new();
    if let Some(max_heap) = request.max_heap {
        arguments.push(format!("-Xmx{max_heap}").into());
    }
    arguments.extend([
        "-XX:+UseParallelGC".into(),
        "-cp".into(),
        jar.into_os_string(),
        "tlc2.TLC".into(),
        "-tool".into(),
        "-workers".into(),
        request.workers.into(),
        "-seed".into(),
        request.seed.into(),
        "-fp".into(),
        "0".into(),
    ]);
    if let Some(fp_mem) = request.fp_mem {
        arguments.extend(["-fpmem".into(), fp_mem.into()]);
    }
    arguments.extend(["-metadir".into(), state_dir.as_os_str().to_os_string()]);
    if let TlcState::Checkpoint {
        checkpoint_minutes, ..
    } = request.state
    {
        arguments.extend([
            "-checkpoint".into(),
            checkpoint_minutes.into(),
            "-gzip".into(),
        ]);
        if let Some(recover_from) = recover_from {
            arguments.extend(["-recover".into(), recover_from.as_os_str().to_os_string()]);
        }
    }
    arguments.extend([
        "-config".into(),
        request.config.into(),
        request.module.into(),
    ]);
    Ok(arguments)
}

#[cfg(target_os = "linux")]
fn tlc_directory_descriptors<'a>(
    state: &'a crate::execution::filesystem::ChildDirectory,
    recover: Option<&'a crate::execution::filesystem::ChildDirectory>,
) -> Vec<BorrowedFd<'a>> {
    let mut descriptors = vec![state.descriptor()];
    if let Some(recover) = recover {
        descriptors.push(recover.descriptor());
    }
    descriptors
}

fn verify_tlc_state_binding(
    state: &HeldDirectory,
    recover: Option<&HeldDirectory>,
) -> Result<(), Box<dyn Error>> {
    state.verify_path_binding()?;
    if let Some(recover) = recover {
        recover.verify_path_binding()?;
    }
    Ok(())
}
