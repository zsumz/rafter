//! Invocation, executable, environment, and tracked plan-input capture.

use std::{collections::BTreeMap, env, error::Error, ffi::OsString, fs, path::Path};

use sha2::{Digest, Sha256};

use crate::evidence::{InvocationReceipt, PlanInput};

use super::{model::CapturedInvocation, validate::confined_path};

pub(crate) fn current_source_ref() -> Result<String, Box<dyn Error>> {
    crate::provenance::source::head_commit_at(Path::new("."))
}

/// Captures the actual invariant binary argv, working directory, and safe
/// environment digest for a receipt.
///
/// # Errors
///
/// Returns an error when argv or the working directory is not valid UTF-8.
pub(crate) fn capture_invocation() -> Result<CapturedInvocation, Box<dyn Error>> {
    let captured = capture_invocation_from(env::args_os().collect())?;
    crate::provenance::image::verify_capture(
        Path::new(&captured.receipt.program),
        &captured.program_bytes,
    )?;
    Ok(captured)
}

pub(super) fn capture_invocation_from(
    argv: Vec<OsString>,
) -> Result<CapturedInvocation, Box<dyn Error>> {
    capture_invocation_from_program(argv, &env::current_exe()?)
}

pub(super) fn capture_invocation_from_program(
    mut argv: Vec<OsString>,
    program: &Path,
) -> Result<CapturedInvocation, Box<dyn Error>> {
    if argv.is_empty() {
        return Err("invariant invocation omitted argv[0]".into());
    }
    let _argv_zero = argv
        .remove(0)
        .into_string()
        .map_err(|_| "invariant program path is not UTF-8")?;
    let arguments = argv
        .into_iter()
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| "invariant argument is not UTF-8")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let current_dir = fs::canonicalize(".")?
        .into_os_string()
        .into_string()
        .map_err(|_| "invariant working directory is not UTF-8")?;
    let environment = safe_environment();
    let environment_sha256 = crate::provenance::invocation::digest_environment(&environment)?;
    let program = fs::canonicalize(program)?
        .into_os_string()
        .into_string()
        .map_err(|_| "invariant executable path is not UTF-8")?;
    let program_bytes = fs::read(&program)?;
    let program_sha256 = format!("{:x}", Sha256::digest(&program_bytes));
    Ok(CapturedInvocation {
        receipt: InvocationReceipt {
            program,
            program_sha256,
            arguments,
            current_dir,
            environment,
            environment_sha256,
            launchers: Vec::new(),
        },
        program_bytes,
    })
}

pub(super) fn plan_input(path: &Path) -> Result<PlanInput, Box<dyn Error>> {
    let root = fs::canonicalize(".")?;
    let canonical = confined_path(path, &root)?;
    let relative = canonical.strip_prefix(&root)?.to_path_buf();
    crate::provenance::source::require_tracked_source_path_at(&root, &relative)
        .map_err(|error| format!("execution-plan input is not tracked: {error}"))?;
    let bytes = fs::read(canonical)?;
    Ok(PlanInput {
        path: relative.to_string_lossy().into_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        size_bytes: bytes.len() as u64,
    })
}

fn safe_environment() -> BTreeMap<String, String> {
    const ALLOWED: &[&str] = &[
        "CARGO_HOME",
        "DEVELOPER_DIR",
        "HOME",
        "PATH",
        "RUSTUP_HOME",
        "SDKROOT",
        "SYSTEMROOT",
    ];
    ALLOWED
        .iter()
        .filter_map(|name| env::var(name).ok().map(|value| ((*name).to_owned(), value)))
        .collect()
}
