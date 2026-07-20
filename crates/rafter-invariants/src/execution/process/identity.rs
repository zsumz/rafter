//! Bounded command execution for source and toolchain identity observation.

use std::{error::Error, path::Path, time::Duration};

use super::internal_command::bounded_identity_output;

const IDENTITY_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IdentityCommandOutput {
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

pub(crate) fn run_identity_command_in(
    program: &str,
    arguments: &[&str],
    current_dir: &Path,
) -> Result<IdentityCommandOutput, Box<dyn Error>> {
    let output =
        bounded_identity_output(program, arguments, current_dir, IDENTITY_COMMAND_TIMEOUT)?;
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    if !output.status.success() {
        return Err(format!(
            "bounded identity command {program} failed with {:?}: stdout: {}; stderr: {}",
            output.status.code(),
            stdout.trim(),
            stderr.trim()
        )
        .into());
    }
    Ok(IdentityCommandOutput { stdout, stderr })
}
