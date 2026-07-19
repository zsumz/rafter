//! Retained subprocess diagnostics for lifecycle and telemetry failures.

use std::{
    error::Error,
    fmt,
    path::{Path, PathBuf},
};

#[derive(Debug)]
struct ProcessCleanupError {
    detail: String,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    telemetry_path: Option<PathBuf>,
}

impl fmt::Display for ProcessCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; retained subprocess stdout at {} and stderr at {}",
            self.detail,
            self.stdout_path.display(),
            self.stderr_path.display()
        )?;
        if let Some(path) = &self.telemetry_path {
            write!(formatter, " and resource telemetry at {}", path.display())?;
        }
        Ok(())
    }
}

impl Error for ProcessCleanupError {}

#[cfg(test)]
pub(crate) fn retained_stderr_path(error: &(dyn Error + 'static)) -> Option<PathBuf> {
    error
        .downcast_ref::<ProcessCleanupError>()
        .map(|error| error.stderr_path.clone())
}

#[cfg(test)]
pub(crate) fn cleanup_error(
    error: impl fmt::Display,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Box<dyn Error> {
    retained_error(error, stdout_path, stderr_path, None)
}

pub(crate) fn retained_error(
    error: impl fmt::Display,
    stdout_path: &Path,
    stderr_path: &Path,
    telemetry_path: Option<&Path>,
) -> Box<dyn Error> {
    Box::new(ProcessCleanupError {
        detail: error.to_string(),
        stdout_path: stdout_path.to_owned(),
        stderr_path: stderr_path.to_owned(),
        telemetry_path: telemetry_path.map(Path::to_owned),
    })
}

pub(crate) fn measurement_error(
    error: impl fmt::Display,
    stdout_path: &Path,
    stderr_path: &Path,
    telemetry_path: &Path,
) -> Box<dyn Error> {
    retained_error(error, stdout_path, stderr_path, Some(telemetry_path))
}

pub(crate) fn retained_result<T, E: fmt::Display>(
    result: Result<T, E>,
    stdout_path: &Path,
    stderr_path: &Path,
    telemetry_path: Option<&Path>,
) -> Result<T, Box<dyn Error>> {
    result.map_err(|error| retained_error(error, stdout_path, stderr_path, telemetry_path))
}
