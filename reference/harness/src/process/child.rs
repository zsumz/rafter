use std::{
    fmt, io,
    process::{Child, Command, ExitStatus, Stdio},
};

use super::{lines::RetainedLines, scratch::ScratchLease, ScratchSpace, Wait};

/// A child whose output is retained and whose lifetime always ends in reaping.
#[derive(Debug)]
pub struct ChildProcess {
    identity: String,
    child: Child,
    stdout: RetainedLines,
    stderr: RetainedLines,
    status: Option<ExitStatus>,
    _scratch: Option<ScratchLease>,
}

impl ChildProcess {
    /// Spawns a caller-configured command with captured output.
    ///
    /// # Errors
    ///
    /// Returns the operating-system error that prevented spawning.
    pub fn spawn(identity: impl Into<String>, command: &mut Command) -> io::Result<Self> {
        Self::spawn_inner(identity.into(), command, None)
    }

    /// Spawns a child while keeping `scratch` alive through the child's drop.
    ///
    /// # Errors
    ///
    /// Returns the operating-system error that prevented spawning.
    pub fn spawn_in(
        identity: impl Into<String>,
        command: &mut Command,
        scratch: &ScratchSpace,
    ) -> io::Result<Self> {
        Self::spawn_inner(identity.into(), command, Some(scratch.lease()))
    }

    fn spawn_inner(
        identity: String,
        command: &mut Command,
        scratch: Option<ScratchLease>,
    ) -> io::Result<Self> {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let stdout = child
            .stdout
            .take()
            .expect("captured child stdout is available");
        let stderr = child
            .stderr
            .take()
            .expect("captured child stderr is available");
        Ok(Self {
            identity,
            child,
            stdout: RetainedLines::capture(stdout),
            stderr: RetainedLines::capture(stderr),
            status: None,
            _scratch: scratch,
        })
    }

    /// Returns the operating-system process identifier.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    /// Returns every stdout line observed so far.
    pub fn stdout_lines(&mut self) -> &[String] {
        self.stdout.lines()
    }

    /// Returns every stderr line observed so far.
    pub fn stderr_lines(&mut self) -> &[String] {
        self.stderr.lines()
    }

    /// Returns whether stdout has contained a line beginning with `prefix`.
    pub fn has_stdout_prefix(&mut self, prefix: &str) -> bool {
        self.stdout_lines()
            .iter()
            .any(|line| line.starts_with(prefix))
    }

    /// Awaits an interpretation of the retained stdout lines.
    ///
    /// # Errors
    ///
    /// Returns diagnostic context when the child exits, becomes unobservable,
    /// or the deadline expires before the predicate yields a value.
    pub fn wait_for_stdout<T>(
        &mut self,
        condition: &str,
        wait: Wait,
        mut predicate: impl FnMut(&[String]) -> Option<T>,
    ) -> Result<T, ChildWaitError> {
        enum Observation<T> {
            Value(T),
            Exited(ExitStatus),
            Failed(String),
        }

        let observation = wait.until(condition, || {
            self.stdout.drain();
            if let Some(value) = predicate(self.stdout.lines()) {
                return Some(Observation::Value(value));
            }
            match self.try_status() {
                Ok(status) => status.map(Observation::Exited),
                Err(error) => Some(Observation::Failed(error.to_string())),
            }
        });
        match observation {
            Ok(Observation::Value(value)) => Ok(value),
            Ok(Observation::Exited(status)) => {
                self.finish_output();
                if let Some(value) = predicate(self.stdout.lines()) {
                    Ok(value)
                } else {
                    Err(self.diagnostic(condition, wait.timeout(), Some(status)))
                }
            }
            Ok(Observation::Failed(detail)) => {
                self.drain_output();
                let condition = format!("{condition}; child status failed: {detail}");
                Err(self.diagnostic(&condition, wait.timeout(), self.status))
            }
            Err(error) => {
                self.drain_output();
                if let Some(value) = predicate(self.stdout.lines()) {
                    Ok(value)
                } else {
                    Err(self.diagnostic(error.condition(), error.timeout(), self.status))
                }
            }
        }
    }

    /// Awaits a stdout line beginning with `prefix`.
    ///
    /// # Errors
    ///
    /// Returns diagnostic context when the line does not arrive before the
    /// child exits or the deadline expires.
    pub fn wait_for_stdout_prefix(
        &mut self,
        prefix: &str,
        wait: Wait,
    ) -> Result<String, ChildWaitError> {
        let condition = format!("stdout line beginning with {prefix:?}");
        self.wait_for_stdout(&condition, wait, |lines| {
            lines.iter().find(|line| line.starts_with(prefix)).cloned()
        })
    }

    /// Awaits the child's exit and returns its status.
    ///
    /// # Errors
    ///
    /// Returns diagnostic context when the child remains live through the
    /// deadline or its status cannot be observed.
    pub fn wait_for_exit(&mut self, wait: Wait) -> Result<ExitStatus, ChildWaitError> {
        let identity = self.identity.clone();
        let status = wait
            .until(format!("{identity} to exit"), || match self.try_status() {
                Ok(Some(status)) => Some(Ok(status)),
                Ok(None) => None,
                Err(error) => Some(Err(error.to_string())),
            })
            .map_err(|error| {
                self.drain_output();
                self.diagnostic(error.condition(), error.timeout(), self.status)
            })?
            .map_err(|detail| {
                self.drain_output();
                self.diagnostic(
                    &format!("{identity} status to remain observable: {detail}"),
                    wait.timeout(),
                    self.status,
                )
            })?;
        self.finish_output();
        Ok(status)
    }

    /// Sends a forceful termination signal when needed and reaps the child.
    ///
    /// # Errors
    ///
    /// Returns an error when the child status, termination, or wait operation
    /// fails.
    pub fn kill_and_reap(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.try_status()? {
            self.finish_output();
            return Ok(status);
        }
        self.child.kill()?;
        let status = self.child.wait()?;
        self.status = Some(status);
        self.finish_output();
        Ok(status)
    }

    fn try_status(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        let status = self.child.try_wait()?;
        if let Some(status) = status {
            self.status = Some(status);
        }
        Ok(status)
    }

    fn drain_output(&mut self) {
        self.stdout.drain();
        self.stderr.drain();
    }

    fn finish_output(&mut self) {
        self.stdout.finish();
        self.stderr.finish();
    }

    fn diagnostic(
        &mut self,
        condition: &str,
        timeout: std::time::Duration,
        status: Option<ExitStatus>,
    ) -> ChildWaitError {
        ChildWaitError {
            identity: self.identity.clone(),
            condition: condition.to_string(),
            timeout,
            status,
            stdout: self.stdout.lines().to_vec(),
            stderr: self.stderr.lines().to_vec(),
        }
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        drop(self.kill_and_reap());
    }
}

/// Diagnostic context for an unmet child condition.
#[derive(Clone, Debug)]
pub struct ChildWaitError {
    identity: String,
    condition: String,
    timeout: std::time::Duration,
    status: Option<ExitStatus>,
    stdout: Vec<String>,
    stderr: Vec<String>,
}

impl ChildWaitError {
    /// Returns the child identity supplied at spawn time.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Returns the awaited condition.
    #[must_use]
    pub fn condition(&self) -> &str {
        &self.condition
    }

    /// Returns the observed exit status, if the child exited.
    #[must_use]
    pub const fn status(&self) -> Option<ExitStatus> {
        self.status
    }

    /// Returns retained stdout.
    #[must_use]
    pub fn stdout(&self) -> &[String] {
        &self.stdout
    }

    /// Returns retained stderr.
    #[must_use]
    pub fn stderr(&self) -> &[String] {
        &self.stderr
    }
}

impl fmt::Display for ChildWaitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} did not satisfy {} within {:?}; exit={:?}; stdout={:?}; stderr={:?}",
            self.identity, self.condition, self.timeout, self.status, self.stdout, self.stderr
        )
    }
}

impl std::error::Error for ChildWaitError {}
