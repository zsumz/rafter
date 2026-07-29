use std::{
    fmt, thread,
    time::{Duration, Instant},
};

/// A deadline and polling rate for a predicate-based wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Wait {
    timeout: Duration,
    poll_interval: Duration,
}

impl Wait {
    /// Creates a wait with an explicit deadline and polling rate.
    ///
    /// # Panics
    ///
    /// Panics when either duration is zero.
    #[must_use]
    pub const fn new(timeout: Duration, poll_interval: Duration) -> Self {
        assert!(!timeout.is_zero(), "a wait timeout must be nonzero");
        assert!(
            !poll_interval.is_zero(),
            "a polling interval must be nonzero"
        );
        Self {
            timeout,
            poll_interval,
        }
    }

    /// Returns the configured deadline duration.
    #[must_use]
    pub const fn timeout(self) -> Duration {
        self.timeout
    }

    /// Polls until `predicate` yields a value or the deadline expires.
    ///
    /// # Errors
    ///
    /// Returns [`WaitError`] when the deadline expires first.
    pub fn until<T>(
        self,
        condition: impl Into<String>,
        mut predicate: impl FnMut() -> Option<T>,
    ) -> Result<T, WaitError> {
        let condition = condition.into();
        let deadline = Instant::now() + self.timeout;
        loop {
            if let Some(value) = predicate() {
                return Ok(value);
            }
            if Instant::now() >= deadline {
                return Err(WaitError {
                    condition,
                    timeout: self.timeout,
                });
            }
            thread::sleep(self.poll_interval);
        }
    }
}

/// A named predicate that did not become true before its deadline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitError {
    condition: String,
    timeout: Duration,
}

impl WaitError {
    /// Returns the condition the caller was awaiting.
    #[must_use]
    pub fn condition(&self) -> &str {
        &self.condition
    }

    /// Returns the elapsed deadline.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }
}

impl fmt::Display for WaitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "timed out after {:?} waiting for {}",
            self.timeout, self.condition
        )
    }
}

impl std::error::Error for WaitError {}
