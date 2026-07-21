//! Absolute replay deadlines with a verifier-owned publication reserve.

use std::time::{Duration, Instant};

use crate::contract::profile::DetectorReplayContract;

const PUBLICATION_RESERVE: Duration = Duration::from_secs(20);

#[derive(Clone, Copy, Debug)]
pub(in crate::verification) struct ReplayDeadlines {
    work: Instant,
    publication: Instant,
}

impl ReplayDeadlines {
    pub(super) fn from_contract(contract: &DetectorReplayContract) -> Result<Self, String> {
        Self::from_timeout(Duration::from_secs(contract.total_timeout_seconds))
    }

    fn from_timeout(total_timeout: Duration) -> Result<Self, String> {
        let now = Instant::now();
        let publication = now
            .checked_add(total_timeout)
            .ok_or_else(|| "detector replay total deadline overflow".to_owned())?;
        let work = publication
            .checked_sub(PUBLICATION_RESERVE)
            .filter(|deadline| *deadline > now)
            .ok_or_else(|| {
                format!(
                    "detector replay total timeout must exceed the {} second publication reserve",
                    PUBLICATION_RESERVE.as_secs()
                )
            })?;
        Ok(Self { work, publication })
    }

    pub(in crate::verification) const fn work(self) -> Instant {
        self.work
    }

    pub(super) const fn publication(self) -> Instant {
        self.publication
    }
}

#[cfg(test)]
#[path = "deadlines_tests.rs"]
mod tests;
