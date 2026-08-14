//! Shared TLA+ execution deadline and finalization-reserve model.

use std::{
    collections::BTreeMap,
    error::Error,
    time::{Duration, Instant},
};

use super::super::{process, tla_output::DETECTOR_PROBES};

pub(super) const TOTAL_TIMEOUT_KEY: &str = "total_timeout";
pub(super) const FINALIZATION_RESERVE_KEY: &str = "finalization_reserve";
// Every phase below is paid out of one `execution_deadline`, so the inventory
// has to be counted whole. The hosted PR tier is 4m setup + 7m qualification +
// 10m proof obligations + 310m main + 2m finalization = 333m inside a 338m
// budget. Scheduled runners retain the wider qualification caps (32m) and fund
// larger obligation families out of a 320m budget, which is why their
// reporting primaries are the shortest of the three. The contract layer
// re-derives this same inventory and refuses a profile that overcommits it.
const PR_PROBE_TIMEOUT: Duration = Duration::from_secs(15);
const PR_MUTATION_SUITE_TIMEOUT: Duration = Duration::from_secs(4 * 60);
const SCHEDULED_PROBE_TIMEOUT: Duration = Duration::from_secs(120);
const SCHEDULED_MUTATION_SUITE_TIMEOUT: Duration = Duration::from_secs(8 * 60);
pub(super) const QUALIFICATION_PHASE_COUNT: usize = DETECTOR_PROBES.len() + 1;

pub(super) fn probe_timeout(profile: &str) -> Duration {
    if profile == "pr" {
        PR_PROBE_TIMEOUT
    } else {
        SCHEDULED_PROBE_TIMEOUT
    }
}

pub(super) fn mutation_suite_timeout(profile: &str) -> Duration {
    if profile == "pr" {
        PR_MUTATION_SUITE_TIMEOUT
    } else {
        SCHEDULED_MUTATION_SUITE_TIMEOUT
    }
}

pub(super) fn maximum_qualification_time(profile: &str) -> Option<Duration> {
    probe_timeout(profile)
        .checked_mul(u32::try_from(QUALIFICATION_PHASE_COUNT).ok()?)
        .and_then(|duration| duration.checked_add(mutation_suite_timeout(profile)))
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ExecutionBudget {
    pub(super) execution_deadline: Instant,
    pub(super) total_deadline: Instant,
}

impl ExecutionBudget {
    pub(super) fn from_configuration(
        profile: &str,
        configuration: &BTreeMap<String, String>,
    ) -> Result<Self, Box<dyn Error>> {
        Self::at(profile, configuration, Instant::now())?;
        let (execution_deadline, total_deadline) = process::active_layer_deadlines(profile, "tla")?;
        Ok(Self {
            execution_deadline,
            total_deadline,
        })
    }

    pub(super) fn at(
        profile: &str,
        configuration: &BTreeMap<String, String>,
        started: Instant,
    ) -> Result<Self, Box<dyn Error>> {
        let total = configured_budget_duration(configuration, TOTAL_TIMEOUT_KEY)?;
        let reserve = configured_budget_duration(configuration, FINALIZATION_RESERVE_KEY)?;
        match (total, reserve) {
            (Some(total), Some(reserve)) => {
                let execution_window = total
                    .checked_sub(reserve)
                    .filter(|window| !window.is_zero())
                    .ok_or("TLA total_timeout must exceed finalization_reserve")?;
                let maximum_probe_time = maximum_qualification_time(profile)
                    .ok_or("TLA qualification budget overflow")?;
                if execution_window <= maximum_probe_time {
                    return Err(
                        "TLA shared execution budget must leave time for the main model check"
                            .into(),
                    );
                }
                let execution_deadline = started
                    .checked_add(execution_window)
                    .ok_or("TLA shared execution deadline overflow")?;
                let total_deadline = started
                    .checked_add(total)
                    .ok_or("TLA total deadline overflow")?;
                Ok(Self {
                    execution_deadline,
                    total_deadline,
                })
            }
            (None, None) => Err(format!(
                "{profile} TLA runner requires total_timeout and finalization_reserve"
            )
            .into()),
            _ => {
                Err("TLA total_timeout and finalization_reserve must be configured together".into())
            }
        }
    }

    pub(super) fn phase_timeout(self, cap: Duration) -> Option<Duration> {
        self.phase_timeout_at(Instant::now(), cap)
    }

    pub(super) fn phase_timeout_at(self, now: Instant, cap: Duration) -> Option<Duration> {
        let remaining = self.execution_deadline.checked_duration_since(now)?;
        (!remaining.is_zero()).then_some(cap.min(remaining))
    }

    pub(super) fn whole_phase_timeout(self, cap: Duration) -> Option<Duration> {
        self.whole_phase_timeout_at(Instant::now(), cap)
    }

    /// A phase that is only meaningful at its full cap, or not at all.
    ///
    /// Truncation is the right answer for the qualification probes and for the
    /// primary continuation: a short probe still qualifies and a short
    /// continuation still accumulates. It is the wrong answer for an
    /// obligation, whose whole contract is "drain this frontier within a
    /// calibrated budget". Handing one a shortened clock converts a budget
    /// shortfall into a theorem that appears not to hold.
    pub(super) fn whole_phase_timeout_at(self, now: Instant, cap: Duration) -> Option<Duration> {
        let remaining = self.execution_deadline.checked_duration_since(now)?;
        (remaining >= cap).then_some(cap)
    }
}

pub(super) fn configured_budget_duration(
    configuration: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<Duration>, Box<dyn Error>> {
    configuration
        .get(key)
        .map(|value| {
            let minutes = value
                .strip_suffix('m')
                .ok_or_else(|| format!("TLA {key} must use whole minutes"))?
                .parse::<u64>()?;
            let seconds = minutes
                .checked_mul(60)
                .ok_or_else(|| format!("TLA {key} is too large"))?;
            Ok(Duration::from_secs(seconds))
        })
        .transpose()
}
