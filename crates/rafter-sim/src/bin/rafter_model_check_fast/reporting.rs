#[path = "reporting/failure.rs"]
mod failure;
#[path = "reporting/liveness.rs"]
mod liveness;
#[path = "reporting/soak.rs"]
mod soak;
#[path = "reporting/summary.rs"]
mod summary;

use std::time::Duration;

use rafter_sim::model_check::Summary;

pub(crate) use failure::{failure_timeline_lines, print_raft_failure, print_soak_failure};
pub(crate) use soak::print_soak_summary;
pub(crate) use summary::{print_profile_total, print_raft_summary};

pub(crate) fn raft_summary_line(name: &str, summary: Summary, duration: Duration) -> String {
    summary::raft_summary_line(name, summary, duration)
}

#[cfg(test)]
use failure::failure_event;
#[cfg(test)]
use soak::{
    soak_event, soak_event_from_reports, soak_event_from_reports_with_contract,
    test_execution_contract,
};
#[cfg(test)]
pub(crate) use summary::raft_summary_line_for_counts;

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

#[cfg(test)]
#[path = "reporting/tests.rs"]
mod tests;
