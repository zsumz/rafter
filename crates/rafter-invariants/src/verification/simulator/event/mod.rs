//! Machine-event contracts, indexing, and semantic failure routing.

mod contract;
mod index;
mod issue;
mod routing;

pub(crate) use contract::{machine_invariant_id, verified_passing_simulator_event_contract};
pub(crate) use index::simulator_events;
pub(crate) use issue::{execution_is_passing, merge_raw_issue, receipt_issue, RawEventIssue};
pub(crate) use routing::{inspect_machine_events, verify_nonpassing_event_classification};

#[cfg(test)]
pub(crate) use index::index_simulator_event;
#[cfg(test)]
pub(crate) use routing::raw_event_issue;
