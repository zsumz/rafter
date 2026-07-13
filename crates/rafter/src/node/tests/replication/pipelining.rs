//! Progress/Inflights pipelining across probe, replicate, and snapshot modes.

pub(super) use super::super::helpers::bootstrap_entry;
pub(super) use super::super::snapshot::support::test_snapshot;
pub(super) use super::*;
pub(super) use crate::node::state::{Inflights, Progress, ProgressMode};

/// Every pipelining test replicates 100-byte payloads under a 180-byte batch
/// budget: one application entry costs 164 replication bytes, so each batch
/// carries exactly one entry and window arithmetic is exact.
const PAYLOAD_BYTES: usize = 100;
const ONE_ENTRY_BATCH_BUDGET: usize = 180;

mod observability;
mod probe;
mod sharing;
mod snapshot;
mod support;
mod window;
