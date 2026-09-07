//! Caller-supplied proposal and read identifiers across the persist fence.
//!
//! The children cover tracked appends, read barriers, rejections that never
//! touch the log, and what a restart deliberately does not carry. Shared here:
//! reading the sequence number back out of a leader's replication traffic.

use super::*;

mod proposal;
mod read_index;
mod recovery;
mod rejection;

fn append_entries_sequence(outputs: &[RaftOutput]) -> u64 {
    outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::Send {
                message: Message::AppendEntries(request),
                ..
            } => Some(request.sequence),
            _ => None,
        })
        .expect("leader output includes append entries")
}
