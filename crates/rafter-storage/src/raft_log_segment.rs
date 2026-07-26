//! Durable Raft log storage facade.
//!
//! This module maps the public retained-log contract to file-backed and
//! in-memory implementations. Continuity, framing, open/replay, state, and
//! durable replacement mechanics live in focused child modules.

mod continuity;
mod contract;
mod error;
mod file;
mod frames;
mod memory;
mod open;
mod rewrite;
mod state;

pub use contract::RaftLogSegment;
pub use error::{
    OpenRaftLogSegmentError, RaftLogReplayError, RaftLogSegmentAppendError,
    RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};
pub use memory::InMemoryRaftLogSegment;
pub use state::FileRaftLogSegment;

use continuity::{
    reject_append_bounds, reject_compact_bounds, reject_truncate_bounds, ContiguousLogEntries,
    NonContiguousRaftEntry,
};
use frames::{
    append_borrowed_raft_log_frame, read_raft_log_frames, write_raft_log_frames,
    WriteRaftLogFramesError,
};
use rewrite::{compaction_marker_path, prepare_log_rewrite, PrepareLogRewriteError};
use state::LogIoFailure;

#[cfg(test)]
use rewrite::inject_log_rewrite_publication_failure;

#[cfg(test)]
use crate::BorrowedPersistedRaftLogEntry;
#[cfg(test)]
use rafter::LogIndex;

#[cfg(test)]
#[path = "raft_log_segment/compaction_test.rs"]
mod compaction_test;
#[cfg(test)]
#[path = "raft_log_segment/health_test.rs"]
mod health_test;
#[cfg(test)]
#[path = "raft_log_segment/memory_test.rs"]
mod memory_test;
#[cfg(test)]
#[path = "raft_log_segment_test.rs"]
mod raft_log_segment_test;
#[cfg(test)]
#[path = "raft_log_segment/test_support.rs"]
mod test_support;
