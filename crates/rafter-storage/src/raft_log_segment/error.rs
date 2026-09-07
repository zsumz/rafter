//! Public error vocabulary for log mutation, open, replay, and repair.

mod mutation;
mod recovery;

pub use mutation::{
    RaftLogSegmentAppendError, RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};
pub use recovery::{OpenRaftLogSegmentError, RaftLogReplayError};
