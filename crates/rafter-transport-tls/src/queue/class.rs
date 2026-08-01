//! Transport traffic classes derived only from Raft message shape.

use std::fmt;

use rafter::Message;

/// Bounded outbound scheduling class.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum TrafficClass {
    /// Elections, leadership transfer, heartbeats, and protocol responses.
    Control,
    /// Append entries carrying log data.
    Replication,
    /// Snapshot data frames.
    Snapshot,
}

impl TrafficClass {
    /// Classifies one Rafter peer message without interpreting application data.
    #[must_use]
    pub fn for_message(message: &Message) -> Self {
        match message {
            Message::RequestVote(_)
            | Message::RequestVoteResponse(_)
            | Message::PreVote(_)
            | Message::PreVoteResponse(_)
            | Message::TimeoutNow(_)
            | Message::AppendEntriesResponse(_)
            | Message::InstallSnapshotResponse(_) => Self::Control,
            Message::AppendEntries(request) if request.entries.is_empty() => Self::Control,
            Message::AppendEntries(_) => Self::Replication,
            Message::InstallSnapshot(_) | Message::InstallSnapshotChunk(_) => Self::Snapshot,
        }
    }
}

impl fmt::Display for TrafficClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Control => "control",
            Self::Replication => "replication",
            Self::Snapshot => "snapshot",
        })
    }
}
