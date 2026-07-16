//! Stable version 1 discriminants and permanent reservations.

use crate::DecodePeerMessageError;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MessageTag {
    RequestVote = 1,
    RequestVoteResponse = 2,
    AppendEntries = 3,
    AppendEntriesResponse = 4,
    // 5 is permanently reserved for the unsupported draft whole-snapshot form.
    InstallSnapshotResponse = 6,
    InstallSnapshotChunk = 7,
    PreVote = 8,
    PreVoteResponse = 9,
    TimeoutNow = 10,
}

impl From<MessageTag> for u8 {
    fn from(tag: MessageTag) -> Self {
        tag as Self
    }
}

impl TryFrom<u8> for MessageTag {
    type Error = DecodePeerMessageError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::RequestVote),
            2 => Ok(Self::RequestVoteResponse),
            3 => Ok(Self::AppendEntries),
            4 => Ok(Self::AppendEntriesResponse),
            6 => Ok(Self::InstallSnapshotResponse),
            7 => Ok(Self::InstallSnapshotChunk),
            8 => Ok(Self::PreVote),
            9 => Ok(Self::PreVoteResponse),
            10 => Ok(Self::TimeoutNow),
            other => Err(DecodePeerMessageError::UnknownMessageType(other)),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LogEntryTag {
    Application = 0,
    ConfigurationStable = 1,
    ConfigurationJoint = 2,
    Noop = 3,
}

impl From<LogEntryTag> for u8 {
    fn from(tag: LogEntryTag) -> Self {
        tag as Self
    }
}

impl TryFrom<u8> for LogEntryTag {
    type Error = DecodePeerMessageError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Application),
            1 => Ok(Self::ConfigurationStable),
            2 => Ok(Self::ConfigurationJoint),
            3 => Ok(Self::Noop),
            other => Err(DecodePeerMessageError::UnknownLogEntryKind(other)),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MembershipTag {
    Stable = 0,
    Joint = 1,
}

impl From<MembershipTag> for u8 {
    fn from(tag: MembershipTag) -> Self {
        tag as Self
    }
}

impl TryFrom<u8> for MembershipTag {
    type Error = DecodePeerMessageError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Stable),
            1 => Ok(Self::Joint),
            other => Err(DecodePeerMessageError::UnknownMembershipKind(other)),
        }
    }
}
