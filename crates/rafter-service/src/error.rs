//! Error types for the managed service layer.
//!
//! A failed managed operation answers three different questions, and this
//! module keeps them apart because they have different types, different
//! lifetimes, and different audiences:
//!
//! - *What kind of failure was this?* — the variant, projected to a `Copy`
//!   category through [`WriteError::kind`], [`ReadError::kind`],
//!   [`TransferLeadershipError::kind`], and [`ShutdownError::kind`]. A metric
//!   label, a map key, or a structured-log field. Every operation surface a
//!   driver can fail on projects to one, so an operator aggregating driver
//!   failures never has to key on a rendered string.
//! - *May the command still take effect?* — a reported [`WriteFate`], never an
//!   inference from the category.
//! - *What actually failed?* — the typed [`ErrorCause`], reached through
//!   `source()`.
//!
//! Collapsing any two of them back together loses one of the three answers.
//!
//! The *vocabulary* those answers are given in — the diagnostic reasons, the
//! fate, the reason a driver refuses on its own standing, and the four category
//! projections — lives in the private `vocabulary` module and is re-exported from
//! here. The split
//! is the module's own sentence read literally: this file holds the error types,
//! and that one holds what they say. They are read at different times, too: an
//! embedder matching on a failure reads the types, and an operator wiring a
//! metric label reads the categories.
//!
//! The types themselves live one per operation surface, in private modules
//! re-exported here, because a caller matching on a write failure never reads
//! the read one. What stays in this file is what they share: the rendering
//! helpers every surface renders through, so a leader hint and a fate read the
//! same whichever operation reports them.

use std::{error::Error, fmt};

use rafter::{
    LeadershipTransferRejection, LocalProposalId, LogIndex, NodeId, ProposalRejection, ReadId,
    ReadIndexCancelReason, ReadIndexRejection, Term,
};
use rafter_app::proposal::ClientRequestId;
use rafter_app::read::ReadConsistency;

/// Types this module's errors carry across the `rafter-app` boundary.
///
/// They are re-exported rather than redeclared: a caller must be able to
/// compare the value it receives here with the one `rafter-app` produced, so
/// there can be only one type.
pub use rafter_app::error::{ErrorCause, StateMachineOperation};

mod kind;
mod lifecycle;
mod read;
mod transfer;
mod vocabulary;
mod write;

pub use lifecycle::{MetricsError, ShutdownError};
pub use read::ReadError;
pub use transfer::TransferLeadershipError;
pub use vocabulary::{
    DriverUnavailableReason, ReadAbandonReason, ReadErrorKind, ShutdownErrorKind,
    TransferLeadershipErrorKind, UnknownOutcomeReason, WriteErrorKind, WriteFate,
};
pub use write::WriteError;

fn write_leader_hint(
    formatter: &mut fmt::Formatter<'_>,
    leader_hint: Option<NodeId>,
) -> fmt::Result {
    if let Some(leader_hint) = leader_hint {
        write!(formatter, "; leader hint is {leader_hint}")?;
    }
    Ok(())
}

/// Renders the fate without rendering the cause.
///
/// The fate is the one thing a client branches on, so it belongs in the
/// message. The cause does not: a `Display` that interpolated it would
/// reproduce today's message in a place a caller cannot parse, and a chain
/// printer would print it twice.
fn write_write_fate(formatter: &mut fmt::Formatter<'_>, fate: WriteFate) -> fmt::Result {
    formatter.write_str(match fate {
        WriteFate::NotAppended => "; the command was not appended",
        WriteFate::Unresolved => "; the command may still commit and apply",
    })
}

const fn read_cancel_reason_message(reason: ReadIndexCancelReason) -> &'static str {
    match reason {
        ReadIndexCancelReason::LeadershipLost => "leadership was lost",
        ReadIndexCancelReason::LeaderStateReset => "leader state was reset",
        ReadIndexCancelReason::LeadershipTransfer { .. } => "leadership transfer started",
    }
}

#[cfg(test)]
#[path = "error/tests.rs"]
mod tests;
