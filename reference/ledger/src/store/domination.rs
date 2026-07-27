//! What a republished ledger has to dominate before it may be adopted.
//!
//! One comparison, and it is scoped to the single publication the applied Raft
//! index cannot judge: a rewrite at the index the store already holds. Two
//! images at one index can disagree about which requests have completed, and
//! the poorer one makes an acknowledged mutation executable a second time.

use std::{collections::BTreeMap, fmt};

use crate::{ClientId, Ledger, Sequence, SessionEpoch};

use super::error::LedgerStoreError;

/// How far one client slot's session had progressed when it was made durable.
///
/// This is the key the deduplication cache is ordered by, and it is deliberately
/// not the applied Raft index: a rewrite may republish the index the store
/// already holds, and at that index the index itself says nothing about which
/// requests have completed.
///
/// Ordering is lexicographic — the session epoch first, then the highest
/// completed sequence under it — because opening a newer epoch is exactly what
/// legitimately clears an older epoch's cache. A slot on a later epoch has not
/// lost anything by holding no completion yet.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DeduplicationProgress {
    /// Session generation the slot was on.
    pub session_epoch: SessionEpoch,
    /// Highest completed sequence cached under that epoch, if any.
    pub completed: Option<Sequence>,
}

impl fmt::Display for DeduplicationProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.completed {
            Some(sequence) => write!(
                formatter,
                "epoch {} through sequence {}",
                self.session_epoch.get(),
                sequence.get()
            ),
            None => write!(
                formatter,
                "epoch {} with nothing completed",
                self.session_epoch.get()
            ),
        }
    }
}

/// Returns every client slot's deduplication progress.
pub(super) fn deduplication_progress_of(
    ledger: &Ledger,
) -> BTreeMap<ClientId, DeduplicationProgress> {
    ledger
        .view()
        .sessions
        .into_iter()
        .map(|session| {
            (
                session.client_id,
                DeduplicationProgress {
                    session_epoch: session.session_epoch,
                    completed: session.cached.map(|(sequence, _, _)| sequence),
                },
            )
        })
        .collect()
}

/// Refuses a ledger that would move any client slot's deduplication state
/// backwards.
///
/// A slot that disappears is the same failure as one whose progress decreases:
/// both let a request identity the store has already answered execute again.
pub(super) fn verify_deduplication_dominates(
    acknowledged: &BTreeMap<ClientId, DeduplicationProgress>,
    offered: &BTreeMap<ClientId, DeduplicationProgress>,
) -> Result<(), LedgerStoreError> {
    for (client_id, progress) in acknowledged {
        let found = offered.get(client_id).copied();
        if found.is_none_or(|offered_progress| offered_progress < *progress) {
            return Err(LedgerStoreError::DeduplicationRegression {
                client_id: *client_id,
                acknowledged: *progress,
                offered: found,
            });
        }
    }
    Ok(())
}
