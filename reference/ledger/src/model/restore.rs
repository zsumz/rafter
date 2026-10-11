//! Incremental restoration owns the model's bounds, uniqueness, and supply checks.

use super::{CachedCompletion, Ledger, LedgerConfig, SessionRecord, SnapshotError};
use crate::{AccountId, ClientId, SessionView};

pub(crate) struct Restore {
    ledger: Ledger,
    total_balance: u128,
}

impl Restore {
    pub(crate) fn new(config: LedgerConfig, successful_deposits: u128) -> Self {
        let mut ledger = Ledger::new(config);
        ledger.successful_deposits = successful_deposits;
        Self {
            ledger,
            total_balance: 0,
        }
    }

    pub(crate) fn admit_accounts(&self, count: usize) -> Result<(), SnapshotError> {
        if count > self.ledger.config.max_accounts() {
            Err(SnapshotError::TooManyAccounts)
        } else {
            Ok(())
        }
    }

    pub(crate) fn admit_sessions(&self, count: usize) -> Result<(), SnapshotError> {
        if u32::try_from(count).map_or(true, |count| count > self.ledger.config.max_clients()) {
            Err(SnapshotError::TooManySessions)
        } else {
            Ok(())
        }
    }

    pub(crate) fn account(&mut self, id: AccountId, balance: u64) -> Result<(), SnapshotError> {
        if self.ledger.accounts.len() >= self.ledger.config.max_accounts() {
            return Err(SnapshotError::TooManyAccounts);
        }
        if self.ledger.accounts.insert(id, balance).is_some() {
            return Err(SnapshotError::DuplicateAccount);
        }
        self.total_balance = self
            .total_balance
            .checked_add(u128::from(balance))
            .ok_or(SnapshotError::SupplyOverflow)?;
        Ok(())
    }

    pub(crate) fn session(&mut self, view: SessionView) -> Result<(), SnapshotError> {
        self.record(
            view.client_id,
            SessionRecord {
                session_epoch: view.session_epoch,
                cached: view
                    .cached
                    .map(|(sequence, mutation, result)| CachedCompletion {
                        sequence,
                        mutation,
                        result,
                    }),
            },
        )
    }

    pub(super) fn record(
        &mut self,
        id: ClientId,
        session: SessionRecord,
    ) -> Result<(), SnapshotError> {
        if self.ledger.sessions.len() as u64 >= u64::from(self.ledger.config.max_clients()) {
            return Err(SnapshotError::TooManySessions);
        }
        if !self.ledger.config.admits_client(id) {
            return Err(SnapshotError::ClientOutOfRange);
        }
        if self.ledger.sessions.insert(id, session).is_some() {
            return Err(SnapshotError::DuplicateSession);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Ledger, SnapshotError> {
        if self.total_balance != self.ledger.successful_deposits {
            return Err(SnapshotError::SupplyMismatch);
        }
        Ok(self.ledger)
    }
}
