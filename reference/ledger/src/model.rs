use std::{cmp::Ordering, collections::BTreeMap};

use crate::{
    AccountId, ApplyDisposition, ApplyOutcome, BusinessRejection, ClientId, Command, LedgerConfig,
    LedgerResponse, LedgerSummary, LedgerView, Mutation, MutationResult, RequestIdentity,
    RequestRejection, Sequence, SessionEpoch, SessionView,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct CachedCompletion {
    sequence: Sequence,
    mutation: Mutation,
    result: MutationResult,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SessionRecord {
    session_epoch: SessionEpoch,
    cached: Option<CachedCompletion>,
}

/// Opaque transport-neutral snapshot of the pure ledger model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerSnapshot {
    accounts: Vec<(AccountId, u64)>,
    sessions: Vec<(ClientId, SessionRecord)>,
    successful_deposits: u128,
}

/// Invalid ledger snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    /// The snapshot exceeds the configured account bound.
    TooManyAccounts,
    /// The snapshot exceeds the configured client-slot bound.
    TooManySessions,
    /// A session belongs to a client outside the configured slot range.
    ClientOutOfRange,
    /// The snapshot contains the same account more than once.
    DuplicateAccount,
    /// The snapshot contains the same client slot more than once.
    DuplicateSession,
    /// Summing account balances exceeded the model's supply representation.
    SupplyOverflow,
    /// Account balances do not equal successful external deposits.
    SupplyMismatch,
}

/// Deterministic ledger implementation that will later sit behind
/// `rafter-app`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Ledger {
    config: LedgerConfig,
    accounts: BTreeMap<AccountId, u64>,
    sessions: BTreeMap<ClientId, SessionRecord>,
    successful_deposits: u128,
}

impl Ledger {
    /// Creates an empty bounded ledger.
    #[must_use]
    pub fn new(config: LedgerConfig) -> Self {
        Self {
            config,
            accounts: BTreeMap::new(),
            sessions: BTreeMap::new(),
            successful_deposits: 0,
        }
    }

    /// Restores and validates a snapshot under `config`.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot violates a resource or supply
    /// invariant.
    pub fn from_snapshot(
        config: LedgerConfig,
        snapshot: LedgerSnapshot,
    ) -> Result<Self, SnapshotError> {
        if snapshot.accounts.len() > config.max_accounts() {
            return Err(SnapshotError::TooManyAccounts);
        }
        if u32::try_from(snapshot.sessions.len())
            .map_or(true, |session_count| session_count > config.max_clients())
        {
            return Err(SnapshotError::TooManySessions);
        }

        let mut accounts = BTreeMap::new();
        let mut total_balance = 0_u128;
        for (account_id, balance) in snapshot.accounts {
            if accounts.insert(account_id, balance).is_some() {
                return Err(SnapshotError::DuplicateAccount);
            }
            total_balance = total_balance
                .checked_add(u128::from(balance))
                .ok_or(SnapshotError::SupplyOverflow)?;
        }
        if total_balance != snapshot.successful_deposits {
            return Err(SnapshotError::SupplyMismatch);
        }

        let mut sessions = BTreeMap::new();
        for (client_id, session) in snapshot.sessions {
            if !config.admits_client(client_id) {
                return Err(SnapshotError::ClientOutOfRange);
            }
            if sessions.insert(client_id, session).is_some() {
                return Err(SnapshotError::DuplicateSession);
            }
        }

        Ok(Self {
            config,
            accounts,
            sessions,
            successful_deposits: snapshot.successful_deposits,
        })
    }

    /// Applies one replicated command.
    pub fn apply(&mut self, command: Command) -> ApplyOutcome {
        match command {
            Command::OpenSession {
                client_id,
                session_epoch,
            } => self.open_session(client_id, session_epoch),
            Command::Execute { request, mutation } => self.execute(request, mutation),
        }
    }

    /// Returns the balance of an open account.
    #[must_use]
    pub fn account_balance(&self, account_id: AccountId) -> Option<u64> {
        self.accounts.get(&account_id).copied()
    }

    /// Returns a summary whose balance and deposit totals must remain equal.
    #[must_use]
    pub fn summary(&self) -> LedgerSummary {
        LedgerSummary {
            open_accounts: self.accounts.len(),
            total_balance: self.total_balance(),
            successful_deposits: self.successful_deposits,
        }
    }

    /// Returns a canonical view for independent differential assertions.
    #[must_use]
    pub fn view(&self) -> LedgerView {
        LedgerView {
            accounts: self
                .accounts
                .iter()
                .map(|(account_id, balance)| (*account_id, *balance))
                .collect(),
            sessions: self
                .sessions
                .iter()
                .map(|(client_id, session)| SessionView {
                    client_id: *client_id,
                    session_epoch: session.session_epoch,
                    cached: session.cached.as_ref().map(|cached| {
                        (
                            cached.sequence,
                            cached.mutation.clone(),
                            cached.result.clone(),
                        )
                    }),
                })
                .collect(),
            successful_deposits: self.successful_deposits,
        }
    }

    /// Captures all safety-relevant state, including the deduplication cache.
    #[must_use]
    pub fn snapshot(&self) -> LedgerSnapshot {
        LedgerSnapshot {
            accounts: self
                .accounts
                .iter()
                .map(|(account_id, balance)| (*account_id, *balance))
                .collect(),
            sessions: self
                .sessions
                .iter()
                .map(|(client_id, session)| (*client_id, session.clone()))
                .collect(),
            successful_deposits: self.successful_deposits,
        }
    }

    fn open_session(&mut self, client_id: ClientId, requested_epoch: SessionEpoch) -> ApplyOutcome {
        if !self.config.admits_client(client_id) {
            return rejected(RequestRejection::ClientOutOfRange);
        }

        let disposition = match self.sessions.get(&client_id) {
            None => ApplyDisposition::SessionOpened,
            Some(current) => match requested_epoch.cmp(&current.session_epoch) {
                Ordering::Less => {
                    return rejected(RequestRejection::StaleSession {
                        current: current.session_epoch,
                    });
                }
                Ordering::Equal => ApplyDisposition::SessionAlreadyOpen,
                Ordering::Greater => ApplyDisposition::SessionReplaced,
            },
        };

        if disposition != ApplyDisposition::SessionAlreadyOpen {
            self.sessions.insert(
                client_id,
                SessionRecord {
                    session_epoch: requested_epoch,
                    cached: None,
                },
            );
        }
        ApplyOutcome {
            response: LedgerResponse::SessionOpened {
                session_epoch: requested_epoch,
            },
            disposition,
        }
    }

    fn execute(&mut self, request: RequestIdentity, mutation: Mutation) -> ApplyOutcome {
        let Some(session) = self.sessions.get(&request.client_id) else {
            return rejected(if self.config.admits_client(request.client_id) {
                RequestRejection::SessionNotOpen
            } else {
                RequestRejection::ClientOutOfRange
            });
        };

        match request.session_epoch.cmp(&session.session_epoch) {
            Ordering::Less => {
                return rejected(RequestRejection::StaleSession {
                    current: session.session_epoch,
                });
            }
            Ordering::Greater => {
                return rejected(RequestRejection::FutureSession {
                    current: session.session_epoch,
                });
            }
            Ordering::Equal => {}
        }

        if let Some(cached) = &session.cached {
            match request.sequence.cmp(&cached.sequence) {
                Ordering::Less => {
                    return rejected(RequestRejection::StaleSequence {
                        highest: cached.sequence,
                    });
                }
                Ordering::Equal if mutation == cached.mutation => {
                    return ApplyOutcome {
                        response: LedgerResponse::Mutation(cached.result.clone()),
                        disposition: ApplyDisposition::Replayed,
                    };
                }
                Ordering::Equal => return rejected(RequestRejection::ConflictingRetry),
                Ordering::Greater => {
                    let expected = cached
                        .sequence
                        .get()
                        .checked_add(1)
                        .and_then(Sequence::new)
                        .expect("a larger u64 sequence implies a representable successor");
                    if request.sequence != expected {
                        return rejected(RequestRejection::SequenceGap { expected });
                    }
                }
            }
        } else {
            let first = Sequence::new(1).expect("one is nonzero");
            if request.sequence != first {
                return rejected(RequestRejection::SequenceGap { expected: first });
            }
        }

        let result = self.apply_mutation(&mutation);
        self.sessions
            .get_mut(&request.client_id)
            .expect("validated session remains present")
            .cached = Some(CachedCompletion {
            sequence: request.sequence,
            mutation,
            result: result.clone(),
        });
        ApplyOutcome {
            response: LedgerResponse::Mutation(result),
            disposition: ApplyDisposition::Applied,
        }
    }

    fn apply_mutation(&mut self, mutation: &Mutation) -> MutationResult {
        match *mutation {
            Mutation::OpenAccount { account_id } => self.open_account(account_id),
            Mutation::Deposit { account_id, amount } => self.deposit(account_id, amount.get()),
            Mutation::Transfer { from, to, amount } => self.transfer(from, to, amount.get()),
            Mutation::CloseAccount { account_id } => self.close_account(account_id),
        }
    }

    fn open_account(&mut self, account_id: AccountId) -> MutationResult {
        if self.accounts.contains_key(&account_id) {
            return MutationResult::Rejected(BusinessRejection::AccountAlreadyExists);
        }
        if self.accounts.len() == self.config.max_accounts() {
            return MutationResult::Rejected(BusinessRejection::AccountCapacityExceeded);
        }
        self.accounts.insert(account_id, 0);
        MutationResult::AccountOpened
    }

    fn deposit(&mut self, account_id: AccountId, amount: u64) -> MutationResult {
        let Some(balance) = self.accounts.get(&account_id).copied() else {
            return MutationResult::Rejected(BusinessRejection::AccountNotFound);
        };
        let Some(updated_balance) = balance.checked_add(amount) else {
            return MutationResult::Rejected(BusinessRejection::BalanceOverflow);
        };
        let Some(updated_supply) = self.successful_deposits.checked_add(u128::from(amount)) else {
            return MutationResult::Rejected(BusinessRejection::SupplyOverflow);
        };

        self.accounts.insert(account_id, updated_balance);
        self.successful_deposits = updated_supply;
        MutationResult::Deposited {
            balance: updated_balance,
        }
    }

    fn transfer(&mut self, from: AccountId, to: AccountId, amount: u64) -> MutationResult {
        if from == to {
            return MutationResult::Rejected(BusinessRejection::SameAccount);
        }
        let (Some(from_balance), Some(to_balance)) = (
            self.accounts.get(&from).copied(),
            self.accounts.get(&to).copied(),
        ) else {
            return MutationResult::Rejected(BusinessRejection::AccountNotFound);
        };
        let Some(updated_from) = from_balance.checked_sub(amount) else {
            return MutationResult::Rejected(BusinessRejection::InsufficientFunds);
        };
        let Some(updated_to) = to_balance.checked_add(amount) else {
            return MutationResult::Rejected(BusinessRejection::BalanceOverflow);
        };

        self.accounts.insert(from, updated_from);
        self.accounts.insert(to, updated_to);
        MutationResult::Transferred {
            from_balance: updated_from,
            to_balance: updated_to,
        }
    }

    fn close_account(&mut self, account_id: AccountId) -> MutationResult {
        let Some(balance) = self.accounts.get(&account_id).copied() else {
            return MutationResult::Rejected(BusinessRejection::AccountNotFound);
        };
        if balance != 0 {
            return MutationResult::Rejected(BusinessRejection::AccountNotEmpty);
        }
        self.accounts.remove(&account_id);
        MutationResult::AccountClosed
    }

    fn total_balance(&self) -> u128 {
        self.accounts.values().fold(0_u128, |total, balance| {
            total
                .checked_add(u128::from(*balance))
                .expect("valid ledger supply fits in u128")
        })
    }
}

fn rejected(reason: RequestRejection) -> ApplyOutcome {
    ApplyOutcome {
        response: LedgerResponse::Rejected(reason),
        disposition: ApplyDisposition::Rejected,
    }
}
