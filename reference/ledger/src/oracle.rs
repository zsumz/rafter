use std::cmp::Ordering;

use crate::{
    AccountId, ApplyDisposition, ApplyOutcome, BusinessRejection, ClientId, Command, LedgerConfig,
    LedgerQuery, LedgerQueryResult, LedgerResponse, LedgerSummary, LedgerView, Mutation,
    MutationResult, RequestIdentity, RequestRejection, Sequence, SessionEpoch, SessionView,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct OracleSession {
    client_id: ClientId,
    session_epoch: SessionEpoch,
    completed: Option<(Sequence, Mutation, MutationResult)>,
}

/// Structurally independent executable specification for the ledger.
///
/// This oracle intentionally uses linear collections and separate transition
/// code. It shares vocabulary with [`crate::Ledger`], never implementation
/// helpers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceLedger {
    config: LedgerConfig,
    accounts: Vec<(AccountId, u64)>,
    sessions: Vec<OracleSession>,
    successful_deposits: u128,
}

impl ReferenceLedger {
    /// Creates an empty reference ledger.
    #[must_use]
    pub fn new(config: LedgerConfig) -> Self {
        Self {
            config,
            accounts: Vec::new(),
            sessions: Vec::new(),
            successful_deposits: 0,
        }
    }

    /// Applies one command through the independent transition system.
    pub fn apply(&mut self, command: Command) -> ApplyOutcome {
        match command {
            Command::OpenSession {
                client_id,
                session_epoch,
            } => self.apply_open_session(client_id, session_epoch),
            Command::Execute { request, mutation } => {
                self.apply_session_mutation(request, mutation)
            }
        }
    }

    /// Returns the balance of an open account.
    #[must_use]
    pub fn account_balance(&self, account_id: AccountId) -> Option<u64> {
        self.accounts
            .iter()
            .find_map(|(candidate, balance)| (*candidate == account_id).then_some(*balance))
    }

    /// Returns the oracle's aggregate ledger summary.
    #[must_use]
    pub fn summary(&self) -> LedgerSummary {
        let total_balance = self.accounts.iter().fold(0_u128, |total, (_, balance)| {
            total.saturating_add(u128::from(*balance))
        });
        LedgerSummary {
            open_accounts: self.accounts.len(),
            total_balance,
            successful_deposits: self.successful_deposits,
        }
    }

    /// Answers one linearizable query from the oracle's own state.
    ///
    /// Queries never change ledger state, so this is the read half of the
    /// sequential specification the history checker linearizes against. The
    /// adapter answers the same query from the implementation model; neither
    /// path calls the other.
    #[must_use]
    pub fn query(&self, query: LedgerQuery) -> LedgerQueryResult {
        match query {
            LedgerQuery::GetAccount { account_id } => LedgerQueryResult::Account {
                account_id,
                balance: self.account_balance(account_id),
            },
            LedgerQuery::GetLedgerSummary => LedgerQueryResult::Summary(self.summary()),
        }
    }

    /// Returns a canonical state view for differential assertions.
    #[must_use]
    pub fn view(&self) -> LedgerView {
        let mut accounts = self.accounts.clone();
        accounts.sort_by_key(|(account_id, _)| *account_id);

        let mut sessions = self
            .sessions
            .iter()
            .map(|session| SessionView {
                client_id: session.client_id,
                session_epoch: session.session_epoch,
                cached: session.completed.clone(),
            })
            .collect::<Vec<_>>();
        sessions.sort_by_key(|session| session.client_id);

        LedgerView {
            accounts,
            sessions,
            successful_deposits: self.successful_deposits,
        }
    }

    fn apply_open_session(
        &mut self,
        client_id: ClientId,
        requested_epoch: SessionEpoch,
    ) -> ApplyOutcome {
        if client_id.get() >= self.config.max_clients() {
            return oracle_rejection(RequestRejection::ClientOutOfRange);
        }

        let existing = self
            .sessions
            .iter()
            .position(|session| session.client_id == client_id);
        let disposition = if let Some(position) = existing {
            let current_epoch = self.sessions[position].session_epoch;
            match requested_epoch.cmp(&current_epoch) {
                Ordering::Less => {
                    return oracle_rejection(RequestRejection::StaleSession {
                        current: current_epoch,
                    });
                }
                Ordering::Equal => ApplyDisposition::SessionAlreadyOpen,
                Ordering::Greater => {
                    self.sessions[position] = OracleSession {
                        client_id,
                        session_epoch: requested_epoch,
                        completed: None,
                    };
                    ApplyDisposition::SessionReplaced
                }
            }
        } else {
            self.sessions.push(OracleSession {
                client_id,
                session_epoch: requested_epoch,
                completed: None,
            });
            ApplyDisposition::SessionOpened
        };

        ApplyOutcome {
            response: LedgerResponse::SessionOpened {
                session_epoch: requested_epoch,
            },
            disposition,
        }
    }

    fn apply_session_mutation(
        &mut self,
        request: RequestIdentity,
        mutation: Mutation,
    ) -> ApplyOutcome {
        if request.client_id.get() >= self.config.max_clients() {
            return oracle_rejection(RequestRejection::ClientOutOfRange);
        }
        let Some(session_position) = self
            .sessions
            .iter()
            .position(|session| session.client_id == request.client_id)
        else {
            return oracle_rejection(RequestRejection::SessionNotOpen);
        };

        let session_epoch = self.sessions[session_position].session_epoch;
        if request.session_epoch < session_epoch {
            return oracle_rejection(RequestRejection::StaleSession {
                current: session_epoch,
            });
        }
        if request.session_epoch > session_epoch {
            return oracle_rejection(RequestRejection::FutureSession {
                current: session_epoch,
            });
        }

        if let Some((completed_sequence, completed_mutation, completed_result)) =
            self.sessions[session_position].completed.clone()
        {
            if request.sequence < completed_sequence {
                return oracle_rejection(RequestRejection::StaleSequence {
                    highest: completed_sequence,
                });
            }
            if request.sequence == completed_sequence {
                return if mutation == completed_mutation {
                    ApplyOutcome {
                        response: LedgerResponse::Mutation(completed_result),
                        disposition: ApplyDisposition::Replayed,
                    }
                } else {
                    oracle_rejection(RequestRejection::ConflictingRetry)
                };
            }
            let expected = Sequence::new(
                completed_sequence
                    .get()
                    .checked_add(1)
                    .expect("a greater sequence implies a successor"),
            )
            .expect("the successor is nonzero");
            if request.sequence != expected {
                return oracle_rejection(RequestRejection::SequenceGap { expected });
            }
        } else {
            let expected = Sequence::new(1).expect("one is nonzero");
            if request.sequence != expected {
                return oracle_rejection(RequestRejection::SequenceGap { expected });
            }
        }

        let result = self.run_mutation(&mutation);
        self.sessions[session_position].completed =
            Some((request.sequence, mutation, result.clone()));
        ApplyOutcome {
            response: LedgerResponse::Mutation(result),
            disposition: ApplyDisposition::Applied,
        }
    }

    fn run_mutation(&mut self, mutation: &Mutation) -> MutationResult {
        match *mutation {
            Mutation::OpenAccount { account_id } => {
                if self
                    .accounts
                    .iter()
                    .any(|(candidate, _)| *candidate == account_id)
                {
                    return MutationResult::Rejected(BusinessRejection::AccountAlreadyExists);
                }
                if self.accounts.len() >= self.config.max_accounts() {
                    return MutationResult::Rejected(BusinessRejection::AccountCapacityExceeded);
                }
                self.accounts.push((account_id, 0));
                MutationResult::AccountOpened
            }
            Mutation::Deposit { account_id, amount } => {
                let Some(position) = self
                    .accounts
                    .iter()
                    .position(|(candidate, _)| *candidate == account_id)
                else {
                    return MutationResult::Rejected(BusinessRejection::AccountNotFound);
                };
                let Some(new_balance) = self.accounts[position].1.checked_add(amount.get()) else {
                    return MutationResult::Rejected(BusinessRejection::BalanceOverflow);
                };
                let Some(new_supply) = self
                    .successful_deposits
                    .checked_add(u128::from(amount.get()))
                else {
                    return MutationResult::Rejected(BusinessRejection::SupplyOverflow);
                };
                self.accounts[position].1 = new_balance;
                self.successful_deposits = new_supply;
                MutationResult::Deposited {
                    balance: new_balance,
                }
            }
            Mutation::Transfer { from, to, amount } => {
                if from == to {
                    return MutationResult::Rejected(BusinessRejection::SameAccount);
                }
                let source = self
                    .accounts
                    .iter()
                    .position(|(candidate, _)| *candidate == from);
                let destination = self
                    .accounts
                    .iter()
                    .position(|(candidate, _)| *candidate == to);
                let (Some(source), Some(destination)) = (source, destination) else {
                    return MutationResult::Rejected(BusinessRejection::AccountNotFound);
                };
                let Some(source_balance) = self.accounts[source].1.checked_sub(amount.get()) else {
                    return MutationResult::Rejected(BusinessRejection::InsufficientFunds);
                };
                let Some(destination_balance) =
                    self.accounts[destination].1.checked_add(amount.get())
                else {
                    return MutationResult::Rejected(BusinessRejection::BalanceOverflow);
                };
                self.accounts[source].1 = source_balance;
                self.accounts[destination].1 = destination_balance;
                MutationResult::Transferred {
                    from_balance: source_balance,
                    to_balance: destination_balance,
                }
            }
            Mutation::CloseAccount { account_id } => {
                let Some(position) = self
                    .accounts
                    .iter()
                    .position(|(candidate, _)| *candidate == account_id)
                else {
                    return MutationResult::Rejected(BusinessRejection::AccountNotFound);
                };
                if self.accounts[position].1 != 0 {
                    return MutationResult::Rejected(BusinessRejection::AccountNotEmpty);
                }
                self.accounts.remove(position);
                MutationResult::AccountClosed
            }
        }
    }
}

fn oracle_rejection(reason: RequestRejection) -> ApplyOutcome {
    ApplyOutcome {
        response: LedgerResponse::Rejected(reason),
        disposition: ApplyDisposition::Rejected,
    }
}
