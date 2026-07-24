use std::num::NonZeroU64;

/// Account identifier in one ledger.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AccountId(u64);

impl AccountId {
    /// Creates an account identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Index of one bounded client-session slot.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ClientId(u32);

impl ClientId {
    /// Creates a client slot identifier.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the numeric slot.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Nonzero session generation for one client slot.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionEpoch(NonZeroU64);

impl SessionEpoch {
    /// Creates a session epoch, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric epoch.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Nonzero monotonically increasing request sequence within one session.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Sequence(NonZeroU64);

impl Sequence {
    /// Creates a request sequence, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric sequence.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Nonzero ledger amount.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Amount(NonZeroU64);

impl Amount {
    /// Creates an amount, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric amount.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Fixed resource limits for one ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerConfig {
    max_clients: u32,
    max_accounts: usize,
}

impl LedgerConfig {
    /// Creates a bounded ledger configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when either bound is zero.
    pub const fn new(max_clients: u32, max_accounts: usize) -> Result<Self, LedgerConfigError> {
        if max_clients == 0 {
            return Err(LedgerConfigError::ZeroClients);
        }
        if max_accounts == 0 {
            return Err(LedgerConfigError::ZeroAccounts);
        }
        Ok(Self {
            max_clients,
            max_accounts,
        })
    }

    /// Maximum number of addressable client slots.
    #[must_use]
    pub const fn max_clients(self) -> u32 {
        self.max_clients
    }

    /// Maximum number of simultaneously open accounts.
    #[must_use]
    pub const fn max_accounts(self) -> usize {
        self.max_accounts
    }

    pub(crate) const fn admits_client(self, client_id: ClientId) -> bool {
        client_id.get() < self.max_clients
    }
}

/// Invalid ledger configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerConfigError {
    /// No client slot could ever open.
    ZeroClients,
    /// No account could ever open.
    ZeroAccounts,
}

/// Identity of one mutation within a client session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestIdentity {
    /// Bounded client slot.
    pub client_id: ClientId,
    /// Exact active session generation.
    pub session_epoch: SessionEpoch,
    /// Monotone sequence within the session.
    pub sequence: Sequence,
}

/// Deterministic mutation applied by the ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    /// Opens a new zero-balance account.
    OpenAccount { account_id: AccountId },
    /// Adds external funds to an existing account.
    Deposit {
        account_id: AccountId,
        amount: Amount,
    },
    /// Moves funds atomically between two existing accounts.
    Transfer {
        from: AccountId,
        to: AccountId,
        amount: Amount,
    },
    /// Removes an existing zero-balance account.
    CloseAccount { account_id: AccountId },
}

/// Replicated ledger command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    /// Opens or advances one bounded client session.
    OpenSession {
        client_id: ClientId,
        session_epoch: SessionEpoch,
    },
    /// Executes a mutation under a client request identity.
    Execute {
        request: RequestIdentity,
        mutation: Mutation,
    },
}

/// Deterministic result of an admitted mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationResult {
    /// A zero-balance account was opened.
    AccountOpened,
    /// External funds were deposited.
    Deposited { balance: u64 },
    /// Funds moved between accounts.
    Transferred { from_balance: u64, to_balance: u64 },
    /// A zero-balance account was closed.
    AccountClosed,
    /// The mutation was admitted under the request identity but rejected by
    /// ledger business rules.
    Rejected(BusinessRejection),
}

/// Ledger-level deterministic rejection that consumes and caches its sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BusinessRejection {
    /// The account is already open.
    AccountAlreadyExists,
    /// The configured open-account bound was reached.
    AccountCapacityExceeded,
    /// A referenced account is absent.
    AccountNotFound,
    /// A transfer used the same source and destination.
    SameAccount,
    /// The source balance is too small.
    InsufficientFunds,
    /// A destination or deposit balance would exceed `u64`.
    BalanceOverflow,
    /// The cumulative external supply would exceed `u128`.
    SupplyOverflow,
    /// A nonzero account cannot close.
    AccountNotEmpty,
}

/// Request/session rejection that does not consume a sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestRejection {
    /// The client ID is outside the configured slot range.
    ClientOutOfRange,
    /// No session has been opened for the client slot.
    SessionNotOpen,
    /// The command names an older session generation.
    StaleSession { current: SessionEpoch },
    /// The command names a newer generation that must be opened first.
    FutureSession { current: SessionEpoch },
    /// The sequence is older than the cached completion.
    StaleSequence { highest: Sequence },
    /// The sequence skipped the required next value.
    SequenceGap { expected: Sequence },
    /// The highest completed identity was reused with another mutation.
    ConflictingRetry,
}

/// Stable client-visible response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerResponse {
    /// The requested session epoch is open.
    SessionOpened { session_epoch: SessionEpoch },
    /// A newly executed or exactly replayed mutation result.
    Mutation(MutationResult),
    /// The request was rejected before mutation admission.
    Rejected(RequestRejection),
}

/// Internal classification useful to tests and adapters without changing the
/// stable client response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyDisposition {
    /// A previously unused client slot opened.
    SessionOpened,
    /// A greater session epoch replaced the old one.
    SessionReplaced,
    /// The requested epoch was already open.
    SessionAlreadyOpen,
    /// A new request sequence executed and was cached.
    Applied,
    /// The highest completed request was replayed exactly.
    Replayed,
    /// The command failed request/session admission.
    Rejected,
}

/// Result of applying one replicated command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyOutcome {
    /// Stable response returned to the client.
    pub response: LedgerResponse,
    /// Whether and how the command affected session/application state.
    pub disposition: ApplyDisposition,
}

/// Query result summarizing one ledger state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerSummary {
    /// Number of currently open accounts.
    pub open_accounts: usize,
    /// Sum of all open account balances.
    pub total_balance: u128,
    /// Sum of successful external deposits.
    pub successful_deposits: u128,
}

/// Public deterministic inspection of one active client session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionView {
    /// Client slot that owns the session.
    pub client_id: ClientId,
    /// Current session generation.
    pub session_epoch: SessionEpoch,
    /// Highest completed request and its exact cached data, when present.
    pub cached: Option<(Sequence, Mutation, MutationResult)>,
}

/// Canonical deterministic state view shared only for differential assertions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerView {
    /// Sorted account balances.
    pub accounts: Vec<(AccountId, u64)>,
    /// Sorted active client sessions.
    pub sessions: Vec<SessionView>,
    /// Cumulative successful external deposits.
    pub successful_deposits: u128,
}
