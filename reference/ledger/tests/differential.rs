mod support;

use rafter_reference_ledger::{
    check_linearizable, AccountId, ApplyDisposition, Command, HistoryEvent, Ledger, LedgerConfig,
    LedgerQuery, LedgerQueryResult, LedgerResponse, Mutation, OperationId, ReferenceLedger,
};
use support::{amount, config, execute, open_session};

/// Client slots the seeded workloads address. The last one is outside the
/// configured bound so out-of-range rejections are reachable.
const ADDRESSABLE_CLIENTS: usize = 3;

/// Accounts the seeded workloads touch. There is one more than the configured
/// account bound so the capacity rejection is reachable.
const ACCOUNTS: [AccountId; 3] = [AccountId::new(1), AccountId::new(2), AccountId::new(3)];

/// Operations one seeded history records.
///
/// Histories stay well inside the checker's bound: the point is to cover many
/// shapes, not to find the largest history the search will still decide.
const OPERATIONS_PER_HISTORY: usize = 14;

/// Operations one seeded history may leave in flight at once.
///
/// Concurrency width is what makes the checker search, and it is also what
/// makes the search expensive, so it is bounded deliberately rather than left
/// to the seed.
const MAX_IN_FLIGHT: usize = 3;

const SEEDS: u64 = 64;

#[test]
fn independent_models_agree_across_small_command_histories() {
    let commands = command_alphabet();
    let bounds = config(2, 2);
    let implementation = Ledger::new(bounds);
    let oracle = ReferenceLedger::new(bounds);
    explore(4, &implementation, &oracle, &commands, &mut Vec::new());
}

#[test]
fn seeded_workloads_with_queries_record_linearizable_histories() {
    let bounds = config(2, 2);
    let mut vocabulary = Vocabulary::default();
    let mut backtracked = false;

    for seed in 0..SEEDS {
        let history = run_workload(seed, bounds);
        let report = check_linearizable(bounds, &history).unwrap_or_else(|error| {
            panic!("seed {seed} recorded an unexplainable history\n{error}")
        });
        assert!(
            report.checked_operations() > 0,
            "seed {seed} recorded no operation the checker had to place"
        );
        // A history whose ordering is forced costs exactly one configuration
        // per operation, so anything above that is the search having tried an
        // ordering and taken it back.
        backtracked |= report.configurations() > report.checked_operations();
        vocabulary.observe(&history);
    }

    // A generator that never emits an outcome leaves that outcome's handling
    // untested while still reporting a green run, so the workload has to prove
    // it reached every part of the vocabulary.
    vocabulary.assert_complete();
    assert!(
        backtracked,
        "no seed produced a history whose ordering the checker had to search for"
    );
}

#[test]
fn a_perturbed_seeded_history_is_rejected() {
    let bounds = config(2, 2);
    let mut perturbed = 0_u64;

    for seed in 0..SEEDS {
        let history = run_workload(seed, bounds);
        let Some(corrupted) = corrupt_one_answered_query(&history) else {
            continue;
        };
        perturbed += 1;
        assert!(
            check_linearizable(bounds, &corrupted).is_err(),
            "seed {seed} accepted a query answer no ordering can produce"
        );
    }

    // Without this the test could pass by never perturbing anything, which
    // would say nothing about whether the recorded histories are tight.
    assert!(
        perturbed >= SEEDS / 2,
        "only {perturbed} seeds carried an answered query to perturb"
    );
}

fn explore(
    remaining: usize,
    implementation: &Ledger,
    oracle: &ReferenceLedger,
    commands: &[Command],
    history: &mut Vec<Command>,
) {
    if remaining == 0 {
        return;
    }

    for command in commands {
        let mut next_implementation = implementation.clone();
        let mut next_oracle = oracle.clone();
        history.push(command.clone());

        let implementation_outcome = next_implementation.apply(command.clone());
        let oracle_outcome = next_oracle.apply(command.clone());
        assert_eq!(
            implementation_outcome, oracle_outcome,
            "outcome disagreement after {history:?}"
        );
        assert_eq!(
            next_implementation.view(),
            next_oracle.view(),
            "state disagreement after {history:?}"
        );
        assert_eq!(
            next_implementation.summary(),
            next_oracle.summary(),
            "summary disagreement after {history:?}"
        );
        let summary = next_implementation.summary();
        assert_eq!(
            summary.total_balance, summary.successful_deposits,
            "supply invariant failed after {history:?}"
        );

        explore(
            remaining - 1,
            &next_implementation,
            &next_oracle,
            commands,
            history,
        );
        history.pop();
    }
}

/// Runs one seeded workload and returns the history a client would have
/// recorded.
///
/// Operations are invoked before they take effect and settle in an order the
/// seed chooses, so the recorded intervals overlap and the linearization order
/// is a genuine permutation of the invocation order rather than a relabelling
/// of it. Every effect still lands inside its own operation's interval, so the
/// history is linearizable by construction and the checker's job is to find the
/// ordering, not to be told it.
fn run_workload(seed: u64, bounds: LedgerConfig) -> Vec<HistoryEvent> {
    let mut rng = Rng::new(seed);
    let mut implementation = Ledger::new(bounds);
    let mut oracle = ReferenceLedger::new(bounds);
    let mut workload = Workload::new();
    let mut recorder = Recorder::default();
    let mut in_flight: Vec<Pending> = Vec::new();
    let mut invoked = 0_usize;

    while invoked < OPERATIONS_PER_HISTORY || !in_flight.is_empty() {
        let room = in_flight.len() < MAX_IN_FLIGHT && invoked < OPERATIONS_PER_HISTORY;
        if room && (in_flight.is_empty() || rng.below(3) != 0) {
            in_flight.push(workload.begin(&mut rng, &mut recorder));
            invoked += 1;
            continue;
        }

        let pending = in_flight.remove(rng.index(in_flight.len()));
        resolve(
            pending,
            seed,
            &mut implementation,
            &mut oracle,
            &mut workload,
            &mut recorder,
        );
    }

    recorder.history
}

/// Resolves one in-flight operation: its effect, its differential comparison,
/// and its terminal event all happen here, at the moment the seed chose.
///
/// Named for the cluster driver's `resolve_proposal` and `resolve_read` rather
/// than for its `settle`, which drives a whole cluster to quiescence.
fn resolve(
    pending: Pending,
    seed: u64,
    implementation: &mut Ledger,
    oracle: &mut ReferenceLedger,
    workload: &mut Workload,
    recorder: &mut Recorder,
) {
    let operation_id = pending.operation_id;
    match pending.action {
        // Neither of these reaches the state machine, so nothing applies them.
        // They differ only in what the client could prove about the command.
        Action::Mutate {
            fate: Fate::Refused,
            ..
        } => recorder.not_committed(operation_id),
        Action::Mutate {
            fate: Fate::LostBeforeApplying,
            ..
        } => recorder.unknown(operation_id),
        Action::Mutate { command, fate } => {
            let observed = implementation.apply(command.clone());
            let specified = oracle.apply(command.clone());
            assert_eq!(
                observed, specified,
                "seed {seed}: models disagreed on {command:?}"
            );
            assert_eq!(
                implementation.view(),
                oracle.view(),
                "seed {seed}: models diverged after {command:?}"
            );
            workload.observe(&command, observed.disposition);
            match fate {
                Fate::Completed => recorder.completed(operation_id, observed.response),
                Fate::LostAfterApplying => recorder.unknown(operation_id),
                Fate::LostBeforeApplying | Fate::Refused => {
                    unreachable!("commands that never ran settle above")
                }
            }
        }
        Action::Read {
            answered: false, ..
        } => recorder.abandoned(operation_id),
        Action::Read {
            query,
            answered: true,
        } => {
            let observed = implementation.query(query);
            assert_eq!(
                observed,
                oracle.query(query),
                "seed {seed}: readers disagreed on {query:?}"
            );
            recorder.answered(operation_id, observed);
        }
    }
}

/// Replaces the first answered account query with a balance the workload's
/// bounded deposits can never reach.
///
/// Returns `None` when a history answered no account query, which leaves
/// nothing to perturb.
fn corrupt_one_answered_query(history: &[HistoryEvent]) -> Option<Vec<HistoryEvent>> {
    let mut corrupted = history.to_vec();
    let target = corrupted.iter_mut().find(|event| {
        matches!(
            event,
            HistoryEvent::QueryCompleted {
                result: LedgerQueryResult::Account { .. },
                ..
            }
        )
    })?;
    let HistoryEvent::QueryCompleted { result, .. } = target else {
        unreachable!("the search above matched this variant");
    };
    let LedgerQueryResult::Account { account_id, .. } = *result else {
        unreachable!("the search above matched this variant");
    };
    *result = LedgerQueryResult::Account {
        account_id,
        balance: Some(u64::MAX),
    };
    Some(corrupted)
}

fn command_alphabet() -> Vec<Command> {
    let one = AccountId::new(1);
    let two = AccountId::new(2);
    vec![
        open_session(0, 1),
        open_session(0, 2),
        open_session(1, 1),
        execute(0, 1, 1, Mutation::OpenAccount { account_id: one }),
        execute(0, 1, 1, Mutation::OpenAccount { account_id: two }),
        execute(
            0,
            1,
            2,
            Mutation::Deposit {
                account_id: one,
                amount: amount(2),
            },
        ),
        execute(0, 1, 3, Mutation::OpenAccount { account_id: two }),
        execute(
            0,
            1,
            4,
            Mutation::Transfer {
                from: one,
                to: two,
                amount: amount(1),
            },
        ),
        execute(0, 1, 5, Mutation::CloseAccount { account_id: one }),
        execute(1, 1, 1, Mutation::OpenAccount { account_id: two }),
        execute(
            1,
            1,
            2,
            Mutation::Deposit {
                account_id: two,
                amount: amount(u64::MAX),
            },
        ),
        execute(
            1,
            1,
            3,
            Mutation::Deposit {
                account_id: two,
                amount: amount(1),
            },
        ),
    ]
}

/// One operation the workload invoked and has not settled.
struct Pending {
    operation_id: OperationId,
    action: Action,
}

enum Action {
    Mutate { command: Command, fate: Fate },
    Read { query: LedgerQuery, answered: bool },
}

/// What the client ends up observing for one mutation, and whether the command
/// actually ran.
///
/// The two lost fates are the reason an unknown outcome cannot be read one way
/// and left there: they are indistinguishable to the client and opposite in the
/// state machine, so a later query is the only thing that decides which one
/// happened.
enum Fate {
    /// The client saw the replicated response.
    Completed,
    /// The client lost the answer to a command that did take effect.
    LostAfterApplying,
    /// The client lost the answer to a command that never took effect.
    LostBeforeApplying,
    /// The command was refused before replication and provably never ran.
    Refused,
}

/// Records events exactly as the cluster driver does.
#[derive(Default)]
struct Recorder {
    history: Vec<HistoryEvent>,
    next_operation_id: u64,
}

impl Recorder {
    fn invoke(&mut self, command: Command) -> OperationId {
        let operation_id = self.allocate();
        self.history.push(HistoryEvent::Invoked {
            operation_id,
            command,
        });
        operation_id
    }

    fn invoke_query(&mut self, query: LedgerQuery) -> OperationId {
        let operation_id = self.allocate();
        self.history.push(HistoryEvent::QueryInvoked {
            operation_id,
            query,
        });
        operation_id
    }

    fn completed(&mut self, operation_id: OperationId, response: LedgerResponse) {
        self.history.push(HistoryEvent::Completed {
            operation_id,
            response,
        });
    }

    fn unknown(&mut self, operation_id: OperationId) {
        self.history.push(HistoryEvent::Unknown { operation_id });
    }

    fn not_committed(&mut self, operation_id: OperationId) {
        self.history
            .push(HistoryEvent::NotCommitted { operation_id });
    }

    fn answered(&mut self, operation_id: OperationId, result: LedgerQueryResult) {
        self.history.push(HistoryEvent::QueryCompleted {
            operation_id,
            result,
        });
    }

    fn abandoned(&mut self, operation_id: OperationId) {
        self.history
            .push(HistoryEvent::QueryAbandoned { operation_id });
    }

    fn allocate(&mut self) -> OperationId {
        self.next_operation_id += 1;
        OperationId::new(self.next_operation_id)
    }
}

/// Tracks what the workload believes each slot's session looks like so most
/// commands are admissible, then deliberately perturbs that belief.
///
/// The belief is corrected only when an operation settles, so two operations in
/// flight for one slot naturally collide on a sequence — which is exactly the
/// retry-under-one-identity traffic the contract cares about.
struct Workload {
    epochs: [u64; ADDRESSABLE_CLIENTS],
    next_sequences: [u64; ADDRESSABLE_CLIENTS],
}

impl Workload {
    fn new() -> Self {
        Self {
            epochs: [1; ADDRESSABLE_CLIENTS],
            next_sequences: [1; ADDRESSABLE_CLIENTS],
        }
    }

    fn begin(&self, rng: &mut Rng, recorder: &mut Recorder) -> Pending {
        if rng.below(3) == 0 {
            let query = random_query(rng);
            let answered = rng.below(6) != 0;
            return Pending {
                operation_id: recorder.invoke_query(query),
                action: Action::Read { query, answered },
            };
        }

        let command = self.random_command(rng);
        let fate = match rng.below(10) {
            0 => Fate::Refused,
            1 => Fate::LostAfterApplying,
            2 => Fate::LostBeforeApplying,
            _ => Fate::Completed,
        };
        Pending {
            operation_id: recorder.invoke(command.clone()),
            action: Action::Mutate { command, fate },
        }
    }

    fn random_command(&self, rng: &mut Rng) -> Command {
        let slot = rng.index(ADDRESSABLE_CLIENTS);
        if rng.below(100) < 12 {
            return open_session(client_id(slot), 1 + rng.below(3));
        }

        let session_epoch = match rng.below(10) {
            0 => self.epochs[slot].saturating_sub(1).max(1),
            1 => self.epochs[slot] + 1,
            _ => self.epochs[slot],
        };
        let request_sequence = match rng.below(10) {
            0 => self.next_sequences[slot].saturating_sub(1).max(1),
            1 => self.next_sequences[slot] + 1,
            _ => self.next_sequences[slot],
        };
        execute(
            client_id(slot),
            session_epoch,
            request_sequence,
            random_mutation(rng),
        )
    }

    fn observe(&mut self, command: &Command, disposition: ApplyDisposition) {
        match command {
            Command::OpenSession {
                client_id,
                session_epoch,
            } => {
                if matches!(
                    disposition,
                    ApplyDisposition::SessionOpened | ApplyDisposition::SessionReplaced
                ) {
                    let slot = slot_index(client_id.get());
                    self.epochs[slot] = session_epoch.get();
                    self.next_sequences[slot] = 1;
                }
            }
            Command::Execute { request, .. } => {
                if disposition == ApplyDisposition::Applied {
                    let slot = slot_index(request.client_id.get());
                    self.epochs[slot] = request.session_epoch.get();
                    self.next_sequences[slot] = request.sequence.get() + 1;
                }
            }
        }
    }
}

fn random_query(rng: &mut Rng) -> LedgerQuery {
    if rng.below(4) == 0 {
        LedgerQuery::GetLedgerSummary
    } else {
        LedgerQuery::GetAccount {
            account_id: ACCOUNTS[rng.index(ACCOUNTS.len())],
        }
    }
}

fn random_mutation(rng: &mut Rng) -> Mutation {
    let account_id = ACCOUNTS[rng.index(ACCOUNTS.len())];
    match rng.below(10) {
        0..=2 => Mutation::OpenAccount { account_id },
        3..=6 => Mutation::Deposit {
            account_id,
            amount: amount(1 + rng.below(100)),
        },
        7..=8 => Mutation::Transfer {
            from: account_id,
            to: ACCOUNTS[rng.index(ACCOUNTS.len())],
            amount: amount(1 + rng.below(50)),
        },
        _ => Mutation::CloseAccount { account_id },
    }
}

/// Counts what the seeded workloads actually produced across every seed.
#[derive(Default)]
struct Vocabulary {
    completed: usize,
    unknown: usize,
    not_committed: usize,
    answered: usize,
    abandoned: usize,
    overlaps: usize,
}

impl Vocabulary {
    fn observe(&mut self, history: &[HistoryEvent]) {
        let mut in_flight = 0_usize;
        for event in history {
            match event {
                HistoryEvent::Invoked { .. } | HistoryEvent::QueryInvoked { .. } => {
                    if in_flight > 0 {
                        self.overlaps += 1;
                    }
                    in_flight += 1;
                    continue;
                }
                HistoryEvent::Completed { .. } => self.completed += 1,
                HistoryEvent::Unknown { .. } => self.unknown += 1,
                HistoryEvent::NotCommitted { .. } => self.not_committed += 1,
                HistoryEvent::QueryCompleted { .. } => self.answered += 1,
                HistoryEvent::QueryAbandoned { .. } => self.abandoned += 1,
            }
            in_flight -= 1;
        }
    }

    fn assert_complete(&self) {
        assert!(self.completed > 0, "no seed recorded a completed mutation");
        assert!(self.unknown > 0, "no seed recorded an unknown outcome");
        assert!(self.not_committed > 0, "no seed recorded a refused command");
        assert!(self.answered > 0, "no seed recorded an answered query");
        assert!(self.abandoned > 0, "no seed recorded an unanswered query");
        assert!(self.overlaps > 0, "no seed recorded overlapping operations");
    }
}

fn client_id(slot: usize) -> u32 {
    u32::try_from(slot).expect("addressable slots fit in u32")
}

fn slot_index(client: u32) -> usize {
    usize::try_from(client).expect("addressable slots fit in usize")
}

/// `SplitMix64`. Reproducing a failure needs only the printed seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x243f_6a88_85a3_08d3)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut word = self.0;
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        word ^ (word >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }

    fn index(&mut self, bound: usize) -> usize {
        let bound = u64::try_from(bound).expect("test bounds fit in u64");
        usize::try_from(self.below(bound)).expect("a value below a usize bound fits in usize")
    }
}
