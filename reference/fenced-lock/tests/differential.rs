mod support;

use std::fmt;

use rafter_reference_fenced_lock::{
    ApplyDisposition, Command, LockConfig, LockService, Operation, ReferenceLockService,
    RequestFingerprint,
};
use support::{
    acquire, config, expire_through, open_session, release, renew, resource, submit,
    submit_with_fingerprint,
};

/// Names the workloads may contend for. There is one more name than the
/// configured resource bound so the tracked-resource limit is reachable.
const NAMES: [&str; 3] = ["alpha", "beta", "gamma"];

/// Slots the workloads may address, including one outside the configured range.
const ADDRESSABLE_CLIENTS: usize = 4;

#[test]
fn independent_models_agree_across_exhaustive_short_histories() {
    let commands = command_alphabet();
    let bounds = config(2, 2);
    let implementation = LockService::new(bounds);
    let oracle = ReferenceLockService::new(bounds);
    explore(4, &implementation, &oracle, &commands, &mut Vec::new());
}

#[test]
fn independent_models_agree_across_seeded_random_workloads() {
    for seed in 0..48_u64 {
        run_workload(seed, config(3, 2));
    }
}

fn explore(
    remaining: usize,
    implementation: &LockService,
    oracle: &ReferenceLockService,
    commands: &[Command],
    history: &mut Vec<Command>,
) {
    if remaining == 0 {
        return;
    }

    for command in commands {
        let mut next_implementation = implementation.clone();
        let mut next_oracle = oracle.clone();
        history.push(*command);

        let implementation_outcome = next_implementation.apply(*command);
        let oracle_outcome = next_oracle.apply(*command);
        assert_eq!(
            implementation_outcome, oracle_outcome,
            "outcome disagreement after {history:?}"
        );
        assert_agreement(&next_implementation, &next_oracle, &*history);

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

fn run_workload(seed: u64, bounds: LockConfig) {
    let mut implementation = LockService::new(bounds);
    let mut oracle = ReferenceLockService::new(bounds);
    let mut rng = Rng::new(seed);
    let mut driver = Driver::new();

    for step in 0..160_usize {
        let command = driver.next_command(&mut rng, &implementation);
        let context = (seed, step, command);

        let implementation_outcome = implementation.apply(command);
        let oracle_outcome = oracle.apply(command);
        assert_eq!(
            implementation_outcome, oracle_outcome,
            "outcome disagreement at seed {seed} step {step} on {command:?}"
        );
        assert_agreement(&implementation, &oracle, &context);

        driver.observe(command, implementation_outcome.disposition);
    }

    let restored = LockService::from_snapshot(bounds, implementation.snapshot())
        .unwrap_or_else(|error| panic!("seed {seed} produced an invalid snapshot: {error:?}"));
    assert_eq!(
        restored.view(),
        implementation.view(),
        "seed {seed} diverged across a snapshot round trip"
    );
    assert_eq!(restored.summary(), implementation.summary());
}

/// Compares every observable surface both models expose. `context` is
/// formatted only when an assertion fails, so exhaustive exploration pays
/// nothing for it.
fn assert_agreement(
    implementation: &LockService,
    oracle: &ReferenceLockService,
    context: &dyn fmt::Debug,
) {
    let view = implementation.view();
    assert_eq!(view, oracle.view(), "state disagreement after {context:?}");
    assert_eq!(
        implementation.summary(),
        oracle.summary(),
        "summary disagreement after {context:?}"
    );
    assert_eq!(
        implementation.logical_time(),
        oracle.logical_time(),
        "logical time disagreement after {context:?}"
    );
    for name in NAMES {
        let named = resource(name);
        assert_eq!(
            implementation.status(named),
            oracle.status(named),
            "status disagreement for {name} after {context:?}"
        );
    }

    for tracked in &view.resources {
        if let Some(holder) = tracked.holder {
            assert!(
                holder.expiry > view.logical_time,
                "{:?} is held past logical time after {context:?}",
                tracked.resource
            );
        }
        assert!(
            tracked
                .holder
                .is_none_or(|holder| holder.token == tracked.token_floor),
            "{:?} holds a token behind its high-water mark after {context:?}",
            tracked.resource
        );
    }
}

fn command_alphabet() -> Vec<Command> {
    vec![
        open_session(0, 1),
        open_session(0, 2),
        open_session(1, 1),
        submit(0, 1, 1, acquire("alpha", 3)),
        submit(0, 1, 1, acquire("beta", 3)),
        submit(0, 1, 2, renew("alpha", 1, 5)),
        submit(0, 1, 2, release("alpha", 1)),
        submit(0, 1, 3, expire_through(3)),
        submit(1, 1, 1, acquire("alpha", 2)),
        submit(1, 1, 1, expire_through(1)),
        submit(1, 1, 2, acquire("gamma", 2)),
        submit(1, 1, 2, release("alpha", 2)),
    ]
}

/// Tracks what the workload believes each slot's session looks like so that
/// most commands are admissible, then deliberately perturbs that belief. The
/// driver never reads either model's session state; it only observes the
/// dispositions both models already agreed on.
struct Driver {
    epochs: [u64; ADDRESSABLE_CLIENTS],
    next_sequences: [u64; ADDRESSABLE_CLIENTS],
    last_applied: [Option<Command>; ADDRESSABLE_CLIENTS],
}

impl Driver {
    fn new() -> Self {
        Self {
            epochs: [1; ADDRESSABLE_CLIENTS],
            next_sequences: [1; ADDRESSABLE_CLIENTS],
            last_applied: [None; ADDRESSABLE_CLIENTS],
        }
    }

    fn next_command(&self, rng: &mut Rng, implementation: &LockService) -> Command {
        let slot = rng.index(ADDRESSABLE_CLIENTS);
        if rng.below(100) < 12 {
            return open_session(client_id(slot), 1 + rng.below(3));
        }

        // Resend the slot's last applied request verbatim, the way a client
        // retries after an unknown outcome.
        if rng.below(8) == 0 {
            if let Some(resent) = self.last_applied[slot] {
                return resent;
            }
        }

        let epoch = match rng.below(10) {
            0 => self.epochs[slot].saturating_sub(1).max(1),
            1 => self.epochs[slot] + 1,
            _ => self.epochs[slot],
        };
        let sequence = match rng.below(10) {
            0 => self.next_sequences[slot].saturating_sub(1).max(1),
            1 => self.next_sequences[slot] + 1,
            _ => self.next_sequences[slot],
        };
        let operation = random_operation(rng, implementation);

        if rng.below(24) == 0 {
            let unrelated = RequestFingerprint::of(&expire_through(1 + rng.below(64)));
            submit_with_fingerprint(client_id(slot), epoch, sequence, unrelated, operation)
        } else {
            submit(client_id(slot), epoch, sequence, operation)
        }
    }

    fn observe(&mut self, command: Command, disposition: ApplyDisposition) {
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
                    self.last_applied[slot] = None;
                }
            }
            Command::Submit { request, .. } => {
                if disposition == ApplyDisposition::Applied {
                    let slot = slot_index(request.client_id.get());
                    self.epochs[slot] = request.session_epoch.get();
                    self.next_sequences[slot] = request.sequence.get() + 1;
                    self.last_applied[slot] = Some(command);
                }
            }
        }
    }
}

fn random_operation(rng: &mut Rng, implementation: &LockService) -> Operation {
    let name = NAMES[rng.index(NAMES.len())];
    match rng.below(10) {
        0..=3 => acquire(name, 1 + rng.below(4)),
        4..=5 => renew(
            name,
            presented_token(rng, implementation, name),
            1 + rng.below(4),
        ),
        6..=7 => release(name, presented_token(rng, implementation, name)),
        _ => {
            let current = implementation.logical_time().get();
            let horizon = if rng.below(6) == 0 {
                current.saturating_sub(rng.below(2))
            } else {
                current + 1 + rng.below(3)
            };
            expire_through(horizon)
        }
    }
}

/// Usually presents the token a holder actually has, so renewals and releases
/// reach their interesting paths, and sometimes presents a wrong one.
fn presented_token(rng: &mut Rng, implementation: &LockService, name: &str) -> u64 {
    let held = implementation
        .status(resource(name))
        .holder
        .map_or(1, |holder| holder.token.get());
    if rng.below(5) == 0 {
        held.saturating_add(1)
    } else {
        held
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
