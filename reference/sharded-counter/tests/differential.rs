mod support;

use std::time::Instant;

use rafter_reference_sharded_counter::{
    AdmissionOutcome, ClientId, CounterCommand, GroupId, GroupIncarnation, LifecycleRequest,
    ReadinessSignal, SchedulerConfig, SessionEpoch, SystemClass, Work,
};
use support::{
    add, client, config, counter, counter_with_fingerprint, create, epoch, faulty, first, group,
    read, system, Recorder,
};

/// Groups the large workload drives. `docs/reference-consumers.md` asks the
/// deterministic workload to model thousands of independent groups; this is
/// that number, and the model allocates a slot only when one is created, so the
/// cost is in the groups that exist rather than in the bound that admits them.
const MANY_GROUPS: u32 = 3_000;

// ---------------------------------------------------------------------------
// Exhaustive short histories
// ---------------------------------------------------------------------------

/// Every ordering of a small alphabet, applied to two already-serving groups.
/// Short histories reach the awkward corners — a retry racing a drain, a stall
/// arriving mid-plan — that a random walk visits rarely.
#[test]
fn independent_models_agree_across_exhaustive_short_histories() {
    let bounds = config(3, 1, 2, 4, 6);
    let mut seed = Recorder::new(bounds);
    for id in [group(0), group(1)] {
        seed.open_group(id, 2);
        seed.open_session(id, first(), client(0), epoch(1));
    }
    explore(4, &seed, &alphabet(), &mut Vec::new());
}

fn explore(remaining: usize, recorder: &Recorder, actions: &[Action], history: &mut Vec<Action>) {
    if remaining == 0 {
        return;
    }
    for action in actions {
        let mut next = recorder.clone();
        history.push(*action);
        apply(&mut next, *action);
        next.assert_agreement(&*history);
        explore(remaining - 1, &next, actions, history);
        history.pop();
    }
}

fn alphabet() -> Vec<Action> {
    vec![
        Action::Tick,
        Action::Tick,
        Action::Lifecycle(group(0), LifecycleRequest::Drain),
        Action::Lifecycle(group(1), create(2)),
        Action::OpenSession(group(0), first(), client(0), epoch(2)),
        Action::Submit(group(0), first(), add(0, 1, 1, 3, 1)),
        Action::Submit(group(0), first(), read(0, 1, 1, 1)),
        Action::Submit(group(1), first(), system(SystemClass::Control, 1)),
        Action::Submit(group(1), first(), faulty(SystemClass::Bulk, 1)),
        Action::Signal(ReadinessSignal::stalled(group(1))),
    ]
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Lifecycle(GroupId, LifecycleRequest),
    OpenSession(GroupId, GroupIncarnation, ClientId, SessionEpoch),
    Submit(GroupId, GroupIncarnation, Work),
    Signal(ReadinessSignal),
    Tick,
}

fn apply(recorder: &mut Recorder, action: Action) {
    match action {
        Action::Lifecycle(id, request) => {
            recorder.lifecycle(id, request);
        }
        Action::OpenSession(id, incarnation, client_id, session_epoch) => {
            recorder.open_session(id, incarnation, client_id, session_epoch);
        }
        Action::Submit(id, incarnation, work) => {
            recorder.submit(id, incarnation, work);
        }
        Action::Signal(signal) => {
            recorder.step(&[signal]);
        }
        Action::Tick => {
            recorder.step(&[]);
        }
    }
}

// ---------------------------------------------------------------------------
// Seeded random workloads
// ---------------------------------------------------------------------------

/// Mixed workloads over a handful of groups, checked after every action. The
/// seed is printed on failure and is the only thing needed to reproduce one.
#[test]
fn independent_models_agree_across_seeded_random_workloads() {
    for seed in 0..24_u64 {
        run_workload(seed, config(12, 3, 3, 12, 64), 12, 400, 1);
    }
}

/// The same generator over a deliberately cramped host, so queue bounds, quota
/// pressure, and worker exhaustion are the common case rather than the corner.
#[test]
fn independent_models_agree_under_saturated_bounds() {
    for seed in 100..116_u64 {
        run_workload(seed, config(8, 1, 2, 3, 6), 8, 300, 1);
    }
}

fn run_workload(
    seed: u64,
    bounds: SchedulerConfig,
    groups: u32,
    steps: usize,
    checkpoint: usize,
) -> Recorder {
    let mut recorder = Recorder::new(bounds);
    let mut rng = Rng::new(seed);
    let mut driver = Driver::new(groups, bounds.max_clients_per_group());

    for step in 0..steps {
        driver.act(&mut recorder, &mut rng);
        if step % checkpoint == 0 {
            recorder.assert_agreement(&(seed, step));
        }
    }
    recorder.assert_agreement(&(seed, "final"));
    recorder
}

/// Generates plausible traffic. It reads the scheduler's own public view to
/// stay mostly admissible and then deliberately misses, which is what puts
/// pressure on the rejection paths. It never reads the oracle.
struct Driver {
    groups: u32,
    clients: u32,
    epochs: Vec<u64>,
    sequences: Vec<u64>,
    resend: Vec<Option<Work>>,
}

impl Driver {
    fn new(groups: u32, clients: u32) -> Self {
        let slots = (groups * clients) as usize;
        Self {
            groups,
            clients,
            epochs: vec![1; slots],
            sequences: vec![1; slots],
            resend: vec![None; slots],
        }
    }

    fn slot(&self, id: GroupId, client_id: u32) -> usize {
        (id.get() * self.clients + client_id) as usize
    }

    fn act(&mut self, recorder: &mut Recorder, rng: &mut Rng) {
        let id = group(u32::try_from(rng.index(self.groups as usize)).expect("group fits in u32"));
        let live = recorder
            .scheduler()
            .group(id)
            .map_or_else(GroupIncarnation::first, |view| view.incarnation);

        match rng.below(100) {
            0..=29 => {
                recorder.step(&[]);
            }
            30..=33 => {
                let signal = if rng.below(2) == 0 {
                    ReadinessSignal::stalled(id)
                } else {
                    ReadinessSignal::available(id)
                };
                recorder.step(&[signal]);
            }
            34..=45 => {
                recorder.lifecycle(
                    id,
                    Self::lifecycle_request(recorder_state(recorder, id), rng),
                );
            }
            46..=53 => {
                let client_id = u32::try_from(rng.index(self.clients as usize + 1))
                    .expect("client slots fit in u32");
                let named = if rng.below(8) == 0 {
                    self.epochs[self.slot(id, client_id.min(self.clients - 1))] + 1
                } else {
                    self.epochs[self.slot(id, client_id.min(self.clients - 1))]
                };
                let outcome = recorder.open_session(id, live, client(client_id), epoch(named));
                if matches!(
                    outcome,
                    rafter_reference_sharded_counter::SessionOutcome::Opened { .. }
                        | rafter_reference_sharded_counter::SessionOutcome::Replaced { .. }
                ) {
                    let slot = self.slot(id, client_id);
                    self.epochs[slot] = named;
                    self.sequences[slot] = 1;
                    self.resend[slot] = None;
                }
            }
            54..=63 => {
                let class = match rng.below(3) {
                    0 => SystemClass::Control,
                    1 => SystemClass::Snapshot,
                    _ => SystemClass::Bulk,
                };
                let work = if rng.below(40) == 0 {
                    faulty(
                        class,
                        1 + u32::try_from(rng.below(3)).expect("costs fit in u32"),
                    )
                } else {
                    system(
                        class,
                        1 + u32::try_from(rng.below(4)).expect("costs fit in u32"),
                    )
                };
                recorder.submit(id, live, work);
            }
            _ => self.submit_counter(recorder, rng, id, live),
        }
    }

    fn lifecycle_request(state: Option<GroupState>, rng: &mut Rng) -> LifecycleRequest {
        // Mostly the legal successor, so groups actually reach serving, and
        // sometimes an arbitrary one, so the conflict table is exercised.
        if rng.below(5) == 0 {
            return match rng.below(6) {
                0 => create(1 + u32::try_from(rng.below(3)).expect("quotas fit in u32")),
                1 => LifecycleRequest::Recover,
                2 => LifecycleRequest::Serve,
                3 => LifecycleRequest::Drain,
                4 => LifecycleRequest::Remove,
                _ => LifecycleRequest::Tombstone,
            };
        }
        match state {
            None => create(1 + u32::try_from(rng.below(3)).expect("quotas fit in u32")),
            Some(GroupState::Creating) => LifecycleRequest::Recover,
            Some(GroupState::Recovering) => LifecycleRequest::Serve,
            Some(GroupState::Serving) => LifecycleRequest::Drain,
            Some(GroupState::Draining) => LifecycleRequest::Remove,
            Some(GroupState::Removed) => {
                if rng.below(3) == 0 {
                    LifecycleRequest::Tombstone
                } else {
                    create(1 + u32::try_from(rng.below(3)).expect("quotas fit in u32"))
                }
            }
            Some(GroupState::Tombstoned) => create(1),
        }
    }

    fn submit_counter(
        &mut self,
        recorder: &mut Recorder,
        rng: &mut Rng,
        id: GroupId,
        live: GroupIncarnation,
    ) {
        let client_id =
            u32::try_from(rng.index(self.clients as usize)).expect("clients fit in u32");
        let slot = self.slot(id, client_id);

        // Resend the last submission verbatim, the way a client retries after
        // an unknown outcome.
        if rng.below(6) == 0 {
            if let Some(resent) = self.resend[slot] {
                recorder.submit(id, live, resent);
                return;
            }
        }

        let session_epoch = match rng.below(12) {
            0 => self.epochs[slot].saturating_sub(1).max(1),
            1 => self.epochs[slot] + 1,
            _ => self.epochs[slot],
        };
        let seq = match rng.below(12) {
            0 => self.sequences[slot].saturating_sub(1).max(1),
            1 => self.sequences[slot] + 1,
            _ => self.sequences[slot],
        };
        let command = if rng.below(4) == 0 {
            CounterCommand::Read
        } else {
            CounterCommand::Add {
                delta: support::delta(rng.signed(200)),
            }
        };
        let cost = 1 + u32::try_from(rng.below(3)).expect("costs fit in u32");

        let work = if rng.below(32) == 0 {
            let unrelated =
                rafter_reference_sharded_counter::RequestFingerprint::of(&CounterCommand::Add {
                    delta: support::delta(rng.signed(9)),
                });
            counter_with_fingerprint(client_id, session_epoch, seq, unrelated, command, cost)
        } else {
            counter(client_id, session_epoch, seq, command, cost)
        };

        if matches!(
            recorder.submit(id, live, work),
            AdmissionOutcome::Queued { .. }
        ) {
            self.epochs[slot] = session_epoch;
            self.sequences[slot] = seq + 1;
            self.resend[slot] = Some(work);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupState {
    Creating,
    Recovering,
    Serving,
    Draining,
    Removed,
    Tombstoned,
}

fn recorder_state(recorder: &Recorder, id: GroupId) -> Option<GroupState> {
    use rafter_reference_sharded_counter::GroupLifecycle;
    recorder.scheduler().group(id).map(|view| match view.state {
        GroupLifecycle::Creating => GroupState::Creating,
        GroupLifecycle::Recovering => GroupState::Recovering,
        GroupLifecycle::Serving => GroupState::Serving,
        GroupLifecycle::Draining => GroupState::Draining,
        GroupLifecycle::Removed => GroupState::Removed,
        GroupLifecycle::Tombstoned => GroupState::Tombstoned,
    })
}

// ---------------------------------------------------------------------------
// Thousands of groups
// ---------------------------------------------------------------------------

/// The scale the program asks for: thousands of independent groups, a hot tail,
/// a slow group, a stalled group, a poisoned group, and enough ticks to
/// complete many passes over all of them.
///
/// The oracle folds the whole history at each checkpoint rather than after
/// every tick. Folding is the price of deriving instead of tracking, and paying
/// it fifty thousand times would say nothing that paying it six times does not.
#[test]
fn independent_models_agree_across_thousands_of_groups() {
    for seed in 0..2_u64 {
        let started = Instant::now();
        // Far fewer workers than ready groups, so a pass genuinely spans many
        // ticks and the plan is the whole host rather than whatever arrived
        // this instant. That is the shape the bound has to survive.
        let bounds = config(MANY_GROUPS, 32, 2, 16, 262_144);
        let mut recorder = Recorder::new(bounds);
        let mut rng = Rng::new(seed);

        let mut ledger = Ledger::new();
        for index in 0..MANY_GROUPS {
            let id = group(index);
            recorder.open_group(id, 1 + index % 3);
            recorder.open_session(id, first(), client(0), epoch(1));
        }
        // Every group starts with work, so the first plan covers the whole host.
        for index in 0..MANY_GROUPS {
            for _ in 0..3 {
                ledger.feed(&mut recorder, &mut rng, index);
            }
        }

        // A handful of groups behave badly on purpose, and the rest are
        // ordinary. The bound has to hold for the ordinary ones anyway.
        let slow = group(7);
        let stalling = group(11);
        let broken = group(13);
        recorder.submit(broken, first(), faulty(SystemClass::Control, 1));
        for _ in 0..12 {
            recorder.submit(slow, first(), system(SystemClass::Snapshot, 9));
        }

        let mut ticks = 0_u64;
        for step in 0..900_usize {
            // Keep topping up a rotating slice so the ready set stays wide and
            // keeps changing underneath the plan in progress.
            for offset in 0..64_u32 {
                let index =
                    (u32::try_from(step).expect("steps fit in u32") * 64 + offset) % MANY_GROUPS;
                ledger.feed(&mut recorder, &mut rng, index);
            }
            let signals = if step % 37 == 0 {
                vec![ReadinessSignal::stalled(stalling)]
            } else if step % 37 == 19 {
                vec![ReadinessSignal::available(stalling)]
            } else {
                Vec::new()
            };
            recorder.step(&signals);
            ticks += 1;

            if step % 150 == 149 {
                recorder.assert_agreement(&(seed, step));
            }
        }
        recorder.assert_agreement(&(seed, "final"));

        let report = recorder
            .oracle()
            .audit()
            .unwrap_or_else(|violation| panic!("seed {seed} broke the bound: {violation:?}"));
        assert_eq!(report.widest_gap, 0, "seed {seed}");
        assert!(
            report.passes_completed >= 8,
            "seed {seed} completed too few passes to mean anything: {report:?}"
        );
        assert!(
            report.widest_plan > MANY_GROUPS / 2,
            "seed {seed} never armed a plan over most of the host: {report:?}"
        );

        // Counters are per group and share nothing, so a fully drained group's
        // value is the sum of the deltas that reached it and no one else's.
        let summary = recorder.scheduler().summary();
        ledger.assert_drained_counters(&recorder);
        assert_eq!(
            summary.live_groups, MANY_GROUPS,
            "no group left the host in this workload"
        );
        assert_eq!(summary.poisoned_groups, 1, "exactly one group broke");
        assert!(
            recorder
                .scheduler()
                .group(broken)
                .is_some_and(|view| view.poisoned),
            "seed {seed}: the poisoned group is the one that ran faulty work"
        );

        println!(
            "seed {seed}: {MANY_GROUPS} groups, {ticks} ticks, {} events, {} passes, widest plan {}, gap {} — {:?}",
            recorder.oracle().len(),
            report.passes_completed,
            report.widest_plan,
            report.widest_gap,
            started.elapsed()
        );
    }
}

/// Tracks what each group's counter should hold, independently of both models.
///
/// This is not a third specification: it only adds up the deltas it watched
/// being accepted, which is the arithmetic a client could do for itself. It
/// exists so the scale workload states a value rather than only agreeing with
/// itself.
struct Ledger {
    expected: Vec<i64>,
    next_sequence: Vec<u64>,
}

impl Ledger {
    fn new() -> Self {
        Self {
            expected: vec![0; MANY_GROUPS as usize],
            next_sequence: vec![1; MANY_GROUPS as usize],
        }
    }

    fn feed(&mut self, recorder: &mut Recorder, rng: &mut Rng, index: u32) {
        let slot = index as usize;
        let amount = rng.signed(50);
        let seq = self.next_sequence[slot];
        if matches!(
            recorder.submit(group(index), first(), add(0, 1, seq, amount, 1)),
            AdmissionOutcome::Queued { .. }
        ) {
            self.next_sequence[slot] = seq + 1;
            self.expected[slot] += amount;
        }
    }

    /// Every group whose queue has emptied must hold exactly what it was sent.
    /// A group with work still queued is mid-flight and says nothing yet.
    fn assert_drained_counters(&self, recorder: &Recorder) {
        let mut checked = 0_u32;
        for index in 0..MANY_GROUPS {
            let id = group(index);
            let Some(view) = recorder.scheduler().group(id) else {
                panic!("{id:?} was created and must have a view");
            };
            if view.queued == 0 && !view.poisoned {
                assert_eq!(
                    view.counter, self.expected[index as usize],
                    "{id:?} does not hold the sum of the deltas it accepted"
                );
                checked += 1;
            }
        }
        assert!(
            checked > MANY_GROUPS / 10,
            "too few groups drained to say anything: {checked}"
        );
    }
}

/// A workload that removes and reopens groups underneath a running scheduler,
/// so the incarnation rules are exercised against a live ready set rather than
/// on a quiet host.
#[test]
fn independent_models_agree_while_groups_are_removed_and_reopened() {
    for seed in 200..208_u64 {
        let bounds = config(24, 2, 2, 8, 128);
        let mut recorder = Recorder::new(bounds);
        let mut rng = Rng::new(seed);
        let mut stale: Vec<(GroupId, GroupIncarnation)> = Vec::new();

        for step in 0..500_usize {
            let id = group(u32::try_from(rng.index(24)).expect("groups fit in u32"));
            let live = recorder
                .scheduler()
                .group(id)
                .map_or_else(GroupIncarnation::first, |view| view.incarnation);

            match rng.below(10) {
                0..=3 => {
                    recorder.step(&[]);
                }
                4 if !stale.is_empty() => {
                    // Address a generation that has already retired. It must be
                    // refused whether the slot is gone or live again.
                    let (old_group, old_incarnation) = stale[rng.index(stale.len())];
                    let outcome =
                        recorder.submit(old_group, old_incarnation, system(SystemClass::Bulk, 1));
                    assert!(
                        !matches!(outcome, AdmissionOutcome::Queued { .. }),
                        "seed {seed}: a retired incarnation was admitted"
                    );
                }
                5..=6 => {
                    recorder.lifecycle(id, self::cycle_request(&recorder, id, &mut stale, live));
                }
                7 => {
                    recorder.open_session(id, live, client(0), epoch(1));
                }
                _ => {
                    recorder.submit(id, live, system(SystemClass::Bulk, 1));
                }
            }
            if step % 25 == 24 {
                recorder.assert_agreement(&(seed, step));
            }
        }
        recorder.assert_agreement(&(seed, "final"));
        recorder
            .oracle()
            .audit()
            .unwrap_or_else(|violation| panic!("seed {seed} broke the bound: {violation:?}"));
    }
}

fn cycle_request(
    recorder: &Recorder,
    id: GroupId,
    stale: &mut Vec<(GroupId, GroupIncarnation)>,
    live: GroupIncarnation,
) -> LifecycleRequest {
    use rafter_reference_sharded_counter::GroupLifecycle;
    match recorder.scheduler().group(id).map(|view| view.state) {
        None | Some(GroupLifecycle::Removed) => create(2),
        Some(GroupLifecycle::Creating) => LifecycleRequest::Recover,
        Some(GroupLifecycle::Recovering) => LifecycleRequest::Serve,
        Some(GroupLifecycle::Serving) => LifecycleRequest::Drain,
        Some(GroupLifecycle::Draining) => {
            stale.push((id, live));
            LifecycleRequest::Remove
        }
        Some(GroupLifecycle::Tombstoned) => LifecycleRequest::Tombstone,
    }
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

    /// Returns a nonzero value in `-magnitude..=magnitude`.
    fn signed(&mut self, magnitude: u64) -> i64 {
        let size = i64::try_from(1 + self.below(magnitude)).expect("test magnitudes fit in i64");
        if self.below(2) == 0 {
            size
        } else {
            -size
        }
    }
}
