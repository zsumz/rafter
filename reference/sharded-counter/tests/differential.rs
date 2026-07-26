mod support;

use std::{collections::BTreeSet, time::Instant};

use rafter_reference_sharded_counter::{
    AdmissionOutcome, ClientId, CounterCommand, GroupId, GroupIncarnation, GroupLifecycle,
    LifecycleOutcome, LifecycleRequest, ReadinessSignal, SchedulerConfig, SessionEpoch,
    SystemClass, Work,
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

/// Groups the combined workload drives. Fewer than [`MANY_GROUPS`], because
/// that one holds every group for the whole run while this one churns them
/// through the lifecycle, and churn costs history per group rather than per
/// tick.
const CHURNED_GROUPS: u32 = 512;

// ---------------------------------------------------------------------------
// Exhaustive short histories
// ---------------------------------------------------------------------------

/// How deep the exhaustive enumeration goes.
const DEPTH: u32 = 4;

/// Every ordering of a small alphabet, applied to two already-serving groups.
/// Short histories reach the awkward corners — a retry racing a drain, a stall
/// arriving mid-plan, a removal landing between a plan and its turn — that a
/// random walk visits rarely.
///
/// The alphabet covers the whole lifecycle rather than its opening moves. A
/// group can reach `Removed` and `Tombstoned` within the depth, an availability
/// report can be withdrawn as well as raised, and the transitions that conflict
/// are enumerated alongside the ones that apply. An enumeration that called
/// itself exhaustive over a vocabulary missing half its symbols would be
/// exhaustive over the wrong thing.
///
/// The count below is the honest one. The alphabet carried `Tick` twice, so a
/// third of the histories visited were exact duplicates of another — work the
/// enumeration paid for and learned nothing from, while inflating the number it
/// reported. `distinct` asserts the symbols are unique so it cannot recur.
#[test]
fn independent_models_agree_across_exhaustive_short_histories() {
    let alphabet = alphabet();
    let distinct: BTreeSet<String> = alphabet
        .iter()
        .map(|action| format!("{action:?}"))
        .collect();
    assert_eq!(
        distinct.len(),
        alphabet.len(),
        "a repeated symbol enumerates the same history twice and reports it as two"
    );

    let bounds = config(3, 1, 2, 4, 6);
    let mut seed = Recorder::new(bounds);
    for id in [group(0), group(1)] {
        seed.open_group(id, 2);
        seed.open_session(id, first(), client(0), epoch(1));
    }
    let started = Instant::now();
    let mut visited = 0_u64;
    explore(DEPTH, &seed, &alphabet, &mut Vec::new(), &mut visited);

    let expected: u64 = (1..=DEPTH)
        .map(|depth| {
            u64::try_from(alphabet.len())
                .expect("an alphabet fits in u64")
                .pow(depth)
        })
        .sum();
    assert_eq!(visited, expected, "every history is visited exactly once");
    println!(
        "{} symbols to depth {DEPTH}: {visited} distinct histories in {:?}",
        alphabet.len(),
        started.elapsed()
    );
}

fn explore(
    remaining: u32,
    recorder: &Recorder,
    actions: &[Action],
    history: &mut Vec<Action>,
    visited: &mut u64,
) {
    if remaining == 0 {
        return;
    }
    for action in actions {
        let mut next = recorder.clone();
        history.push(*action);
        apply(&mut next, *action);
        next.assert_agreement(&*history);
        *visited += 1;
        explore(remaining - 1, &next, actions, history, visited);
        history.pop();
    }
}

fn alphabet() -> Vec<Action> {
    vec![
        Action::Tick,
        // The first group's whole retirement path, so `Removed` and
        // `Tombstoned` are reachable within the depth rather than named in a
        // table no enumeration reaches.
        Action::Lifecycle(group(0), LifecycleRequest::Drain),
        Action::Lifecycle(group(0), LifecycleRequest::Remove),
        Action::Lifecycle(group(0), LifecycleRequest::Tombstone),
        // The second group's transitions from a state that admits none of
        // them, so the conflict table is exercised under interleaving.
        Action::Lifecycle(group(1), create(2)),
        Action::Lifecycle(group(1), LifecycleRequest::Recover),
        Action::Lifecycle(group(1), LifecycleRequest::Serve),
        Action::OpenSession(group(0), first(), client(0), epoch(2)),
        Action::Submit(group(0), first(), add(0, 1, 1, 3, 1)),
        Action::Submit(group(0), first(), read(0, 1, 1, 1)),
        Action::Submit(group(1), first(), system(SystemClass::Control, 1)),
        Action::Submit(group(1), first(), faulty(SystemClass::Bulk, 1)),
        // Both directions of the one readiness input the scheduler does not
        // derive. A stall that could never be withdrawn is half a signal.
        Action::Signal(ReadinessSignal::stalled(group(1))),
        Action::Signal(ReadinessSignal::available(group(1))),
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
    let mut completed = 0_u64;
    for seed in 0..24_u64 {
        completed += run_workload(seed, config(12, 3, 3, 12, 64), 12, 400, 1);
    }
    // Observed: 219 across the twenty-four seeds. The floor sits at roughly
    // half that, so it fails a workload that stopped scheduling rather than one
    // whose generator drifted by a pass.
    assert_scheduled(completed, 24 * 4);
}

/// The same generator over a deliberately cramped host, so queue bounds, quota
/// pressure, and worker exhaustion are the common case rather than the corner.
#[test]
fn independent_models_agree_under_saturated_bounds() {
    let mut completed = 0_u64;
    for seed in 100..116_u64 {
        completed += run_workload(seed, config(8, 1, 2, 3, 6), 8, 300, 1);
    }
    // Observed: 121 across the sixteen seeds. One worker and a queue bound of
    // three buys far fewer passes than the roomy host, which is the point of
    // the configuration and the reason this floor is its own number.
    assert_scheduled(completed, 16 * 4);
}

/// Returns the passes the workload actually retired.
///
/// The count is returned rather than discarded because an agreement check is
/// silent about a scheduler that scheduled nothing: two models that both did
/// nothing agree perfectly, and the fairness audit certifies the emptiness. The
/// caller asserts a floor, which is what turns agreement into evidence.
fn run_workload(
    seed: u64,
    bounds: SchedulerConfig,
    groups: u32,
    steps: usize,
    checkpoint: usize,
) -> u64 {
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
    let report = recorder
        .oracle()
        .audit()
        .unwrap_or_else(|violation| panic!("seed {seed} broke the bound: {violation:?}"));
    assert_eq!(report.widest_gap, 0, "seed {seed}");
    report.passes_completed
}

/// Asserts that a set of workloads retired enough passes to have proved
/// something, and says so when they did not.
fn assert_scheduled(completed: u64, floor: u64) {
    assert!(
        completed >= floor,
        "the workloads retired {completed} passes, below the floor of {floor}: \
         a green audit over histories that scheduled nothing proves nothing"
    );
    println!("workloads retired {completed} passes");
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
                let state = recorder.scheduler().group(id).map(|view| view.state);
                recorder.lifecycle(id, Self::lifecycle_request(state, rng));
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

    fn lifecycle_request(state: Option<GroupLifecycle>, rng: &mut Rng) -> LifecycleRequest {
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
            Some(GroupLifecycle::Creating) => LifecycleRequest::Recover,
            Some(GroupLifecycle::Recovering) => LifecycleRequest::Serve,
            Some(GroupLifecycle::Serving) => LifecycleRequest::Drain,
            Some(GroupLifecycle::Draining) => LifecycleRequest::Remove,
            Some(GroupLifecycle::Removed) => {
                if rng.below(3) == 0 {
                    LifecycleRequest::Tombstone
                } else {
                    create(1 + u32::try_from(rng.below(3)).expect("quotas fit in u32"))
                }
            }
            Some(GroupLifecycle::Tombstoned) => create(1),
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

/// Every dimension at once, at scale.
///
/// The other two large workloads each hold half the picture: the
/// thousands-of-groups run has classes, costs, a slow group, a stall, and a
/// poisoning, but no group ever leaves the host; the removal run churns the
/// lifecycle hard but submits nothing except unit-cost bulk work. Between them
/// they say that each dimension survives, and nothing at all about the
/// dimensions combining — which is where a scheduler is most likely to be
/// wrong, because that is where a removal lands on a group that is holding a
/// worker, or a poisoning lands on one mid-drain.
///
/// So: five hundred groups cycling through their whole lifecycle while
/// carrying four work classes, costs from one to nine ticks, faulty items, and
/// external stalls, with sixteen workers for the lot. The floors at the end are
/// what make it evidence rather than a run: a workload that churned every group
/// into oblivion before it could be scheduled would agree with the oracle
/// perfectly and prove nothing.
#[test]
fn independent_models_agree_when_every_dimension_combines_at_scale() {
    for seed in 300..302_u64 {
        let started = Instant::now();
        let bounds = config(CHURNED_GROUPS, 64, 2, 12, 65_536);
        let mut recorder = Recorder::new(bounds);
        let mut rng = Rng::new(seed);
        let mut census = Census::default();

        for index in 0..CHURNED_GROUPS {
            recorder.open_group(group(index), 1 + index % 3);
        }

        let mut ticks = 0_u64;
        for step in 0..600_usize {
            let base = u32::try_from(step).expect("steps fit in u32") * 48;

            // Feed a rotating slice with the whole class and cost range, and
            // an occasional item that will poison the group it reaches.
            for offset in 0..48_u32 {
                let id = group((base + offset) % CHURNED_GROUPS);
                let class = match rng.below(3) {
                    0 => SystemClass::Control,
                    1 => SystemClass::Snapshot,
                    _ => SystemClass::Bulk,
                };
                let cost = 1 + u32::try_from(rng.below(9)).expect("costs fit in u32");
                let item = if rng.below(400) == 0 {
                    faulty(class, cost)
                } else {
                    system(class, cost)
                };
                recorder.submit(id, live_incarnation(&recorder, id), item);
            }

            // Churn a different slice through the lifecycle, so removals and
            // reopenings land on groups that are queued, dispatched, drained,
            // or poisoned rather than on quiet ones.
            for offset in 0..6_u32 {
                let id = group((base / 8 + offset) % CHURNED_GROUPS);
                let request = churn_request(&recorder, id, &mut rng);
                let transition = recorder.lifecycle(id, request);
                census.record(id, transition.outcome);
            }

            let signals = match step % 11 {
                0 => vec![ReadinessSignal::stalled(group(base % CHURNED_GROUPS))],
                5 => vec![ReadinessSignal::available(group(base % CHURNED_GROUPS))],
                _ => Vec::new(),
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
        let summary = recorder.scheduler().summary();
        println!(
            "seed {seed}: {CHURNED_GROUPS} groups, {ticks} ticks, {} events, {} passes, \
             widest plan {}, gap {}, {} serviced, {} failed — reopened {}, removed {}, \
             tombstoned {}, poisoned {} — {:?}",
            recorder.oracle().len(),
            report.passes_completed,
            report.widest_plan,
            report.widest_gap,
            summary.serviced,
            summary.failed,
            census.reopened.len(),
            census.removed.len(),
            census.tombstoned.len(),
            summary.poisoned_groups,
            started.elapsed()
        );

        // Every dimension has to have actually happened, or the agreement was
        // over a workload that only looked combined.
        assert_eq!(report.widest_gap, 0, "seed {seed}");
        assert!(
            report.passes_completed >= 8,
            "seed {seed} retired too few passes to mean anything: {report:?}"
        );
        assert!(
            report.widest_plan >= 16,
            "seed {seed} never armed a plan wide enough to contend: {report:?}"
        );
        assert!(
            summary.serviced > 1_000,
            "seed {seed} serviced {} items, so the churn outran the work",
            summary.serviced
        );
        assert!(summary.failed > 0, "seed {seed} retired no poisoned queue");
        assert!(
            summary.poisoned_groups > 0,
            "seed {seed} poisoned no group, so the faulty items never landed"
        );
        assert!(
            census.reopened.len() >= 8 && census.removed.len() >= 8,
            "seed {seed} churned too little: {} reopened, {} removed",
            census.reopened.len(),
            census.removed.len()
        );
        assert!(
            !census.tombstoned.is_empty(),
            "seed {seed} tombstoned nothing"
        );
    }
}

/// Which groups the combined workload actually put through each edge.
///
/// The workload asks for transitions and the scheduler decides them, so this
/// reads the outcomes rather than the requests. Removal in particular is
/// refused while a queue is still owed, which is exactly the case worth
/// counting: a floor built from requests would be satisfied by four hundred
/// removals that were all refused.
#[derive(Default)]
struct Census {
    removed: BTreeSet<GroupId>,
    reopened: BTreeSet<GroupId>,
    tombstoned: BTreeSet<GroupId>,
}

impl Census {
    fn record(&mut self, id: GroupId, outcome: LifecycleOutcome) {
        match outcome {
            LifecycleOutcome::Created { incarnation }
                if incarnation > GroupIncarnation::first() =>
            {
                self.reopened.insert(id);
            }
            LifecycleOutcome::Applied {
                to: GroupLifecycle::Removed,
                ..
            } => {
                self.removed.insert(id);
            }
            LifecycleOutcome::Applied {
                to: GroupLifecycle::Tombstoned,
                ..
            } => {
                self.tombstoned.insert(id);
            }
            LifecycleOutcome::Created { .. }
            | LifecycleOutcome::Applied { .. }
            | LifecycleOutcome::Idempotent { .. }
            | LifecycleOutcome::Rejected(_) => {}
        }
    }
}

fn live_incarnation(recorder: &Recorder, id: GroupId) -> GroupIncarnation {
    recorder
        .scheduler()
        .group(id)
        .map_or_else(GroupIncarnation::first, |view| view.incarnation)
}

/// Walks a group one step around its lifecycle, occasionally sideways.
fn churn_request(recorder: &Recorder, id: GroupId, rng: &mut Rng) -> LifecycleRequest {
    let state = recorder.scheduler().group(id).map(|view| view.state);
    match state {
        None => create(1 + u32::try_from(rng.below(3)).expect("quotas fit in u32")),
        Some(GroupLifecycle::Creating) => LifecycleRequest::Recover,
        Some(GroupLifecycle::Recovering) => LifecycleRequest::Serve,
        // A serving group is drained only sometimes, so most groups stay in
        // service long enough to contend for turns.
        Some(GroupLifecycle::Serving) => {
            if rng.below(4) == 0 {
                LifecycleRequest::Drain
            } else {
                LifecycleRequest::Serve
            }
        }
        // Removal is refused while the queue is owed, which is the point: the
        // request lands on drained, mid-service, and poisoned groups alike.
        Some(GroupLifecycle::Draining) => {
            if rng.below(5) == 0 {
                // A repeated drain is how a poisoned group's backlog is
                // retired, and the only way `Remove` ever stops being refused.
                LifecycleRequest::Drain
            } else {
                LifecycleRequest::Remove
            }
        }
        Some(GroupLifecycle::Removed) => {
            if rng.below(6) == 0 {
                LifecycleRequest::Tombstone
            } else {
                create(1 + u32::try_from(rng.below(3)).expect("quotas fit in u32"))
            }
        }
        Some(GroupLifecycle::Tombstoned) => LifecycleRequest::Tombstone,
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
        let report = recorder
            .oracle()
            .audit()
            .unwrap_or_else(|violation| panic!("seed {seed} broke the bound: {violation:?}"));
        assert!(
            report.passes_completed >= 8,
            "seed {seed} retired too few passes to mean anything: {report:?}"
        );
    }
}

fn cycle_request(
    recorder: &Recorder,
    id: GroupId,
    stale: &mut Vec<(GroupId, GroupIncarnation)>,
    live: GroupIncarnation,
) -> LifecycleRequest {
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
