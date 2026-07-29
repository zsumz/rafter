//! The process loop and the peer control plane it must not outlive.
//!
//! Split from [`process_cluster`] along the line between what the *service*
//! does and what the *process* does. That suite drives the lock: fencing,
//! failover, restart recovery, session deduplication, the readiness gate. This
//! one drives the loop underneath it — how a pass divides its attention between
//! clients and the protocol, and what happens when the record no other artifact
//! holds cannot be made durable.
//!
//! **The two questions share one answer and that is why they share a file.**
//! Everything the loop owes the cluster sits behind its client drain: peer
//! frames, the clock, granted reads, expiring deadlines, and this process's own
//! terminal exit. A drain that yields only when the clients stop makes all of
//! them conditional on client behaviour, and a terminal state discovered *below*
//! the drain is a terminal state the next batch of clients does not know about.
//! One suite tests the budget that bounds the drain and the condition that ends
//! it early.
//!
//! # Running it
//!
//! The suite is `#[ignore]`d by default and runs on request:
//!
//! ```text
//! scripts/reference-process-check
//! scripts/reference-source-check -- -p rafter-reference-fenced-lock \
//!     --test process_control_plane -- --ignored
//! ```
//!
//! # What each test does about the kill window
//!
//! Every test that kills a process has to say what it assumes about *where* the
//! kill landed, because a real `SIGKILL` cannot be aimed:
//!
//! - `a_deleted_control_plane_checkpoint_refuses_to_open_as_a_first_boot` kills
//!   a follower while idle and then deletes the file itself. Both windows are
//!   shut: the deletion is the test's, not the kill's.
//! - `a_client_flood_cannot_starve_the_protocol_on_the_replicas_it_floods`
//!   kills an idle leader and never restarts it, so neither window can affect
//!   what the survivors prove.
//! - The three control-plane failure tests kill a replica while idle and
//!   restart it under a plain `open`, so they assert nothing about a commit
//!   point; the seal window is handled by `restart_with_control_plane_fault`
//!   waiting for readiness, which a replica that refused would never reach.
//! - `the_peer_control_plane_checkpoint_is_durable_across_a_restart` kills a
//!   follower while idle. The commit window is empty and the assertions are
//!   inequalities that hold under either recovery mode.

// The command builders are shared by every suite, and this one uses a subset:
// its writes exist to give the replicas something to commit, not to exercise
// the lock's own vocabulary.
#[allow(dead_code, reason = "this suite uses a subset of the shared builders")]
mod support;

// The orchestration harness is shared with `process_cluster`, and each suite
// reaches a different part of it: the lock-protocol helpers and the slot-damage
// paths belong to that one.
#[allow(dead_code, reason = "this suite uses a subset of the shared harness")]
#[path = "support/process.rs"]
mod process;
#[path = "support/scratch.rs"]
mod scratch;

use rafter::{LogIndex, NodeId};
use rafter_reference_fenced_lock::{LockConfig, OperationResult};

use process::{ProcessCluster, SubmitOutcome};
use support::{acquire, config, open_session, renew, resource, submit};

/// The bounds every process test runs under.
///
/// The same four-by-four bound the service suite uses, for the same reason: a
/// durable publication is a whole state image, so a larger bound would make
/// every transaction bigger without testing anything more.
fn process_config() -> LockConfig {
    config(4, 4)
}

/// How many sockets a flood keeps `STATUS` in flight on.
///
/// Four is enough that the replica's job queue is essentially never empty — a
/// loopback round trip is tens of microseconds and the loop's idle wait is two
/// milliseconds — without spending threads on load that proves nothing more.
const FLOOD_CONNECTIONS: usize = 4;

/// How many service requests are queued behind the one that trips the fault.
///
/// The reviewer's regression needs two; this is eight so the assertion covers a
/// batch rather than a single follower, and is still far under the loop's
/// sixty-four-job budget — a fix that merely *lowered* the budget would leave
/// every one of these admitted and would still fail.
const QUEUED_BEHIND: usize = 8;

/// Returns the main-loop admission ticket exposed by the injected-fault seam.
fn harness_ticket(response: &str) -> u64 {
    response
        .rsplit_once(" HARNESS_TICKET ")
        .unwrap_or_else(|| panic!("fault-seam response has no admission ticket: {response:?}"))
        .1
        .parse()
        .unwrap_or_else(|error| panic!("fault-seam response has an invalid ticket: {error}"))
}

/// Asserts an operation committed with an exact result.
#[track_caller]
fn assert_operation(outcome: &SubmitOutcome, expected: OperationResult) {
    assert_eq!(
        outcome.operation(),
        expected,
        "the replicated result must be exactly the contract's result"
    );
}

/// One line of a checkpoint file, by its leading field name.
fn checkpoint_line<'a>(text: &'a str, field: &str) -> &'a str {
    text.lines()
        .find(|line| line == &field || line.starts_with(&format!("{field} ")))
        .unwrap_or_else(|| panic!("the checkpoint has no `{field}` line: {text:?}"))
}

/// Where a checkpoint's current committed state was observed, or `None` if it
/// has observed nothing.
///
/// One position rather than the two consumer offsets a version-4 file carried.
/// It is not an offset at all: nothing is skipped against it, and what it
/// decides is which of two observations of the committed membership is the later
/// and therefore the one a join believes.
fn checkpoint_position(text: &str) -> Option<u64> {
    match checkpoint_line(text, "through")
        .strip_prefix("through ")
        .expect("the `through` line names its field")
    {
        "-" => None,
        value => Some(value.parse().expect("the position is a log index")),
    }
}

/// A replica whose control-plane checkpoint was deleted refuses to open.
///
/// **The failure this closes is a deletion that reads as a first boot.** An
/// absent file used to mean "nothing has been retired here", which is true of a
/// replica that has never run and false of every other one — so removing one
/// file downgraded a replica that had been serving for months to a blank
/// retirement record, and it started cheerfully with no mark, no live set, and
/// no fence obligations. Every identity the cluster had spent became allocatable
/// again, and every fence the link layer had refused was forgotten.
///
/// The evidence that distinguishes the two is the durable Raft commit floor,
/// which this replica has because it has been committing this test's own writes.
/// A directory probe could not answer it: the process creates `raft/` and `app/`
/// before it writes this file, so a first boot that crashed in its own opening
/// sequence leaves those behind too.
///
/// The refusal is a nonzero exit with the reason on stdout, which is the same
/// posture the store takes for a slot that should exist and does not.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_deleted_control_plane_checkpoint_refuses_to_open_as_a_first_boot() {
    let mut cluster = ProcessCluster::start("deleted-control-plane", process_config());
    let leader = cluster.wait_for_leader();
    let victim = *cluster
        .live_nodes()
        .iter()
        .find(|node_id| **node_id != leader)
        .expect("a three-replica cluster has a follower");

    cluster.submit_to_leader(open_session(0, 1));
    cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));
    cluster.wait_applied_through(victim, LogIndex(1));

    let checkpoint_path =
        process::NodeProcess::node_dir(cluster.root(), victim).join("control-plane");
    cluster.kill(victim);
    std::fs::remove_file(&checkpoint_path).expect("the replica published one before it was killed");

    let refused = cluster.restart_expecting_failure(victim);
    assert!(
        refused.status.code().is_some_and(|code| code != 0),
        "a replica that lost its retirement record must not start: {refused:?}"
    );
    assert!(
        refused.stdout.contains("FATAL") && refused.stdout.contains("is missing"),
        "and must say which artifact is gone: {:?}",
        refused.stdout
    );
    assert!(
        !checkpoint_path.exists(),
        "the refusal does not regenerate the file it refused over, which would \
         be the silent forgetting with an extra step"
    );

    cluster.shutdown();
}

/// A continuous client flood cannot starve Raft's clock or its inbound frames.
///
/// **The loop's fairness property, proved with the protocol rather than with a
/// counter.** One pass of the process loop answers client requests and then does
/// everything else it owes — delivering peer frames, ticking, driving reads,
/// expiring deadlines. Draining the client channel until it went quiet made "and
/// then" conditional on the clients stopping, and `STATUS` is enough to prevent
/// that: it answers from memory, so a connection can keep one in flight
/// perpetually without the replica doing any work that would slow the flood
/// down.
///
/// The leader is killed and **both survivors are flooded**, which is what makes
/// this a statement about the loop rather than about luck. A three-replica
/// cluster needs two votes, so neither flooded replica can be carried by the
/// other: the election requires both of them to reach their own election
/// timeout, send a poll, and deliver the answer — three separate things that all
/// live behind the client drain. Starved, no election happens at all and
/// `wait_for_leader` fails on its deadline.
///
/// A linearizable read on a flooded replica follows, because an election proves
/// ticks and votes and a `ReadIndex` proves the rest: the barrier is granted by
/// a quorum round the replica must both send and receive, and the grant is
/// *consumed* by a read call the pass makes after the delivery.
///
/// Kill window: the leader is killed while idle, so no write is in flight and
/// the seal window cannot affect anything — the killed replica is never
/// restarted.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_client_flood_cannot_starve_the_protocol_on_the_replicas_it_floods() {
    let mut cluster = ProcessCluster::start("client-flood", process_config());
    let leader = cluster.wait_for_agreed_leader();
    cluster.submit_to_leader(open_session(0, 1));
    let acquired = cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));
    assert_operation(
        &acquired,
        OperationResult::Acquired {
            token: acquired.acquired_token(),
            expiry: support::time(10),
        },
    );

    let survivors: Vec<NodeId> = cluster
        .live_nodes()
        .into_iter()
        .filter(|node_id| *node_id != leader)
        .collect();
    assert_eq!(survivors.len(), 2, "a three-replica cluster has two others");

    // Started before the kill, so the survivors are already saturated when the
    // election they have to run becomes necessary.
    let floods: Vec<process::StatusFlood> = survivors
        .iter()
        .map(|node_id| {
            process::StatusFlood::start(cluster.node_mut(*node_id).client_addr(), FLOOD_CONNECTIONS)
        })
        .collect();
    cluster.kill(leader);

    let successor = cluster.wait_for_leader();
    assert!(
        survivors.contains(&successor),
        "a survivor took office: {successor:?}"
    );

    // And the barrier half, on a replica that is still being flooded.
    let observed = cluster.get_lock(resource("vault"));
    assert!(
        observed.is_held(),
        "a linearizable read completed under the flood: {observed:?}"
    );

    let answered: u64 = floods.into_iter().map(process::StatusFlood::stop).sum();
    assert!(
        answered > 0,
        "the flood has to have been real load for this to have proved anything"
    );

    cluster.shutdown();
}

/// A replica that cannot make its control plane durable stops serving and exits
/// nonzero, however many clients keep asking.
///
/// **The terminal transition is behind the client drain, and that is the whole
/// defect.** A replica whose control-plane persistence has failed answers every
/// service request `ABANDONED` and is going to exit nonzero, because its next
/// restart would begin by forgetting whatever it retired. Both of those live in
/// the pass *after* the client drain: the drain answers `ABANDONED` and then
/// yields to the work that ends the process. When the drain only yielded on a
/// quiet channel, a replica that already knew its control plane was not durable
/// kept answering forever — the one process state where continuing to serve is
/// exactly the wrong thing — and a supervisor watching the exit code never
/// learned anything had happened.
///
/// So the flood runs *through* the failure rather than before it. The fault is
/// armed at the first client operation, the replica opens and serves normally,
/// the flood starts, and then one `QUERY` makes it undurable while `STATUS`
/// requests keep arriving on every other connection.
///
/// `STATUS` deliberately still answers in that state and service does not,
/// which is why the flood cannot mask the assertion: the requests keeping the
/// queue full are exactly the ones a failed replica is still allowed to answer.
///
/// Kill window: the replica is killed while idle and restarts under a plain
/// `open`, so this asserts nothing about a commit point; the seal window is
/// handled by `restart_with_control_plane_fault` waiting for readiness, which a
/// replica that refused would never reach.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn an_unpersistable_control_plane_stops_the_process_under_a_client_flood() {
    let mut cluster = ProcessCluster::start("unpersistable-control-plane", process_config());
    let leader = cluster.wait_for_leader();
    let victim = *cluster
        .live_nodes()
        .iter()
        .find(|node_id| **node_id != leader)
        .expect("a three-replica cluster has a follower");

    cluster.submit_to_leader(open_session(0, 1));
    cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));
    cluster.wait_applied_through(victim, LogIndex(1));

    cluster.kill(victim);
    let mut faulted = cluster.restart_with_control_plane_fault(victim, 1);
    let flood = process::StatusFlood::start(faulted.client_addr(), FLOOD_CONNECTIONS);

    // The client operation the fault is armed at. Its own answer is already the
    // refusal, because the loop abandons every waiter in the same pass that
    // discovers the failure.
    let answer = faulted
        .ask("QUERY LOCK vault")
        .expect("the replica answers the request that breaks it");
    assert!(
        answer.starts_with("ABANDONED"),
        "the operation that made the control plane undurable is refused rather \
         than served: {answer}"
    );

    // And the process ends, with the flood still running against it.
    let refused = faulted.wait_refused();
    let answered = flood.stop();
    assert!(
        refused.status.code().is_some_and(|code| code != 0),
        "a replica that cannot record what it retired must not exit cleanly, \
         because a supervisor reads exit 0 as a reason to restart it: {refused:?}"
    );
    assert!(
        refused.stdout.contains("CONTROL_PLANE_UNPERSISTED"),
        "and must say so on its own lifecycle channel: {:?}",
        refused.stdout
    );
    assert!(
        refused.stdout.contains("FATAL"),
        "and must not end through STOPPED: {:?}",
        refused.stdout
    );
    assert!(
        !refused.stdout.contains("STOPPED"),
        "STOPPED and an unpersisted control plane are mutually exclusive: {:?}",
        refused.stdout
    );
    assert!(
        answered > 0,
        "the flood has to have been real load for this to have proved anything"
    );

    cluster.shutdown();
}

/// A replica refuses the request queued behind the one that broke its control
/// plane, in the same pass.
///
/// **The bound was the wrong thing to be proud of.** The terminal transition
/// runs once per pass, below the client drain, and `submit` and `query` answer
/// their client with a value rather than a `Result` — so a persist failure
/// inside one is *stored* and the state does not move. Everything left of the
/// per-pass budget was then spent admitting work the process already knew it
/// must not do: up to sixty-three further jobs, with writes started, reads
/// served, and `STATUS` answered as if nothing had happened. Bounded fail-open
/// is still fail-open.
///
/// `LOCAL` is the discriminator because it is the one service verb whose success
/// is visible in its own answer: a served one returns a lock view, and a refused
/// one returns `ABANDONED`. It also does not count toward the fault ordinal —
/// only `submit` and `query` do — so the `QUERY` below is deterministically the
/// operation that trips the seam however many local reads precede it.
///
/// Socket write order is deliberately not mistaken for admission order. Every
/// connection has its own reader thread, so a later-written local read can reach
/// the main loop first under load. The injected-fault seam suffixes replies with
/// that loop's ticket: a served local read is legal only when its ticket precedes
/// the query's, while every later ticket must be refused. The baseline assertion
/// before the fault keeps a fixture that stopped reaching the replica at all from
/// passing silently.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_stored_control_plane_failure_refuses_the_requests_queued_behind_it() {
    let mut cluster = ProcessCluster::start("fail-closed-batch", process_config());
    let leader = cluster.wait_for_leader();
    let victim = *cluster
        .live_nodes()
        .iter()
        .find(|node_id| **node_id != leader)
        .expect("a three-replica cluster has a follower");

    cluster.submit_to_leader(open_session(0, 1));
    cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));
    cluster.wait_applied_through(victim, LogIndex(1));

    cluster.kill(victim);
    let mut faulted = cluster.restart_with_control_plane_fault(victim, 1);
    let addr = faulted.client_addr();

    // The control. A local read is served normally right now, so a later refusal
    // is the failure taking effect rather than the fixture never having worked.
    assert!(
        faulted
            .ask("LOCAL LOCK vault")
            .expect("a healthy replica answers a local read")
            .starts_with("OK LOCK"),
        "the fixture only means anything if this replica serves local reads \
         before the fault trips"
    );

    // Every connection established before anything is written, so the sends
    // below are one write each on an open socket.
    let mut breaking = process::QueuedRequest::connect(addr);
    let mut queued: Vec<process::QueuedRequest> = (0..QUEUED_BEHIND)
        .map(|_| process::QueuedRequest::connect(addr))
        .collect();

    breaking.send("QUERY LOCK vault");
    for request in &mut queued {
        request.send("LOCAL LOCK vault");
    }

    let breaking_answer = breaking
        .recv()
        .expect("the replica answers the request that breaks it");
    assert!(
        breaking_answer.starts_with("ABANDONED"),
        "the operation that made the control plane undurable is refused rather \
         than served: {breaking_answer}"
    );
    let breaking_ticket = harness_ticket(&breaking_answer);
    let mut observed_behind = false;
    for (index, request) in queued.iter_mut().enumerate() {
        // A connection the replica closed as it exits is not a served read, and
        // is the ordinary ending for a request that arrived after the loop.
        let Ok(answer) = request.recv() else {
            observed_behind = true;
            continue;
        };
        if answer.starts_with("OK LOCK") {
            let ticket = harness_ticket(&answer);
            assert!(
                ticket < breaking_ticket,
                "request {index} was served after the replica knew its control \
                 plane was not durable: query ticket {breaking_ticket}, local \
                 ticket {ticket}, answer {answer}"
            );
        } else {
            observed_behind = true;
        }
    }
    assert!(
        observed_behind,
        "the fixture did not place any request behind the breaking query"
    );

    let refused = faulted.wait_refused();
    assert!(
        refused.status.code().is_some_and(|code| code != 0),
        "and the process still exits nonzero: {refused:?}"
    );

    cluster.shutdown();
}

/// `STATUS` stops claiming readiness once the replica will not serve again.
///
/// **Readiness is one-way, and that is right for readiness and wrong for
/// this.** `Replica::is_ready` says this replica has applied everything it knows
/// to be committed; it never falls, because catching up is not something a
/// replica un-does. Reporting it for a `State::Failed` replica made a process on
/// its way to a nonzero exit answer `STATUS ready`, and a supervisor that polls
/// readiness rather than watching exit codes saw nothing wrong at all.
///
/// The terminal window is short — the loop answers at most one job per remaining
/// pass and then ends — so a flood is the observer that can see it: it keeps a
/// request in flight continuously, which is exactly what is needed to be
/// answered *during* the window rather than before or after it. The flood starts
/// only after readiness, so any `abandoned` answer it counts is the transition
/// and not the opening state.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_terminal_replica_stops_reporting_itself_ready() {
    let mut cluster = ProcessCluster::start("terminal-status", process_config());
    let leader = cluster.wait_for_leader();
    let victim = *cluster
        .live_nodes()
        .iter()
        .find(|node_id| **node_id != leader)
        .expect("a three-replica cluster has a follower");

    cluster.submit_to_leader(open_session(0, 1));
    cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));
    cluster.wait_applied_through(victim, LogIndex(1));

    cluster.kill(victim);
    let mut faulted = cluster.restart_with_control_plane_fault(victim, 1);
    let flood = process::StatusFlood::start(faulted.client_addr(), FLOOD_CONNECTIONS);

    assert!(
        faulted
            .ask("QUERY LOCK vault")
            .expect("the replica answers the request that breaks it")
            .starts_with("ABANDONED"),
        "the operation that made the control plane undurable is refused"
    );

    let refused = faulted.wait_refused();
    let abandoned = flood.abandoned();
    let answered = flood.stop();

    assert!(
        abandoned > 0,
        "no STATUS reported the replica abandoned, so a supervisor polling \
         readiness saw a healthy process all the way to its nonzero exit \
         ({answered} answered)"
    );
    assert!(
        refused.status.code().is_some_and(|code| code != 0),
        "and the process still exits nonzero under the flood: {refused:?}"
    );

    cluster.shutdown();
}

/// The peer-control-plane checkpoint is durable, and survives a restart.
///
/// Rafter opens no files, so the retirement record and the fence obligations a
/// driver derives are its embedder's to persist — and a process that did not
/// would come back with no high-water mark, stop retrying a refused fence, and
/// let an identity a committed removal spent be allocated again. Raft cannot
/// give either fact back: retirement is the *difference* between two committed
/// configurations, a restarted process sees only the latest one, and compaction
/// erases the rest.
///
/// **This cluster's membership never changes**, which is what this test can and
/// cannot show. It cannot produce a removal without violating the contract in
/// `CONTRACT.md`, so what a removal costs is pinned where a driver can actually
/// be destroyed and rebuilt — `crates/rafter-service/tests/transport_service_state.rs`.
/// What it shows here is that the *plumbing is real* in a process that can be
/// killed: the file is published, it names this cluster's committed
/// configuration and its high-water mark, and a replica that restarts comes back
/// with them rather than with nothing.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn the_peer_control_plane_checkpoint_is_durable_across_a_restart() {
    let mut cluster = ProcessCluster::start("control-plane-checkpoint", process_config());
    let leader = cluster.wait_for_leader();
    let victim = *cluster
        .live_nodes()
        .iter()
        .find(|node_id| **node_id != leader)
        .expect("a three-replica cluster has a follower");

    cluster.submit_to_leader(open_session(0, 1));
    let token = cluster
        .submit_to_leader(submit(0, 1, 1, acquire("vault", 10)))
        .acquired_token();

    let checkpoint_path =
        process::NodeProcess::node_dir(cluster.root(), victim).join("control-plane");
    let before = std::fs::read_to_string(&checkpoint_path)
        .expect("a serving replica has published its control-plane checkpoint");
    assert!(
        before.starts_with("rafter-lock-control-plane 7\n"),
        "the file names its own format so a later shape is a refusal: {before:?}"
    );
    assert!(
        before.contains("group 1"),
        "and names the group it describes, so a process hosting several replicas \
         cannot cross two files: {before:?}"
    );
    assert!(
        before
            .lines()
            .next_back()
            .is_some_and(|line| line
                .strip_prefix("crc32 ")
                .is_some_and(|digits| digits.len() == 8
                    && digits.chars().all(|digit| digit.is_ascii_hexdigit()))),
        "and is sealed, so a flipped bit is a refusal rather than a lowered \
         mark: {before:?}"
    );
    assert!(
        before.contains("high_water 3"),
        "the mark is the highest id this group has ever committed: {before:?}"
    );
    assert!(
        before.contains("live 1 2 3"),
        "and the live committed set is this cluster's configuration: {before:?}"
    );
    assert!(
        !before.contains("fences"),
        "and carries no obligation ledger: retirement is the mark read against \
         the live set, and version 6 dropped the line that used to say what a \
         link layer had accepted: {before:?}"
    );
    assert!(
        before.contains("contradicted -"),
        "and says it has not been contradicted, which is a fact rather than an \
         absence: a record with no field for it cannot be told from one written \
         past a fork it could not record: {before:?}"
    );
    // **The position travels with the membership it dates**, and a real replica
    // is where that stops being a type-level claim. This cluster never
    // reconfigures, so nothing crosses a configuration entry after the opening
    // one and the only observations are the ones adoption and the
    // committed-membership comparison produce — but the record still has to say
    // *where* it looked, because a later record's membership is the one a join
    // believes and there is nothing else to decide it by.
    let observed_before = checkpoint_position(&before)
        .expect("a serving replica records where it observed the committed configuration");

    cluster.kill(victim);
    cluster.restart(victim);
    cluster.wait_applied_through(victim, LogIndex(1));

    let after = std::fs::read_to_string(&checkpoint_path)
        .expect("the restarted replica republishes what it restored");
    // The record is compared line by line rather than the whole file, because
    // one line is *expected* to move. The two facts below are what a restart
    // must not re-derive differently; the position is where this replica last
    // looked, so it advances as the commit index does and would make a byte
    // comparison assert the opposite of the contract.
    for fact in ["high_water", "live"] {
        assert_eq!(
            checkpoint_line(&after, fact),
            checkpoint_line(&before, fact),
            "the restored driver re-derived `{fact}` differently, rather than \
             carrying what it was handed:\nbefore {before:?}\nafter  {after:?}"
        );
    }
    let observed_after =
        checkpoint_position(&after).expect("the restarted replica records one too");
    assert!(
        observed_after >= observed_before,
        "the current state's position went backwards across a restart, which is \
         how an older observation outranks a newer one and reads what the newer \
         one added as removed: {observed_before} then {observed_after}"
    );

    // The replica is a full member again, which is the point of restoring the
    // mark rather than merely storing it: a driver that read its own checkpoint
    // as a set of retirements would refuse every replica in its own cluster —
    // the mark names them all — and this write could not reach a quorum.
    let leader = cluster.wait_for_leader();
    let renewed = cluster.submit(leader, submit(0, 1, 2, renew("vault", token.get(), 30)));
    assert_operation(
        &renewed,
        OperationResult::Renewed {
            token,
            expiry: support::time(30),
        },
    );
    cluster.wait_applied_through(victim, LogIndex(2));

    // No history well-formedness check here, deliberately. That assertion is a
    // structural check on the *recorder* rather than on the replicas, and it
    // belongs with the suite whose subject is the lock protocol; this suite's
    // writes exist only to give the replicas something to commit.
    cluster.shutdown();
}

/// Re-seals a control-plane record with its `contradicted` line set.
///
/// **A hand-written file is the only seam that reaches this state from
/// outside.** A contradiction needs two irreconcilable claims about one committed
/// membership, and a correct runtime cannot produce them — the driver-level
/// suites reach it by scripting a runtime that breaks its own contract, which is
/// not something a spawned process can be asked to do. What *is* reachable is the
/// durable half: a record carrying the marker is exactly what the previous
/// incarnation of a contradicted replica leaves behind, and reading one back is
/// the behaviour under test.
///
/// The checksum is recomputed, because a file that failed its seal would be
/// refused before any of this mattered and the test would prove nothing about the
/// marker.
fn contradicted(text: &str, through: u64) -> String {
    let body = text
        .split_inclusive('\n')
        .take_while(|line| !line.starts_with("crc32 "))
        .collect::<String>();
    assert!(
        body.contains("\ncontradicted -\n"),
        "the fixture rewrites an uncontradicted record: {text:?}"
    );
    let marked = body.replacen(
        "\ncontradicted -\n",
        &format!("\ncontradicted {through}\n"),
        1,
    );
    format!(
        "{marked}crc32 {:08x}\n",
        rafter_reference_fenced_lock::store::crc32(marked.as_bytes())
    )
}

/// A replica whose control-plane record records a contradiction refuses to serve
/// and exits nonzero.
///
/// **The terminal state used to last exactly as long as the process that found
/// it.** A driver whose licensing inputs contradict each other stops serving and
/// publishes nothing, but the record it wrote said nothing about that — so a
/// crash and a restart produced a clean driver, and where the rebuilt runtime
/// agreed at the record's position the replica went straight back to serving. The
/// marker is what makes the refusal outlive the incarnation.
///
/// **And the process used to report itself ready through all of it.** `STATUS`
/// rendered the application-floor readiness bit without consulting the driver at
/// all, and the loop moved to its terminal state only for persistence failures.
/// A supervisor polling readiness saw a healthy replica; a supervisor watching
/// exit codes saw nothing, because the process never exited.
///
/// The readiness half is asserted through the `READY` line rather than through a
/// client flood, and that is a stronger observation rather than a weaker one:
/// `READY` and the `STATUS` readiness word are the same `Replica::is_ready` call,
/// so a run that never announced readiness is a run in which `STATUS` could not
/// have claimed it. A flood would only be able to sample the few milliseconds
/// before the exit.
///
/// Kill window: the replica is killed while idle and the file is rewritten by the
/// test rather than by the kill, so both windows are shut — exactly as in
/// `a_deleted_control_plane_checkpoint_refuses_to_open_as_a_first_boot`.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_contradicted_control_plane_record_stops_the_process() {
    let mut cluster = ProcessCluster::start("contradicted-control-plane", process_config());
    let leader = cluster.wait_for_leader();
    let victim = *cluster
        .live_nodes()
        .iter()
        .find(|node_id| **node_id != leader)
        .expect("a three-replica cluster has a follower");

    cluster.submit_to_leader(open_session(0, 1));
    cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));
    cluster.wait_applied_through(victim, LogIndex(1));

    let checkpoint_path =
        process::NodeProcess::node_dir(cluster.root(), victim).join("control-plane");
    cluster.kill(victim);

    let before = std::fs::read_to_string(&checkpoint_path)
        .expect("a serving replica has published its control-plane checkpoint");
    let observed = checkpoint_position(&before)
        .expect("a serving replica records where it observed the committed configuration");
    std::fs::write(&checkpoint_path, contradicted(&before, observed))
        .expect("the marker is written where the replica's own record is");

    let refused = cluster.restart_expecting_failure(victim);
    assert!(
        refused.status.code().is_some_and(|code| code != 0),
        "a replica whose control plane is contradicted must not exit cleanly, \
         because a supervisor reads exit 0 as a reason to restart it: {refused:?}"
    );
    assert!(
        refused.stdout.contains("CONTROL_PLANE_CONTRADICTED"),
        "and must say so on its own lifecycle channel, apart from the \
         unpersisted line: {:?}",
        refused.stdout
    );
    assert!(
        refused.stdout.contains("FATAL"),
        "and must not end through STOPPED: {:?}",
        refused.stdout
    );
    assert!(
        !refused.stdout.contains("STOPPED"),
        "STOPPED and a contradicted control plane are mutually exclusive: {:?}",
        refused.stdout
    );
    assert!(
        !refused
            .stdout
            .lines()
            .any(|line| line.starts_with("READY ")),
        "and the readiness gate never opened, so no supervisor polling STATUS \
         could have read this replica as ready: {:?}",
        refused.stdout
    );
    assert!(
        !refused.stdout.contains("could not be made durable"),
        "the refusal names the contradiction rather than a persistence failure: \
         {:?}",
        refused.stdout
    );

    cluster.shutdown();
}
