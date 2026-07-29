//! The ledger as three real processes: election, failover, restart, readiness.
//!
//! **This suite is integration evidence only.** `docs/reference-consumers.md`
//! defines two process-composition levels, and this is the first: it proves
//! process boundaries, routing, kill and restart recovery, client session
//! deduplication across a real leader failure, and that the readiness gate
//! gates. It proves nothing about production composition, because the link
//! between these processes authenticates nothing and the client protocol
//! authenticates nothing. No test here closes the 1.0 production-composition
//! criterion, and none of them claims to.
//!
//! # Running it
//!
//! The suite is `#[ignore]`d by default and runs on request:
//!
//! ```text
//! scripts/reference-source-check -- --test process_cluster -- --ignored
//! ```
//!
//! Ignoring it by default is the program document's own lane split rather than
//! a hedge: durable process tests belong to the main/nightly lane, while the
//! every-PR lane wants a package build and the deterministic suites.
//! `--all-targets` still compiles this file and the `ledger-node` binary in
//! both dependency modes, so a consumer that stopped building is caught by
//! every lane; only the running of it is deferred.
//!
//! # What each test does about the kill window
//!
//! Every test that kills a process has to say what it assumes about *where*
//! the kill landed, because a real `SIGKILL` cannot be aimed:
//!
//! - Killing an idle leader: the window is empty, and the test asserts the
//!   exact cached result a retry must observe.
//! - Killing a leader with a write in flight: the window is real and both
//!   readings are possible, so the test asserts the property that survives
//!   both — the mutation took effect exactly once.
//! - Killing a follower mid-write: the survivors hold a quorum, so the write's
//!   outcome is not in question; what is in question is the killed replica's
//!   durable floor, and the test asserts it recovered one and caught up past it.
//! - Killing every replica: each one dies wherever it was, and the test asserts
//!   that nothing acknowledged before the kill executed a second time after it.

use std::path::Path;

mod support;

#[path = "support/process.rs"]
mod process;
#[path = "support/scratch.rs"]
mod scratch;

use rafter::{LogIndex, NodeId};
use rafter_reference_ledger::{
    check_linearizable,
    store::{raw_journal, COMMIT_LEN},
    AccountId, ApplyDisposition, LedgerConfig, LedgerQuery, LedgerResponse, Mutation,
    MutationResult,
};

use process::{NodeProcess, ProcessCluster, QueryOutcome, SubmitOutcome};
use support::{amount, config, execute, open_session};

/// The bounds every process test runs under.
///
/// Small on purpose: the journal image is a whole state snapshot, so a large
/// bound would make every transaction larger without testing anything more.
fn process_config() -> LedgerConfig {
    config(4, 8)
}

/// Damages the last committed journal image without changing its framing.
///
/// Recovery therefore reaches a complete frame whose checksum it cannot verify,
/// which deterministically requires the explicit repair path. Both refusal and
/// escalation tests use this exact residue so they cannot drift into proving
/// different failures.
fn corrupt_journal_into_needs_repair(cluster_root: &Path, node_id: NodeId) {
    let app_dir = NodeProcess::node_dir(cluster_root, node_id).join("app");
    let mut bytes = raw_journal::read(&app_dir).expect("the killed replica's journal reads back");
    let target = bytes
        .len()
        .checked_sub(COMMIT_LEN + 3)
        .expect("the journal contains a committed image to corrupt");
    bytes[target] ^= 0xFF;
    raw_journal::write(&app_dir, &bytes).expect("the journal rewrites");
}

fn account(id: u64) -> AccountId {
    AccountId::new(id)
}

fn deposit(account_id: u64, value: u64) -> Mutation {
    Mutation::Deposit {
        account_id: account(account_id),
        amount: amount(value),
    }
}

fn open_account(account_id: u64) -> Mutation {
    Mutation::OpenAccount {
        account_id: account(account_id),
    }
}

fn transfer(from: u64, to: u64, value: u64) -> Mutation {
    Mutation::Transfer {
        from: account(from),
        to: account(to),
        amount: amount(value),
    }
}

/// Asserts a submission committed and returns its disposition and response.
#[track_caller]
fn applied(outcome: &SubmitOutcome) -> (ApplyDisposition, LedgerResponse) {
    match outcome {
        SubmitOutcome::Applied {
            disposition,
            response,
        } => (*disposition, response.clone()),
        other => panic!("expected a committed command, observed {other:?}"),
    }
}

/// Asserts a mutation committed with an exact result.
#[track_caller]
fn assert_mutation(outcome: &SubmitOutcome, expected: &MutationResult) {
    let (_, response) = applied(outcome);
    assert_eq!(
        response,
        LedgerResponse::Mutation(expected.clone()),
        "the replicated response must be exactly the contract's result"
    );
}

/// Asserts the recorded history admits a legal real-time ordering.
///
/// The checker is the same black-box one the deterministic suites use, and it
/// reads only the history: it never inspects a replica, a log, or an applied
/// index. A process history is therefore checked by exactly the code an
/// external user recording the same client-visible events could run.
#[track_caller]
fn assert_linearizable(cluster: &ProcessCluster) {
    let report = check_linearizable(cluster.config(), cluster.history()).unwrap_or_else(|error| {
        panic!("the recorded process history is not linearizable: {error}")
    });
    assert!(
        report.checked_operations() > 0,
        "a process history that placed no operations proves nothing"
    );
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn three_processes_elect_a_leader_and_serve_the_ledger() {
    let mut cluster = ProcessCluster::start("elect-and-serve", process_config());

    let leader = cluster.wait_for_leader();
    assert_eq!(
        leader,
        NodeId(1),
        "in a first election nobody is sticky, so the shortest election timeout wins"
    );
    assert_eq!(
        cluster.wait_for_agreed_leader(),
        leader,
        "every replica routes clients to the replica that actually leads"
    );
    for node_id in cluster.live_nodes() {
        let status = cluster
            .status(node_id)
            .unwrap_or_else(|| panic!("replica {} answers STATUS", node_id.0));
        assert!(
            status.ready,
            "every replica announced readiness before serving"
        );
    }

    let session = cluster.submit_to_leader(&open_session(0, 1));
    assert_eq!(
        applied(&session).0,
        ApplyDisposition::SessionOpened,
        "an unused client slot accepts its first epoch"
    );

    assert_mutation(
        &cluster.submit_to_leader(&execute(0, 1, 1, open_account(7))),
        &MutationResult::AccountOpened,
    );
    assert_mutation(
        &cluster.submit_to_leader(&execute(0, 1, 2, deposit(7, 50))),
        &MutationResult::Deposited { balance: 50 },
    );
    assert_mutation(
        &cluster.submit_to_leader(&execute(0, 1, 3, open_account(8))),
        &MutationResult::AccountOpened,
    );
    assert_mutation(
        &cluster.submit_to_leader(&execute(0, 1, 4, transfer(7, 8, 20))),
        &MutationResult::Transferred {
            from_balance: 30,
            to_balance: 20,
        },
    );

    assert_eq!(
        cluster
            .query_leader(LedgerQuery::GetAccount {
                account_id: account(7)
            })
            .account_balance(),
        Some(30),
        "a linearizable read observes the transfer that preceded it"
    );
    let summary = cluster
        .query_leader(LedgerQuery::GetLedgerSummary)
        .summary()
        .expect("the summary query answers");
    assert_eq!(summary.open_accounts, 2);
    assert_eq!(
        summary.total_balance, summary.successful_deposits,
        "a transfer preserves total balance"
    );

    assert_linearizable(&cluster);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_session_retry_after_the_leader_is_killed_returns_the_cached_result() {
    // The kill window here is empty by construction: every command below was
    // acknowledged before the leader was killed, so the retry must observe the
    // cached result and must not execute anything.
    let mut cluster = ProcessCluster::start("dedup-across-failover", process_config());
    let leader = cluster.wait_for_leader();

    cluster.submit_to_leader(&open_session(0, 1));
    cluster.submit_to_leader(&execute(0, 1, 1, open_account(7)));
    assert_mutation(
        &cluster.submit_to_leader(&execute(0, 1, 2, deposit(7, 50))),
        &MutationResult::Deposited { balance: 50 },
    );

    cluster.kill(leader);
    let failover_leader = cluster.wait_for_leader();
    assert!(
        cluster.live_nodes().contains(&failover_leader),
        "a surviving replica takes over"
    );
    assert_ne!(
        failover_leader, leader,
        "the killed replica cannot still be leading"
    );

    let retry = cluster.submit_to_leader(&execute(0, 1, 2, deposit(7, 50)));
    assert_eq!(
        applied(&retry).0,
        ApplyDisposition::Replayed,
        "an exact retry of the highest completed sequence is replayed, not executed"
    );
    assert_mutation(&retry, &MutationResult::Deposited { balance: 50 });

    let summary = cluster
        .query_leader(LedgerQuery::GetLedgerSummary)
        .summary()
        .expect("the summary query answers");
    assert_eq!(
        summary.total_balance, 50,
        "the deposit took effect exactly once across the failover"
    );
    assert_eq!(summary.successful_deposits, 50);

    assert_linearizable(&cluster);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_write_lost_to_a_leader_kill_takes_effect_exactly_once_after_its_retry() {
    // The kill window here is real: the deposit below may or may not have
    // committed when its leader died, and the client cannot tell. Every
    // assertion is therefore one that holds for both readings.
    let mut cluster = ProcessCluster::start("unknown-outcome-retry", process_config());
    let leader = cluster.wait_for_leader();

    cluster.submit_to_leader(&open_session(0, 1));
    cluster.submit_to_leader(&execute(0, 1, 1, open_account(7)));
    assert_mutation(
        &cluster.submit_to_leader(&execute(0, 1, 2, deposit(7, 50))),
        &MutationResult::Deposited { balance: 50 },
    );

    let in_flight = cluster.begin_submit(leader, &execute(0, 1, 3, deposit(7, 30)));
    cluster.kill(leader);
    // Every terminal outcome is legal here and the client cannot influence
    // which one it gets: the command may have committed before the kill, may
    // have been refused before it was ever appended, or may simply have taken
    // its answer with the connection. What the client does about it is the same
    // in all three cases, which is the whole point of a request identity.
    let lost = cluster.resolve_submit(in_flight);

    // Retrying the *same* request identity is the contract's answer, and it is
    // the only safe one: a fresh identity would execute a second deposit if the
    // first had in fact committed.
    let retry = cluster.submit_to_leader(&execute(0, 1, 3, deposit(7, 30)));
    assert_mutation(&retry, &MutationResult::Deposited { balance: 80 });
    assert!(
        matches!(
            applied(&retry).0,
            ApplyDisposition::Applied | ApplyDisposition::Replayed
        ),
        "the retry either executed the never-committed command or replayed the committed one; \
         the lost attempt reported {lost:?}"
    );

    let summary = cluster
        .query_leader(LedgerQuery::GetLedgerSummary)
        .summary()
        .expect("the summary query answers");
    assert_eq!(
        summary.successful_deposits, 80,
        "the retried identity contributed one deposit, not two"
    );
    assert_eq!(summary.total_balance, 80);

    assert_linearizable(&cluster);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_replica_killed_mid_write_recovers_from_its_journal_and_rejoins() {
    // The killed replica is a follower, so the surviving two hold a quorum and
    // the writes below are never in doubt. What the kill window decides is how
    // far the follower's own journal got, and the test asserts only that it
    // recovered a durable floor and then caught up past it.
    let mut cluster = ProcessCluster::start("restart-and-catch-up", process_config());
    let leader = cluster.wait_for_leader();

    cluster.submit_to_leader(&open_session(0, 1));
    cluster.submit_to_leader(&execute(0, 1, 1, open_account(7)));
    cluster.submit_to_leader(&execute(0, 1, 2, deposit(7, 20)));

    let in_flight = cluster.begin_submit(leader, &execute(0, 1, 3, deposit(7, 30)));
    cluster.kill(NodeId(3));
    // The surviving two hold a quorum, but that does not make this write's
    // outcome certain: a leader under load can still lose leadership and drop
    // the proposal. Retrying the same identity settles it either way, and the
    // balance below is the same under both readings.
    let lost = cluster.resolve_submit(in_flight);
    let settled = cluster.submit_to_leader(&execute(0, 1, 3, deposit(7, 30)));
    assert_eq!(
        applied(&settled).1,
        LedgerResponse::Mutation(MutationResult::Deposited { balance: 50 }),
        "the retried identity deposited once whatever became of the first attempt, \
         which reported {lost:?}"
    );
    assert_mutation(
        &cluster.submit_to_leader(&execute(0, 1, 4, deposit(7, 40))),
        &MutationResult::Deposited { balance: 90 },
    );

    let leader_applied = cluster
        .status(leader)
        .expect("the leader answers STATUS")
        .applied;

    let recovered_floor = cluster.restart(NodeId(3));
    assert!(
        recovered_floor > LogIndex::ZERO,
        "the restarted replica recovered a durable applied floor from its own journal"
    );
    assert!(
        recovered_floor.0 <= leader_applied,
        "a replica cannot recover past what the cluster committed"
    );

    cluster.wait_applied_through(NodeId(3), LogIndex(leader_applied));
    assert_eq!(
        cluster
            .local_read(
                NodeId(3),
                LedgerQuery::GetAccount {
                    account_id: account(7)
                }
            )
            .account_balance(),
        Some(90),
        "the rejoined replica's own state agrees with the cluster it caught up to"
    );

    assert_linearizable(&cluster);
    cluster.shutdown();
}

/// The other restart: the one that shortens the journal and serves anyway.
///
/// `--repair-app-store` is not the only way this process can lose acknowledged
/// work, though the documentation said so for several revisions. A journal
/// whose tail reached the medium as zeros is truncated by the plain `open`,
/// with no flag and no operator, and the deleted bytes may be committed frames
/// a zeroed region erased. `TornTail::is_truncatable_residue` argues why that
/// trade is made rather than gated; what is owed here is that it reaches
/// somebody, which is the `possibly_committed=` field.
///
/// The zeros are **appended** rather than written over a committed frame, and
/// that is deliberate. Appended zeros lose nothing. An erased final frame loses
/// a transaction. They are the same bytes, the store cannot tell them apart,
/// and the announcement is identical for both — which is the whole reason the
/// announcement has to exist. A test that only ever damaged the harmless one
/// would be checking the shape of a line; this checks that the process says the
/// same thing in the case where it matters, because it has no way to say less.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_truncated_zero_tail_is_announced_on_a_plain_restart() {
    let mut cluster = ProcessCluster::start("zero-tail-announced", process_config());
    cluster.submit_to_leader(&open_session(0, 1));
    cluster.submit_to_leader(&execute(0, 1, 1, open_account(7)));
    cluster.submit_to_leader(&execute(0, 1, 2, deposit(7, 60)));

    let victim = NodeId(3);
    cluster.wait_applied_through(victim, LogIndex(1));
    cluster.kill(victim);

    let app_dir = NodeProcess::node_dir(cluster.root(), victim).join("app");
    let mut bytes = raw_journal::read(&app_dir).expect("the killed replica's journal reads back");
    bytes.extend(std::iter::repeat_n(0_u8, 96));
    raw_journal::write(&app_dir, &bytes).expect("the journal rewrites");

    let mut restarted = NodeProcess::spawn(cluster.root(), victim, cluster.config());
    let announced = restarted.wait_for_line("RECOVERED");
    assert!(
        announced.contains("possibly_committed=96"),
        "a restart that shortened the journal without proof must say how much, \
         observed {announced:?}"
    );
    assert!(
        !restarted.has_announced("NEEDS_REPAIR"),
        "a zero tail must not need an operator; refusing it would send them to the flag"
    );
    assert!(
        !restarted.has_announced("REPAIRED"),
        "no flag was passed, so nothing may report itself as a repair"
    );
    restarted.wait_ready();
    cluster.adopt(victim, restarted);

    let leader = cluster.wait_for_agreed_leader();
    cluster.submit(leader, &execute(0, 1, 3, deposit(7, 5)));
    cluster.wait_applied_through(victim, LogIndex(1));
    let summary = cluster
        .query_leader(LedgerQuery::GetLedgerSummary)
        .summary()
        .expect("the summary query answers");
    assert_eq!(summary.total_balance, 65);

    assert_linearizable(&cluster);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn readiness_refuses_a_journal_that_needs_an_operator_decision() {
    // The kill window does not matter here: the replica is killed while idle,
    // and what the test corrupts afterwards is a frame the surviving quorum
    // already committed, so there is nothing indeterminate about what was lost.
    //
    // This is the process half of the store's fail-closed rule. A journal
    // holding a region this build cannot read may still open at a shorter
    // history, and that shorter history may be missing transactions this
    // replica already answered a client with. The store refuses; the readiness
    // gate is where that refusal becomes visible, and it must not be reachable
    // by restarting.
    let mut cluster = ProcessCluster::start("needs-repair", process_config());
    cluster.submit_to_leader(&open_session(0, 1));
    cluster.submit_to_leader(&execute(0, 1, 1, open_account(7)));
    cluster.submit_to_leader(&execute(0, 1, 2, deposit(7, 60)));

    let victim = NodeId(3);
    cluster.wait_applied_through(victim, LogIndex(1));
    cluster.kill(victim);

    // Corrupt the image of a frame the replica had already committed. Only the
    // checksum changes; the framing is untouched, so recovery reaches the frame
    // and cannot verify it.
    corrupt_journal_into_needs_repair(cluster.root(), victim);

    // Restarting does not clear it, and the replica says so rather than sitting
    // silently at NOTREADY forever.
    let mut refused = NodeProcess::spawn(cluster.root(), victim, cluster.config());
    let announced = refused.wait_for_line("NEEDS_REPAIR");
    assert!(
        announced.contains("--repair-app-store"),
        "the refusal must name the way out, observed {announced:?}"
    );
    assert!(
        !refused.has_announced("READY"),
        "a replica whose journal will not open must never announce readiness"
    );
    assert!(
        !refused.has_announced("REPAIRED"),
        "a plain restart must never repair anything"
    );
    drop(refused);

    // The surviving quorum was serving throughout, which is what makes the
    // refusal safe to take: one replica declining to guess is not an outage.
    let leader = cluster.wait_for_agreed_leader();
    assert_ne!(leader, victim, "the refusing replica cannot be the leader");
    cluster.submit(leader, &execute(0, 1, 3, deposit(7, 5)));

    // Repairing is an explicit act, and it reports what it discarded.
    let mut repaired = NodeProcess::spawn_repairing(cluster.root(), victim, cluster.config());
    let report = repaired.wait_for_line("REPAIRED");
    assert!(
        report.contains("discarded"),
        "a repair must say what it discarded, observed {report:?}"
    );
    repaired.wait_ready();
    cluster.adopt(victim, repaired);

    // And the repaired replica catches up through ordinary replication rather
    // than through anything the repair did, so the cluster converges.
    cluster.submit(leader, &execute(0, 1, 4, deposit(7, 5)));
    cluster.wait_applied_through(victim, LogIndex(1));
    let summary = cluster
        .query_leader(LedgerQuery::GetLedgerSummary)
        .summary()
        .expect("the summary query answers");
    assert_eq!(
        summary.total_balance, 70,
        "the quorum's history is intact after one replica repaired its own journal"
    );

    assert_linearizable(&cluster);
    cluster.shutdown();
}

/// The cluster harness reaches repair only after a plain restart refuses.
///
/// The cluster-wide kill test permits this branch because its crash point is
/// real-time dependent. This test makes the same branch an executable contract:
/// deterministic damage produces `NEEDS_REPAIR`, `restart` records that refusal,
/// repairs once under the named mode, and the replica rejoins.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn restart_escalates_only_after_a_journal_requests_repair() {
    let mut cluster = ProcessCluster::start("restart-escalation", process_config());
    cluster.submit_to_leader(&open_session(0, 1));
    cluster.submit_to_leader(&execute(0, 1, 1, open_account(7)));
    cluster.submit_to_leader(&execute(0, 1, 2, deposit(7, 60)));

    let victim = NodeId(3);
    cluster.wait_applied_through(victim, LogIndex(1));
    cluster.kill(victim);
    corrupt_journal_into_needs_repair(cluster.root(), victim);

    let recovered_floor = cluster.restart(victim);
    assert!(
        recovered_floor >= LogIndex(1),
        "repair retains the last intact committed application image"
    );
    assert_eq!(
        cluster.escalations().len(),
        1,
        "the deterministic refusal must exercise exactly one escalation"
    );
    let escalation = &cluster.escalations()[0];
    assert_eq!(escalation.node_id, victim);
    assert_eq!(escalation.mode, "repair");
    assert!(
        escalation.refusal.contains("--repair-app-store"),
        "repair must follow the process's explicit refusal, observed {escalation:?}"
    );
    assert!(
        cluster.node_mut(victim).has_announced("REPAIRED"),
        "the escalated process must announce the repair it performed"
    );
    cluster.wait_applied_through(victim, LogIndex(1));

    assert_linearizable(&cluster);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn readiness_refuses_service_until_recovery_completes() {
    // There is no kill window to reason about until the last step, because the
    // refusal being tested is reached without any kill: a second process for an
    // owned directory cannot recover, so it cannot serve. That makes the
    // negative assertion deterministic rather than a race against recovery.
    let mut cluster = ProcessCluster::start("readiness-gates", process_config());
    cluster.submit_to_leader(&open_session(0, 1));
    cluster.submit_to_leader(&execute(0, 1, 1, open_account(7)));
    cluster.submit_to_leader(&execute(0, 1, 2, deposit(7, 60)));

    let mut contender = cluster.spawn_contender(NodeId(2));
    contender.wait_for_line("WAITING_FOR_OWNERSHIP");
    assert!(
        !contender.has_announced("READY"),
        "a replica that has not recovered must not announce readiness"
    );

    let refused_submit = contender
        .ask("SUBMIT 0 1 3 DEPOSIT 7 5")
        .expect("a recovering replica still answers its client port");
    assert!(
        refused_submit.starts_with("NOTREADY "),
        "a recovering replica must refuse to replicate, observed {refused_submit:?}"
    );
    let refused_query = contender
        .ask("QUERY ACCOUNT 7")
        .expect("a recovering replica still answers its client port");
    assert!(
        refused_query.starts_with("NOTREADY "),
        "a recovering replica must refuse to read, observed {refused_query:?}"
    );
    let status = contender.ask("STATUS").expect("STATUS is never gated");
    assert!(
        status.starts_with("STATUS recovering "),
        "readiness must be observable while it is closed, observed {status:?}"
    );
    // The refusals above are asserted rather than recorded. A request the
    // replica never handed to `rafter-app` constrains no ordering, so the
    // checker would discharge it without searching; asserting the exact refusal
    // is the stronger statement.

    // Only now is the incumbent killed, which is what lets the contender finish
    // recovering. The gate opens because recovery completed, not because time
    // passed.
    cluster.kill(NodeId(2));
    let recovered_floor = contender.wait_ready();
    assert!(
        recovered_floor > LogIndex::ZERO,
        "the replacement recovered the incumbent's durable state"
    );

    // The gate now answers the request it previously refused. It answers with
    // this replica's *own* recovered state, which is the exact scope of the
    // claim: readiness means recovery finished, not that this replica has
    // caught up with the cluster. A local read here may legitimately be behind,
    // and asserting a current balance at this instant would be asserting
    // something readiness never promised.
    let served = contender
        .ask("LOCAL ACCOUNT 7")
        .expect("a ready replica serves");
    assert!(
        served.starts_with("OK ACCOUNT 7 "),
        "the request that was refused before recovery is answered after it, observed {served:?}"
    );

    cluster.adopt(NodeId(2), contender);
    assert_mutation(
        &cluster.submit_to_leader(&execute(0, 1, 3, deposit(7, 5))),
        &MutationResult::Deposited { balance: 65 },
    );
    // Currency is a separate condition from readiness, and it is waited for
    // separately.
    let leader = cluster.wait_for_leader();
    let leader_applied = cluster
        .status(leader)
        .expect("the leader answers STATUS")
        .applied;
    cluster.wait_applied_through(NodeId(2), LogIndex(leader_applied));
    assert_eq!(
        cluster
            .local_read(
                NodeId(2),
                LedgerQuery::GetAccount {
                    account_id: account(7)
                }
            )
            .account_balance(),
        Some(65),
        "once caught up, the recovered replica holds the cluster's state"
    );

    assert_linearizable(&cluster);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn acknowledged_commands_never_re_execute_after_a_cluster_wide_restart() {
    // Every replica is killed wherever it happened to be, so each one recovers
    // from its own crash window independently. The assertion is the one the
    // contract makes about all of them together: an acknowledged command is
    // never executed a second time.
    let mut cluster = ProcessCluster::start("cluster-wide-restart", process_config());

    cluster.submit_to_leader(&open_session(0, 1));
    cluster.submit_to_leader(&execute(0, 1, 1, open_account(7)));
    cluster.submit_to_leader(&execute(0, 1, 2, deposit(7, 50)));
    cluster.submit_to_leader(&execute(0, 1, 3, open_account(8)));
    assert_mutation(
        &cluster.submit_to_leader(&execute(0, 1, 4, transfer(7, 8, 20))),
        &MutationResult::Transferred {
            from_balance: 30,
            to_balance: 20,
        },
    );

    let before = cluster
        .query_leader(LedgerQuery::GetLedgerSummary)
        .summary()
        .expect("the summary query answers");

    for node_id in cluster.live_nodes() {
        cluster.kill(node_id);
    }
    for node_id in process::NODE_IDS {
        cluster.restart(node_id);
    }
    // A kill may land after a complete frame is synced but before it is
    // sealed. That branch is timing-dependent, so the test does not require
    // it; when it occurs, it must be an explicit response to the process's
    // refusal rather than an eager repair hidden by the harness.
    for escalation in cluster.escalations() {
        assert_eq!(escalation.mode, "repair");
        assert!(
            escalation.refusal.contains("--repair-app-store"),
            "repair must follow a refusal that names the mode, observed {escalation:?}"
        );
    }

    let restarted_leader = cluster.wait_for_leader();
    assert!(
        process::NODE_IDS.contains(&restarted_leader),
        "a restarted replica leads the recovered cluster"
    );
    let after = cluster
        .query_leader(LedgerQuery::GetLedgerSummary)
        .summary()
        .expect("the summary query answers");
    assert_eq!(
        after, before,
        "a cluster-wide restart reconstructs the exact acknowledged state"
    );

    let retry = cluster.submit_to_leader(&execute(0, 1, 4, transfer(7, 8, 20)));
    assert_eq!(
        applied(&retry).0,
        ApplyDisposition::Replayed,
        "the deduplication cache survived the restart, so the retry replayed"
    );
    assert_mutation(
        &retry,
        &MutationResult::Transferred {
            from_balance: 30,
            to_balance: 20,
        },
    );

    let after_retry = cluster
        .query_leader(LedgerQuery::GetLedgerSummary)
        .summary()
        .expect("the summary query answers");
    assert_eq!(
        after_retry, before,
        "replaying an acknowledged command moved nothing"
    );
    assert!(
        matches!(
            cluster.query_leader(LedgerQuery::GetAccount {
                account_id: account(8)
            }),
            QueryOutcome::Ready(_)
        ),
        "the restarted cluster serves linearizable reads"
    );

    assert_linearizable(&cluster);
    cluster.shutdown();
}
