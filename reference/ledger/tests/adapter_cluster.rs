mod support;

#[path = "support/cluster.rs"]
mod cluster;
#[path = "support/storage.rs"]
mod storage;

use std::collections::BTreeSet;

use rafter::{LogIndex, ProposalRejection};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_ledger::{
    check_linearizable, AccountId, ApplyDisposition, BusinessRejection, Command, HistoryEvent,
    LedgerAdapterError, LedgerConfig, LedgerQuery, LedgerQueryResult, LedgerResponse,
    LedgerStateMachine, Mutation, MutationResult, ReferenceLedger, RequestRejection,
};

use cluster::{LedgerCluster, ProposalOutcome, ReadOutcome};
use support::{amount, config, epoch, execute, open_session, sequence};

const ALPHA: AccountId = AccountId::new(11);
const BETA: AccountId = AccountId::new(12);

#[test]
fn replicated_ledger_traffic_agrees_with_the_independent_oracle() {
    let mut cluster = LedgerCluster::new(config(2, 4));
    let leader = cluster.elect_leader();

    commit(&mut cluster, leader, open_session(0, 1));
    commit(&mut cluster, leader, open_session(1, 1));
    commit(
        &mut cluster,
        leader,
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
    );
    commit(
        &mut cluster,
        leader,
        execute(0, 1, 2, Mutation::OpenAccount { account_id: BETA }),
    );
    commit(
        &mut cluster,
        leader,
        execute(
            0,
            1,
            3,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(30),
            },
        ),
    );
    commit(
        &mut cluster,
        leader,
        execute(
            1,
            1,
            1,
            Mutation::Transfer {
                from: ALPHA,
                to: BETA,
                amount: amount(12),
            },
        ),
    );
    commit(
        &mut cluster,
        leader,
        execute(
            1,
            1,
            2,
            Mutation::Transfer {
                from: BETA,
                to: ALPHA,
                amount: amount(12),
            },
        ),
    );
    commit(
        &mut cluster,
        leader,
        execute(1, 1, 3, Mutation::CloseAccount { account_id: BETA }),
    );
    cluster.settle();

    let replayed = replay_through_oracle(cluster.config(), &cluster.committed_commands(leader));
    let leader_ledger = cluster.state_machine(leader).ledger();
    assert_eq!(
        leader_ledger.view(),
        replayed.view(),
        "the replicated ledger diverged from the independent oracle"
    );
    assert_eq!(leader_ledger.summary(), replayed.summary());
    assert_eq!(
        leader_ledger.summary().total_balance,
        leader_ledger.summary().successful_deposits,
        "supply invariant"
    );
    assert_eq!(leader_ledger.account_balance(ALPHA), Some(30));
    assert_eq!(leader_ledger.account_balance(BETA), None);

    for node_id in cluster.node_ids() {
        assert_eq!(
            cluster.state_machine(node_id).ledger().view(),
            leader_ledger.view(),
            "replica {node_id} diverged"
        );
    }
    // The aggregate invariants above hold for orderings that never happened,
    // so the history gets its own check.
    assert_eq!(assert_linearizable(&cluster), 8);
}

#[test]
fn session_protocol_survives_real_replication() {
    let mut cluster = LedgerCluster::new(config(2, 4));
    let leader = cluster.elect_leader();
    commit(&mut cluster, leader, open_session(0, 1));

    let deposit_setup = execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA });
    commit(&mut cluster, leader, deposit_setup);
    let deposit = execute(
        0,
        1,
        2,
        Mutation::Deposit {
            account_id: ALPHA,
            amount: amount(7),
        },
    );
    let applied = cluster.submit(leader, deposit.clone());
    assert_eq!(applied.disposition(), Some(ApplyDisposition::Applied));

    let replay = cluster.submit(leader, deposit);
    assert_eq!(replay.disposition(), Some(ApplyDisposition::Replayed));
    assert_eq!(replay.response(), applied.response());
    cluster.settle();
    assert_eq!(
        cluster
            .state_machine(leader)
            .ledger()
            .account_balance(ALPHA),
        Some(7),
        "an exact retry must not deposit twice"
    );

    let conflict = cluster.submit(
        leader,
        execute(0, 1, 2, Mutation::CloseAccount { account_id: ALPHA }),
    );
    assert_eq!(
        conflict.response(),
        Some(&LedgerResponse::Rejected(
            RequestRejection::ConflictingRetry
        ))
    );

    let stale = cluster.submit(
        leader,
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
    );
    assert_eq!(
        stale.response(),
        Some(&LedgerResponse::Rejected(RequestRejection::StaleSequence {
            highest: sequence(2)
        }))
    );

    let gap = cluster.submit(
        leader,
        execute(0, 1, 9, Mutation::OpenAccount { account_id: BETA }),
    );
    assert_eq!(
        gap.response(),
        Some(&LedgerResponse::Rejected(RequestRejection::SequenceGap {
            expected: sequence(3)
        }))
    );

    commit(&mut cluster, leader, open_session(0, 2));
    let stale_epoch_open = cluster.submit(leader, open_session(0, 1));
    assert_eq!(
        stale_epoch_open.response(),
        Some(&LedgerResponse::Rejected(RequestRejection::StaleSession {
            current: epoch(2)
        })),
        "an older session epoch cannot displace a newer one"
    );
    let stale_epoch_execute = cluster.submit(
        leader,
        execute(0, 1, 3, Mutation::CloseAccount { account_id: ALPHA }),
    );
    assert_eq!(
        stale_epoch_execute.response(),
        Some(&LedgerResponse::Rejected(RequestRejection::StaleSession {
            current: epoch(2)
        }))
    );

    cluster.settle();
    assert_eq!(
        cluster
            .state_machine(leader)
            .ledger()
            .account_balance(ALPHA),
        Some(7),
        "rejected identities changed no ledger state"
    );
    assert_linearizable(&cluster);
}

#[test]
fn a_leader_change_leaves_an_unknown_outcome_that_a_retry_resolves() {
    let mut cluster = LedgerCluster::new(config(2, 4));
    let first_leader = cluster.elect_leader();
    commit(&mut cluster, first_leader, open_session(0, 1));
    commit(
        &mut cluster,
        first_leader,
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
    );
    cluster.settle();

    let deposit = execute(
        0,
        1,
        2,
        Mutation::Deposit {
            account_id: ALPHA,
            amount: amount(25),
        },
    );
    cluster.partition(first_leader);
    let unknown = cluster.submit(first_leader, deposit.clone());
    assert_eq!(
        unknown,
        ProposalOutcome::Unknown,
        "an isolated leader cannot prove the outcome of its proposal"
    );

    let second_leader = cluster.elect_leader();
    assert_ne!(second_leader, first_leader, "leadership moved");
    cluster.heal();
    cluster.run_rounds(2);
    cluster.settle();
    assert!(
        cluster.runtime_unknown_outcomes() > 0,
        "the former leader reported that it lost its proposal's outcome"
    );

    // This read is what forces the unknown outcome to be read one specific way.
    // The isolated leader's entry never replicated, so no ordering that runs
    // the deposit can explain a zero balance here, and the checker has to
    // backtrack into the never-happened reading of the same operation.
    assert_eq!(
        cluster.read(second_leader, LedgerQuery::GetAccount { account_id: ALPHA }),
        ReadOutcome::Ready(LedgerQueryResult::Account {
            account_id: ALPHA,
            balance: Some(0),
        }),
        "the proposal the former leader lost never reached the replicated log"
    );

    let retry = cluster.submit(second_leader, deposit);
    assert_eq!(
        retry.response(),
        Some(&LedgerResponse::Mutation(MutationResult::Deposited {
            balance: 25
        })),
        "the retry under the same identity resolves the unknown window"
    );
    cluster.settle();
    assert_eq!(
        cluster.read(second_leader, LedgerQuery::GetAccount { account_id: ALPHA }),
        ReadOutcome::Ready(LedgerQueryResult::Account {
            account_id: ALPHA,
            balance: Some(25),
        }),
        "and the retry's effect is visible afterwards"
    );

    for node_id in cluster.node_ids() {
        assert_eq!(
            cluster
                .state_machine(node_id)
                .ledger()
                .account_balance(ALPHA),
            Some(25),
            "replica {node_id} applied the deposit exactly once"
        );
    }

    let replayed =
        replay_through_oracle(cluster.config(), &cluster.committed_commands(second_leader));
    assert_eq!(
        cluster.state_machine(second_leader).ledger().view(),
        replayed.view()
    );
    assert!(
        cluster
            .history()
            .iter()
            .any(|event| matches!(event, HistoryEvent::Unknown { .. })),
        "the history retains the unknown-outcome window"
    );
    assert_linearizable(&cluster);
}

#[test]
fn linearizable_reads_interleave_with_mutations() {
    let mut cluster = LedgerCluster::new(config(2, 4));
    let leader = cluster.elect_leader();
    commit(&mut cluster, leader, open_session(0, 1));
    commit(
        &mut cluster,
        leader,
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
    );

    assert_eq!(
        cluster.read(leader, LedgerQuery::GetAccount { account_id: ALPHA }),
        ReadOutcome::Ready(LedgerQueryResult::Account {
            account_id: ALPHA,
            balance: Some(0),
        })
    );

    for (request_sequence, deposit) in [(2_u64, 5_u64), (3, 9)] {
        commit(
            &mut cluster,
            leader,
            execute(
                0,
                1,
                request_sequence,
                Mutation::Deposit {
                    account_id: ALPHA,
                    amount: amount(deposit),
                },
            ),
        );
        let expected = if request_sequence == 2 { 5 } else { 14 };
        assert_eq!(
            cluster.read(leader, LedgerQuery::GetAccount { account_id: ALPHA }),
            ReadOutcome::Ready(LedgerQueryResult::Account {
                account_id: ALPHA,
                balance: Some(expected),
            }),
            "a read after a committed deposit observes it"
        );
    }

    let summary = cluster.read(leader, LedgerQuery::GetLedgerSummary);
    assert_eq!(
        summary,
        ReadOutcome::Ready(LedgerQueryResult::Summary(
            cluster.state_machine(leader).ledger().summary()
        ))
    );

    let follower = cluster
        .node_ids()
        .into_iter()
        .find(|node_id| *node_id != leader)
        .expect("a three-node cluster has followers");
    let refused = cluster.read(follower, LedgerQuery::GetLedgerSummary);
    let ReadOutcome::Rejected { leader_hint, .. } = refused else {
        panic!("a follower cannot issue the linearizable barrier itself, got {refused:?}");
    };
    // The rejection reaches the driver only as a read event inside the step
    // report the read returned, and it carries the redirect a client needs. A
    // driver that could not observe the report would have neither.
    assert_eq!(
        leader_hint,
        Some(leader),
        "a refused barrier redirects the client to the leader the follower believes in"
    );

    // Every answered query is part of the ordering the checker has to find; the
    // refused one delivered no value and constrains nothing.
    assert_eq!(assert_linearizable(&cluster), 8);
    assert!(
        cluster
            .history()
            .iter()
            .any(|event| matches!(event, HistoryEvent::QueryAbandoned { .. })),
        "the history retains the query that answered nothing"
    );
}

#[test]
fn a_refused_proposal_is_recorded_as_provably_uncommitted() {
    let mut cluster = LedgerCluster::new(config(2, 4));
    let leader = cluster.elect_leader();
    commit(&mut cluster, leader, open_session(0, 1));
    commit(
        &mut cluster,
        leader,
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
    );
    cluster.settle();

    let follower = cluster
        .node_ids()
        .into_iter()
        .find(|node_id| *node_id != leader)
        .expect("a three-node cluster has followers");
    let deposit = execute(
        0,
        1,
        2,
        Mutation::Deposit {
            account_id: ALPHA,
            amount: amount(25),
        },
    );
    let refused = cluster.submit(follower, deposit.clone());
    let ProposalOutcome::Rejected {
        reason,
        leader_hint,
    } = refused
    else {
        panic!("a follower refuses a proposal before replicating it, got {refused:?}");
    };
    assert!(matches!(reason, ProposalRejection::NotLeader { .. }));
    assert_eq!(leader_hint, Some(leader));
    assert!(
        cluster
            .history()
            .iter()
            .any(|event| matches!(event, HistoryEvent::NotCommitted { .. })),
        "the history records the stronger terminal event, not merely an unknown outcome"
    );

    // The stronger event is only honest if the command really is absent
    // everywhere. Two independent client-visible facts say so: the balance is
    // untouched, and the request identity the refusal carried is still unused,
    // so resubmitting it executes rather than replaying a cached result.
    assert_eq!(
        cluster.read(leader, LedgerQuery::GetAccount { account_id: ALPHA }),
        ReadOutcome::Ready(LedgerQueryResult::Account {
            account_id: ALPHA,
            balance: Some(0),
        })
    );
    let accepted = cluster.submit(leader, deposit);
    assert_eq!(accepted.disposition(), Some(ApplyDisposition::Applied));
    assert_eq!(
        accepted.response(),
        Some(&LedgerResponse::Mutation(MutationResult::Deposited {
            balance: 25
        }))
    );

    assert_eq!(
        cluster.crashed(),
        Vec::new(),
        "an in-memory replica has no durable backend that could fail"
    );
    let report = check_linearizable(cluster.config(), cluster.history())
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        report.discharged_operations(),
        1,
        "the refused command is settled without searching for a place to put it"
    );
}

#[test]
fn overlapping_reads_and_writes_order_correctly_across_a_leader_change() {
    let mut cluster = LedgerCluster::new(config(2, 4));
    let first_leader = cluster.elect_leader();
    commit(&mut cluster, first_leader, open_session(0, 1));
    commit(
        &mut cluster,
        first_leader,
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
    );
    commit(
        &mut cluster,
        first_leader,
        execute(
            0,
            1,
            2,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(10),
            },
        ),
    );
    cluster.settle();

    cluster.partition(first_leader);
    let second_leader = cluster.elect_leader();
    assert_ne!(second_leader, first_leader, "leadership moved");
    cluster.heal();
    cluster.settle();

    // Three operations the client starts before any of them answers. Their
    // real-time intervals all overlap, so the history permits several orderings
    // and the responses have to pick out a consistent one.
    let balance_across =
        cluster.begin_read(second_leader, LedgerQuery::GetAccount { account_id: ALPHA });
    let deposit = cluster.begin_submit(
        second_leader,
        execute(
            0,
            1,
            3,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(7),
            },
        ),
    );
    let summary_across = cluster.begin_read(second_leader, LedgerQuery::GetLedgerSummary);

    let observed_balance = cluster.resolve_read(balance_across);
    let committed = cluster.resolve_proposal(deposit);
    let observed_summary = cluster.resolve_read(summary_across);

    assert_eq!(
        committed.response(),
        Some(&LedgerResponse::Mutation(MutationResult::Deposited {
            balance: 17
        })),
        "the new leader replicated the write that overlapped both reads"
    );
    // Either balance is legal: the read overlaps the write, so an ordering may
    // place it on either side. What is not legal is a balance the write can
    // never produce, or a summary that disagrees with the balance the same
    // ordering already committed to.
    let ReadOutcome::Ready(LedgerQueryResult::Account { balance, .. }) = observed_balance else {
        panic!("the new leader must answer a barrier without a write behind it, got {observed_balance:?}");
    };
    assert!(
        balance == Some(10) || balance == Some(17),
        "a read overlapping the deposit saw {balance:?}"
    );
    assert!(matches!(
        observed_summary,
        ReadOutcome::Ready(LedgerQueryResult::Summary(_))
    ));

    assert_has_overlapping_operations(cluster.history());
    assert_eq!(assert_linearizable(&cluster), 6);
}

#[test]
fn a_restarted_replica_recovers_its_ledger_and_keeps_replicating() {
    let mut cluster = LedgerCluster::new(config(2, 4));
    let leader = cluster.elect_leader();
    commit(&mut cluster, leader, open_session(0, 1));
    commit(
        &mut cluster,
        leader,
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
    );
    commit(
        &mut cluster,
        leader,
        execute(
            0,
            1,
            2,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(40),
            },
        ),
    );
    cluster.settle();

    let follower = cluster
        .node_ids()
        .into_iter()
        .find(|node_id| *node_id != leader)
        .expect("a three-node cluster has followers");
    let before = cluster.state_machine(follower).ledger().view();
    let applied_before = cluster.applied_index(follower);
    let committed_before = cluster.committed_application_index(follower);
    assert!(
        committed_before > LogIndex::ZERO,
        "the replica has committed application entries to recover"
    );

    cluster.restart(follower);
    assert_eq!(
        cluster.state_machine(follower).ledger().view(),
        before,
        "the restarted replica recovered its ledger"
    );
    assert_eq!(cluster.applied_index(follower), applied_before);
    // The new incarnation recovered from the stores the retired runtime handed
    // back, so it knows exactly the same committed application entries. A
    // restart that opened a different medium would report a lower floor here
    // and the readiness comparison would silently pass on an empty replica.
    assert_eq!(
        cluster.committed_application_index(follower),
        committed_before,
        "the reopened runtime recovered from the retired one's durable stores"
    );

    commit(
        &mut cluster,
        leader,
        execute(
            0,
            1,
            3,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(2),
            },
        ),
    );
    cluster.settle();
    assert_eq!(
        cluster
            .state_machine(follower)
            .ledger()
            .account_balance(ALPHA),
        Some(42),
        "the restarted replica kept replicating"
    );
    assert_linearizable(&cluster);
}

#[test]
fn a_snapshot_round_trip_preserves_balances_sessions_and_deduplication() {
    let mut cluster = LedgerCluster::new(config(2, 4));
    let leader = cluster.elect_leader();
    commit(&mut cluster, leader, open_session(0, 1));
    commit(
        &mut cluster,
        leader,
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
    );
    let acknowledged = execute(
        0,
        1,
        2,
        Mutation::Deposit {
            account_id: ALPHA,
            amount: amount(18),
        },
    );
    let acknowledged_response = commit(&mut cluster, leader, acknowledged.clone());
    cluster.settle();
    assert_linearizable(&cluster);

    let applied_index = cluster.applied_index(leader);
    let snapshot = cluster
        .state_machine_mut(leader)
        .build_snapshot(applied_index)
        .expect("the leader snapshots its own applied index");
    assert_eq!(snapshot.applied_index, applied_index);

    let mut restored = LedgerStateMachine::new(cluster.config());
    restored
        .install_snapshot(snapshot)
        .expect("a self-built snapshot installs");
    assert_eq!(
        restored.applied_index(),
        Ok(applied_index),
        "the snapshot carries the applied floor with the data"
    );
    assert_eq!(
        restored.ledger().view(),
        cluster.state_machine(leader).ledger().view(),
        "balances, sessions, cached mutations, and cached results all survive"
    );

    // Compaction must never make an acknowledged command executable again: the
    // restored deduplication state replays it, and the applied floor refuses
    // to run it as a new entry at all.
    let replayed = restored
        .apply_batch(ApplyBatch {
            entries: vec![ApplyEntry {
                index: LogIndex(applied_index.0 + 1),
                term: rafter::Term(1),
                command: acknowledged.clone(),
                local_proposal_id: None,
            }],
        })
        .expect("a fresh index applies");
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].result.disposition, ApplyDisposition::Replayed);
    assert_eq!(
        LedgerResponse::Mutation(MutationResult::Deposited { balance: 18 }),
        replayed[0].result.response
    );
    assert_eq!(replayed[0].result.response, acknowledged_response);
    assert_eq!(restored.ledger().account_balance(ALPHA), Some(18));

    assert_eq!(
        restored.apply_batch(ApplyBatch {
            entries: vec![ApplyEntry {
                index: applied_index,
                term: rafter::Term(1),
                command: acknowledged,
                local_proposal_id: None,
            }],
        }),
        Err(LedgerAdapterError::AppliedIndexRegression {
            entry_index: applied_index,
            applied_index: LogIndex(applied_index.0 + 1),
        })
    );
}

#[test]
fn business_rejections_replicate_as_results_rather_than_adapter_errors() {
    let mut cluster = LedgerCluster::new(config(2, 4));
    let leader = cluster.elect_leader();
    commit(&mut cluster, leader, open_session(0, 1));

    let rejected = commit(
        &mut cluster,
        leader,
        execute(
            0,
            1,
            1,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(3),
            },
        ),
    );
    assert_eq!(
        rejected,
        LedgerResponse::Mutation(MutationResult::Rejected(BusinessRejection::AccountNotFound))
    );

    let cached = cluster.submit(
        leader,
        execute(
            0,
            1,
            1,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(3),
            },
        ),
    );
    assert_eq!(cached.disposition(), Some(ApplyDisposition::Replayed));
    assert_eq!(cached.response(), Some(&rejected));
    assert_linearizable(&cluster);
}

/// Submits a command and asserts that it committed, returning its response.
fn commit(
    cluster: &mut LedgerCluster,
    node_id: rafter::NodeId,
    command: Command,
) -> LedgerResponse {
    let outcome = cluster.submit(node_id, command);
    match outcome {
        ProposalOutcome::Committed { outcome, .. } => outcome.response,
        other => panic!("expected a committed outcome, got {other:?}"),
    }
}

/// Replays a real committed command sequence through the independent oracle.
///
/// This is the application-invariant half of the ledger's evidence: it says the
/// replicated state machine holds the state the specification says it should.
/// It says nothing about ordering, which is why every caller also checks the
/// recorded history for linearizability.
fn replay_through_oracle(config: LedgerConfig, commands: &[Command]) -> ReferenceLedger {
    let mut state = ReferenceLedger::new(config);
    for command in commands {
        state.apply(command.clone());
    }
    state
}

/// Checks the recorded history for linearizability, printing it on failure.
///
/// This subsumes the per-response oracle replay this suite used to do: matching
/// each response against some position in the committed log allowed answers
/// that no single real-time ordering could produce together, and a query's
/// answer was never checked at all.
fn assert_linearizable(cluster: &LedgerCluster) -> usize {
    // Nothing in this suite may lose a replica. The driver records a refused
    // step rather than panicking on it, so without this the whole cluster could
    // be dead and the history would still linearize — over no operations.
    assert_eq!(
        cluster.crashed(),
        Vec::new(),
        "an in-memory replica has no durable backend that could fail"
    );
    match check_linearizable(cluster.config(), cluster.history()) {
        Ok(report) => {
            assert!(
                report.checked_operations() > 0,
                "the checker was handed a history with nothing to check"
            );
            report.checked_operations()
        }
        Err(error) => panic!("{error}"),
    }
}

/// Asserts that the recorded history really contains concurrent operations.
///
/// A driver change that quietly serialized every operation would leave the
/// checker with a single forced ordering, and the linearizability assertions in
/// the concurrent scenarios would pass for the wrong reason.
fn assert_has_overlapping_operations(history: &[HistoryEvent]) {
    let mut in_flight = BTreeSet::new();
    let mut overlapped = false;
    for event in history {
        let operation_id = event.operation_id();
        match event {
            HistoryEvent::Invoked { .. } | HistoryEvent::QueryInvoked { .. } => {
                overlapped |= !in_flight.is_empty();
                in_flight.insert(operation_id);
            }
            _ => {
                in_flight.remove(&operation_id);
            }
        }
    }
    assert!(
        overlapped,
        "the recorded history has no overlapping operations to order"
    );
}
