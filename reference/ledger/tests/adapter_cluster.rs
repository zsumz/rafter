mod support;

#[path = "support/cluster.rs"]
mod cluster;
#[path = "support/storage.rs"]
mod storage;

use std::collections::BTreeMap;

use rafter::LogIndex;
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_ledger::{
    AccountId, ApplyDisposition, BusinessRejection, Command, HistoryEvent, LedgerAdapterError,
    LedgerConfig, LedgerQuery, LedgerQueryResult, LedgerResponse, LedgerStateMachine, Mutation,
    MutationResult, OperationId, ReferenceLedger, RequestRejection,
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
        replayed.state.view(),
        "the replicated ledger diverged from the independent oracle"
    );
    assert_eq!(leader_ledger.summary(), replayed.state.summary());
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
    assert_history_agrees_with_oracle(cluster.history(), &replayed.responses);
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

    let retry = cluster.submit(second_leader, deposit);
    assert_eq!(
        retry.response(),
        Some(&LedgerResponse::Mutation(MutationResult::Deposited {
            balance: 25
        })),
        "the retry under the same identity resolves the unknown window"
    );
    cluster.settle();

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
        replayed.state.view()
    );
    assert_history_agrees_with_oracle(cluster.history(), &replayed.responses);
    assert!(
        cluster
            .history()
            .iter()
            .any(|event| matches!(event, HistoryEvent::Unknown { .. })),
        "the history retains the unknown-outcome window"
    );
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
    assert!(
        matches!(
            cluster.read(follower, LedgerQuery::GetLedgerSummary),
            ReadOutcome::Rejected { .. }
        ),
        "a follower cannot issue the linearizable barrier itself"
    );
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

    cluster.restart(follower);
    assert_eq!(
        cluster.state_machine(follower).ledger().view(),
        before,
        "the restarted replica recovered its ledger"
    );
    assert_eq!(cluster.applied_index(follower), applied_before);

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

struct OracleReplay {
    state: ReferenceLedger,
    responses: Vec<(Command, LedgerResponse)>,
}

/// Replays a real committed command sequence through the independent oracle.
fn replay_through_oracle(config: LedgerConfig, commands: &[Command]) -> OracleReplay {
    let mut state = ReferenceLedger::new(config);
    let responses = commands
        .iter()
        .map(|command| (command.clone(), state.apply(command.clone()).response))
        .collect();
    OracleReplay { state, responses }
}

/// Checks every terminal client response against the oracle's replay.
///
/// Operations that ended in an unknown outcome constrain nothing, which is
/// exactly what the contract's `Unknown` event means.
fn assert_history_agrees_with_oracle(
    history: &[HistoryEvent],
    responses: &[(Command, LedgerResponse)],
) {
    let mut invoked = BTreeMap::<OperationId, &Command>::new();
    for event in history {
        if let HistoryEvent::Invoked {
            operation_id,
            command,
        } = event
        {
            invoked.insert(*operation_id, command);
        }
    }

    let mut checked = 0_usize;
    for event in history {
        let HistoryEvent::Completed {
            operation_id,
            response,
        } = event
        else {
            continue;
        };
        let command = invoked
            .get(operation_id)
            .expect("every completion follows its invocation");
        assert!(
            responses
                .iter()
                .any(|(replayed, replayed_response)| replayed == *command
                    && replayed_response == response),
            "no committed execution of {command:?} produced the observed response {response:?}"
        );
        checked += 1;
    }
    assert!(checked > 0, "the history checked no completed operations");
}
