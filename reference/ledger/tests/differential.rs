mod support;

use rafter_reference_ledger::{AccountId, Command, Ledger, Mutation, ReferenceLedger};
use support::{amount, config, execute, open_session};

#[test]
fn independent_models_agree_across_small_command_histories() {
    let commands = command_alphabet();
    let bounds = config(2, 2);
    let implementation = Ledger::new(bounds);
    let oracle = ReferenceLedger::new(bounds);
    explore(4, &implementation, &oracle, &commands, &mut Vec::new());
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
