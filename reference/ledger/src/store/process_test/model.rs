//! A nonempty state whose balances, supply, deduplication and floor move together.

use rafter::LogIndex;

use crate::{
    store::LedgerStore, AccountId, Amount, ApplyDisposition, ClientId, Command, Ledger,
    LedgerConfig, Mutation, RequestIdentity, Sequence, SessionEpoch,
};

pub(super) fn config() -> LedgerConfig {
    LedgerConfig::new(2, 4).expect("valid bounds")
}

fn execute(sequence: u64, mutation: Mutation) -> Command {
    Command::Execute {
        request: RequestIdentity {
            client_id: ClientId::new(0),
            session_epoch: SessionEpoch::new(1).expect("valid epoch"),
            sequence: Sequence::new(sequence).expect("positive sequence"),
        },
        mutation,
    }
}

pub(super) fn commands() -> [Command; 5] {
    let alpha = AccountId::new(11);
    let beta = AccountId::new(12);
    [
        Command::OpenSession {
            client_id: ClientId::new(0),
            session_epoch: SessionEpoch::new(1).expect("valid epoch"),
        },
        execute(1, Mutation::OpenAccount { account_id: alpha }),
        execute(2, Mutation::OpenAccount { account_id: beta }),
        execute(
            3,
            Mutation::Deposit {
                account_id: alpha,
                amount: Amount::new(40).expect("positive deposit"),
            },
        ),
        execute(
            4,
            Mutation::Transfer {
                from: alpha,
                to: beta,
                amount: Amount::new(7).expect("positive transfer"),
            },
        ),
    ]
}

pub(super) fn image(count: usize) -> Ledger {
    let mut ledger = Ledger::new(config());
    for command in commands().into_iter().take(count) {
        let outcome = ledger.apply(command);
        assert!(matches!(
            outcome.disposition,
            ApplyDisposition::Applied | ApplyDisposition::SessionOpened
        ));
    }
    ledger
}

pub(super) fn seed(store: &mut LedgerStore) {
    for count in 1..=4 {
        store
            .commit(&image(count), LogIndex(count as u64))
            .expect("seed commits");
    }
}

pub(super) fn assert_state(store: &LedgerStore, count: usize) {
    assert_eq!(store.applied_index(), LogIndex(count as u64));
    assert_eq!(store.ledger().view(), image(count).view());
    assert!(!store.requires_reopen());
}

pub(super) fn continue_after_recovery(store: &mut LedgerStore, count: usize) {
    assert_state(store, count);
    let mut ledger = store.ledger().clone();
    if count != 0 {
        // The most recent request must replay its exact result without mutation.
        let last = commands()[count - 1].clone();
        let expected = image(count - 1).apply(last.clone());
        let before = ledger.view();
        let replayed = ledger.apply(last);
        assert_eq!(replayed.disposition, ApplyDisposition::Replayed);
        assert_eq!(replayed.response, expected.response);
        assert_eq!(ledger.view(), before);
    }
    let next = if count == 0 {
        commands()[0].clone()
    } else {
        execute(
            count as u64,
            Mutation::Deposit {
                account_id: AccountId::new(12),
                amount: Amount::new(1).expect("positive next deposit"),
            },
        )
    };
    let outcome = ledger.apply(next);
    assert!(matches!(
        outcome.disposition,
        ApplyDisposition::Applied | ApplyDisposition::SessionOpened
    ));
    store
        .commit(&ledger, LogIndex(count as u64 + 1))
        .expect("recovered store commits");
}
