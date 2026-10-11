//! Model configuration must agree with the journal before publication starts.

#[path = "support/scratch.rs"]
mod scratch;
#[allow(dead_code)]
mod support;

use rafter::LogIndex;
use rafter_reference_ledger::{
    store::{raw_journal, FaultPlan, LedgerStore, LedgerStoreError, WriteFault},
    AccountId, Ledger, Mutation,
};
use scratch::ScratchDir;
use support::{config, execute, open_session};

fn foreign(clients: u32, accounts: usize, populated: bool) -> Ledger {
    let mut ledger = Ledger::new(config(clients, accounts));
    if populated {
        let client = clients - 1;
        ledger.apply(open_session(client, 1));
        for id in 0..accounts as u64 {
            ledger.apply(execute(
                client,
                1,
                id + 1,
                Mutation::OpenAccount {
                    account_id: AccountId::new(id),
                },
            ));
        }
    }
    ledger
}

#[test]
fn a_foreign_model_cannot_change_the_journals_resource_contract() {
    for replace in [false, true] {
        for (clients, accounts, populated) in [(2, 1, true), (1, 2, true), (2, 2, false)] {
            let scratch = ScratchDir::new("journal-model-config");
            let mut store = LedgerStore::open(scratch.path(), config(1, 1)).unwrap();
            let before = raw_journal::read(scratch.path()).unwrap();
            let ledger = foreign(clients, accounts, populated);
            let result = if replace {
                store.replace(&ledger, LogIndex(5))
            } else {
                store.commit(&ledger, LogIndex(5))
            };
            if result.is_ok() {
                drop(store);
                panic!(
                    "foreign model was acknowledged; reopen: {:?}",
                    LedgerStore::open(scratch.path(), config(1, 1))
                );
            }
            assert!(matches!(result, Err(LedgerStoreError::ConfigMismatch {
                journal_max_clients: 1, journal_max_accounts: 1,
                requested_max_clients, requested_max_accounts,
            }) if requested_max_clients == clients && requested_max_accounts == accounts as u64));
            assert_eq!(store.applied_index(), LogIndex(0));
            assert!(!store.requires_reopen());
            assert_eq!(raw_journal::read(scratch.path()).unwrap(), before);
            assert!(!scratch.path().join("ledger.journal.tmp").exists());
            drop(store);
            assert_eq!(
                LedgerStore::open(scratch.path(), config(1, 1))
                    .unwrap()
                    .applied_index(),
                LogIndex(0)
            );
        }
    }
}

#[test]
fn configuration_refusal_does_not_consume_a_write_plan() {
    for replace in [false, true] {
        let scratch = ScratchDir::new("journal-config-before-plan");
        let mut store = LedgerStore::open_with_faults(
            scratch.path(),
            config(1, 1),
            FaultPlan::at(1, WriteFault::BeforeFirstByte),
        )
        .unwrap();
        let foreign = foreign(1, 2, false);
        let result = if replace {
            store.replace(&foreign, LogIndex(1))
        } else {
            store.commit(&foreign, LogIndex(1))
        };
        assert!(matches!(
            result,
            Err(LedgerStoreError::ConfigMismatch { .. })
        ));
        assert_eq!(store.fired_fault(), None);
        assert!(!store.requires_reopen());
        assert!(matches!(
            store.commit(&Ledger::new(config(1, 1)), LogIndex(1)),
            Err(LedgerStoreError::InjectedFault {
                plan: 1,
                fault: WriteFault::BeforeFirstByte
            })
        ));
    }
}
