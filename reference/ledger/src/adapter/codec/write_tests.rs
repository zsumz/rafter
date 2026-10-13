//! Literal v1 tags and fixed-width fields, independent of decoder constants.

use super::{command, Fields};
use crate::{
    AccountId, Amount, BusinessRejection, ClientId, Command, Mutation, MutationResult,
    RequestIdentity, Sequence, SessionEpoch,
};

fn id(value: u64) -> AccountId {
    AccountId::new(value)
}
fn amount() -> Amount {
    Amount::new(3).unwrap()
}

#[test]
fn every_command_mutation_preserves_the_version_one_wire_bytes() {
    let mut session = Vec::new();
    command(
        &mut session,
        &Command::OpenSession {
            client_id: ClientId::new(5),
            session_epoch: SessionEpoch::new(6).unwrap(),
        },
    )
    .unwrap();
    assert_eq!(session, [1, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6]);
    for (mutation, tag, fields) in [
        (Mutation::OpenAccount { account_id: id(1) }, 1, vec![1_u64]),
        (
            Mutation::Deposit {
                account_id: id(1),
                amount: amount(),
            },
            2,
            vec![1, 3],
        ),
        (
            Mutation::Transfer {
                from: id(1),
                to: id(2),
                amount: amount(),
            },
            3,
            vec![1, 2, 3],
        ),
        (Mutation::CloseAccount { account_id: id(1) }, 4, vec![1]),
    ] {
        let mut bytes = Vec::new();
        command(
            &mut bytes,
            &Command::Execute {
                request: RequestIdentity {
                    client_id: ClientId::new(5),
                    session_epoch: SessionEpoch::new(6).unwrap(),
                    sequence: Sequence::new(7).unwrap(),
                },
                mutation,
            },
        )
        .unwrap();
        let mut expected = vec![
            1, 2, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7, tag,
        ];
        for field in fields {
            expected.extend_from_slice(&field.to_be_bytes());
        }
        assert_eq!(bytes, expected);
    }
}

#[test]
fn cached_results_and_all_business_rejections_keep_their_literal_v1_tags() {
    for (result, tag, fields) in [
        (MutationResult::AccountOpened, 1, vec![]),
        (MutationResult::Deposited { balance: 13 }, 2, vec![13_u64]),
        (
            MutationResult::Transferred {
                from_balance: 11,
                to_balance: 17,
            },
            3,
            vec![11, 17],
        ),
        (MutationResult::AccountClosed, 4, vec![]),
    ] {
        let mut bytes = Vec::new();
        Fields(&mut bytes).result(&result).unwrap();
        let mut expected = vec![tag];
        for field in fields {
            expected.extend_from_slice(&field.to_be_bytes());
        }
        assert_eq!(bytes, expected);
    }
    for (rejection, tag) in [
        (BusinessRejection::AccountAlreadyExists, 1),
        (BusinessRejection::AccountCapacityExceeded, 2),
        (BusinessRejection::AccountNotFound, 3),
        (BusinessRejection::SameAccount, 4),
        (BusinessRejection::InsufficientFunds, 5),
        (BusinessRejection::BalanceOverflow, 6),
        (BusinessRejection::SupplyOverflow, 7),
        (BusinessRejection::AccountNotEmpty, 8),
    ] {
        let mut bytes = Vec::new();
        Fields(&mut bytes)
            .result(&MutationResult::Rejected(rejection))
            .unwrap();
        assert_eq!(bytes, [5, tag]);
    }
}
