//! Fixtures the byte-level checks are written against.

use rafter::LogIndex;

use crate::{
    store::frame::encode_frame, AccountId, Amount, ClientId, Command, Ledger, LedgerConfig,
    Mutation, RequestIdentity, Sequence, SessionEpoch,
};

/// One sealed frame over a ledger that actually holds something.
///
/// An empty ledger would exercise the framing and almost none of the image,
/// and the invariants below are about *every* byte of a frame.
pub(super) fn sealed_frame() -> Vec<u8> {
    let config = LedgerConfig::new(2, 4).expect("bounds are non-zero");
    let mut ledger = Ledger::new(config);
    let client_id = ClientId::new(0);
    let session_epoch = SessionEpoch::new(1).expect("epoch one is valid");
    ledger.apply(Command::OpenSession {
        client_id,
        session_epoch,
    });
    let mut execute = |sequence: u64, mutation: Mutation| {
        ledger.apply(Command::Execute {
            request: RequestIdentity {
                client_id,
                session_epoch,
                sequence: Sequence::new(sequence).expect("sequences start at one"),
            },
            mutation,
        });
    };
    let alpha = AccountId::new(11);
    let beta = AccountId::new(12);
    execute(1, Mutation::OpenAccount { account_id: alpha });
    execute(2, Mutation::OpenAccount { account_id: beta });
    execute(
        3,
        Mutation::Deposit {
            account_id: alpha,
            amount: Amount::new(40).expect("a deposit is non-zero"),
        },
    );
    encode_frame(&ledger, LogIndex(4)).expect("the frame encodes")
}
