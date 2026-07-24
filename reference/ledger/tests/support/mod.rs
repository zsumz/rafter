use rafter_reference_ledger::{
    Amount, ClientId, Command, LedgerConfig, Mutation, RequestIdentity, Sequence, SessionEpoch,
};

pub fn config(max_clients: u32, max_accounts: usize) -> LedgerConfig {
    LedgerConfig::new(max_clients, max_accounts).expect("test bounds are nonzero")
}

pub fn client(value: u32) -> ClientId {
    ClientId::new(value)
}

pub fn epoch(value: u64) -> SessionEpoch {
    SessionEpoch::new(value).expect("test epoch is nonzero")
}

pub fn sequence(value: u64) -> Sequence {
    Sequence::new(value).expect("test sequence is nonzero")
}

pub fn amount(value: u64) -> Amount {
    Amount::new(value).expect("test amount is nonzero")
}

pub fn open_session(client_id: u32, session_epoch: u64) -> Command {
    Command::OpenSession {
        client_id: client(client_id),
        session_epoch: epoch(session_epoch),
    }
}

pub fn execute(
    client_id: u32,
    session_epoch: u64,
    request_sequence: u64,
    mutation: Mutation,
) -> Command {
    Command::Execute {
        request: RequestIdentity {
            client_id: client(client_id),
            session_epoch: epoch(session_epoch),
            sequence: sequence(request_sequence),
        },
        mutation,
    }
}
