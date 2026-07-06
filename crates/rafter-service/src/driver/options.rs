use rafter::{LogIndex, Term};
use rafter_app::{proposal::ClientRequestId, read::ReadProof};

/// Options for a managed write.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteOptions {
    pub client_request_id: Option<ClientRequestId>,
}

/// Receipt returned only after the proposed command has committed and applied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteReceipt<R = ()> {
    pub index: LogIndex,
    pub term: Term,
    pub result: R,
}

/// Receipt returned by a managed read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryReceipt<G, R = ()> {
    pub result: R,
    pub proof: Option<ReadProof<G>>,
}
