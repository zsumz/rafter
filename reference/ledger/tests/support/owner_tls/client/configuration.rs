//! Exact configuration observations, including recovery above the applied floor.
use rafter::{LogIndex, MembershipConfig, NodeId, Term};

#[derive(Clone, Debug)]
pub(super) struct ConfigurationCommit {
    pub(super) node: NodeId,
    pub(super) index: LogIndex,
    pub(super) term: Term,
    pub(super) configuration: MembershipConfig,
}
