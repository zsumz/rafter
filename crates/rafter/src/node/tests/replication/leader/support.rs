//! Shared output normalization and append-batch inspection for leader scenarios.

pub(super) use super::*;

pub(super) fn erase_local_annotations(outputs: &[Output]) -> Vec<Output> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::LocalProposalAppended { .. } | Output::LocalProposalDropped { .. } => None,
            Output::Apply {
                index,
                term,
                payload,
                ..
            } => Some(Output::Apply {
                index: *index,
                term: *term,
                payload: payload.clone(),
                local_proposal_id: None,
            }),
            output => Some(output.clone()),
        })
        .collect()
}

pub(super) fn node_with_max_append_entries_bytes(id: u64, peers: &[u64], max_bytes: usize) -> Node {
    Node::new(
        NodeConfig::new(NodeId(id), peers.iter().copied().map(NodeId).collect(), 3)
            .expect("test Raft node config is valid")
            .with_max_append_entries_bytes(max_bytes),
    )
}

pub(super) fn append_entries_to(outputs: &[Output], to: NodeId) -> &AppendEntries {
    append_entries_batches_to(outputs, to)
        .first()
        .copied()
        .expect("expected append entries for peer")
}

pub(super) fn append_entries_batches_to(outputs: &[Output], to: NodeId) -> Vec<&AppendEntries> {
    outputs
        .iter()
        .filter_map(|output| {
            let Output::Send {
                to: actual_to,
                message,
            } = output
            else {
                return None;
            };
            if *actual_to != to {
                return None;
            }
            let Message::AppendEntries(request) = message else {
                return None;
            };
            Some(request)
        })
        .collect()
}

pub(super) fn replication_bytes(request: &AppendEntries) -> usize {
    request
        .entries
        .iter()
        .map(LogEntry::replication_bytes)
        .sum()
}
