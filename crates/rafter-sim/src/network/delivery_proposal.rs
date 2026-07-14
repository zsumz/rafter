use rafter::{NodeId, Output};

use crate::records::LocalProposalEvent;

pub(super) fn local_proposal_event(from: NodeId, output: &Output) -> Option<LocalProposalEvent> {
    match output {
        Output::LocalProposalAppended {
            proposal_id,
            index,
            term,
        } => Some(LocalProposalEvent::Appended {
            node_id: from,
            proposal_id: *proposal_id,
            index: *index,
            term: *term,
        }),
        Output::Apply {
            index,
            term,
            payload,
            local_proposal_id: Some(proposal_id),
        } => Some(LocalProposalEvent::Applied {
            node_id: from,
            proposal_id: *proposal_id,
            index: *index,
            term: *term,
            payload: payload.clone(),
        }),
        Output::LocalProposalDropped {
            proposal_id,
            index,
            term,
            reason,
        } => Some(LocalProposalEvent::Dropped {
            node_id: from,
            proposal_id: *proposal_id,
            index: *index,
            term: *term,
            reason: *reason,
        }),
        Output::RejectProposal {
            proposal_id: Some(proposal_id),
            reason,
        } => Some(LocalProposalEvent::Rejected {
            node_id: from,
            proposal_id: *proposal_id,
            reason: reason.clone(),
        }),
        Output::Apply {
            local_proposal_id: None,
            ..
        }
        | Output::ApplySnapshot { .. }
        | Output::SendSnapshotChunk { .. }
        | Output::StageSnapshotChunk { .. }
        | Output::RejectProposal {
            proposal_id: None, ..
        }
        | Output::LeadershipTransferRejected { .. }
        | Output::ReadIndexGranted { .. }
        | Output::ReadIndexRejected { .. }
        | Output::ReadIndexCanceled { .. }
        | Output::Send { .. } => None,
    }
}
