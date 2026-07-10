use crate::{ClientProposalInput, LogEntry};

use super::super::state::LocalProposal;
use super::{Node, Output, ProposalRejection, Role};

impl Node {
    /// Appends one deterministic client proposal without constructing the
    /// batch plan used for multi-proposal bursts.
    pub(in crate::node) fn step_client_proposal(
        &mut self,
        proposal: ClientProposalInput,
    ) -> Vec<Output> {
        if let Some(rejection) = self.proposal_preflight_rejection(&proposal) {
            return vec![rejection];
        }

        let payload_len = proposal.payload.len();
        let max_payload_len =
            LogEntry::max_application_payload_len(self.config.max_append_entries_bytes());
        if LogEntry::application_replication_bytes(payload_len)
            > self.config.max_append_entries_bytes()
        {
            return vec![Output::RejectProposal {
                proposal_id: proposal.proposal_id,
                reason: ProposalRejection::PayloadTooLarge {
                    payload_len,
                    max_payload_len,
                },
            }];
        }

        let term = self.current_term();
        let index = self.last_log_index().next();
        self.append_log_entry(LogEntry::application(term, proposal.payload));

        let mut outputs = Vec::new();
        if let Some(id) = proposal.proposal_id {
            self.volatile
                .local_proposals
                .insert(index, LocalProposal { term, id });
            outputs.push(Output::LocalProposalAppended {
                proposal_id: id,
                index,
                term,
            });
        }

        self.record_local_progress();
        self.advance_commit_index_into(&mut outputs);
        if self.role() == Role::Leader {
            self.broadcast_append_entries_into(&mut outputs);
        }
        outputs
    }

    /// Appends one deterministic client-proposal batch as a contiguous log
    /// range and fills follower windows once after the whole range exists.
    ///
    /// Local append and rejection annotations are emitted in proposal input
    /// order before apply outputs caused by the batch commit advancement. That
    /// intentionally makes a single-voter proposal batch observable as one
    /// protocol moment rather than as concatenated single-proposal steps.
    #[must_use]
    pub fn step_proposal_batch(&mut self, mut proposals: Vec<ClientProposalInput>) -> Vec<Output> {
        if proposals.is_empty() {
            return Vec::new();
        }
        if proposals.len() == 1 {
            return match proposals.pop() {
                Some(proposal) => self.step_client_proposal(proposal),
                None => Vec::new(),
            };
        }
        if let Some(rejection) = self.proposal_batch_preflight_rejection(&proposals) {
            return rejection;
        }

        let term = self.current_term();
        let max_payload_len =
            LogEntry::max_application_payload_len(self.config.max_append_entries_bytes());
        let mut next_index = self.last_log_index().next();
        let mut accepted = false;
        let mut outputs = Vec::new();

        for proposal in proposals {
            let payload_len = proposal.payload.len();
            if LogEntry::application_replication_bytes(payload_len)
                > self.config.max_append_entries_bytes()
            {
                outputs.push(Output::RejectProposal {
                    proposal_id: proposal.proposal_id,
                    reason: ProposalRejection::PayloadTooLarge {
                        payload_len,
                        max_payload_len,
                    },
                });
                continue;
            }

            let index = next_index;
            next_index = next_index.next();
            self.append_log_entry(LogEntry::application(term, proposal.payload));
            if let Some(id) = proposal.proposal_id {
                self.volatile
                    .local_proposals
                    .insert(index, LocalProposal { term, id });
                outputs.push(Output::LocalProposalAppended {
                    proposal_id: id,
                    index,
                    term,
                });
            }
            accepted = true;
        }

        if !accepted {
            return outputs;
        }

        self.record_local_progress();
        self.advance_commit_index_into(&mut outputs);
        if self.role() == Role::Leader {
            self.broadcast_append_entries_into(&mut outputs);
        }
        outputs
    }

    fn proposal_batch_preflight_rejection(
        &self,
        proposals: &[ClientProposalInput],
    ) -> Option<Vec<Output>> {
        if self.role() != Role::Leader {
            let role = self.role();
            let term = self.current_term();
            return Some(
                proposals
                    .iter()
                    .map(|proposal| Output::RejectProposal {
                        proposal_id: proposal.proposal_id,
                        reason: ProposalRejection::NotLeader {
                            role,
                            term,
                            payload_len: proposal.payload.len(),
                        },
                    })
                    .collect(),
            );
        }
        if let Some(transfer) = self.leader.pending_transfer.as_ref() {
            let target = transfer.target;
            return Some(
                proposals
                    .iter()
                    .map(|proposal| Output::RejectProposal {
                        proposal_id: proposal.proposal_id,
                        reason: ProposalRejection::LeadershipTransferInProgress { target },
                    })
                    .collect(),
            );
        }
        None
    }

    fn proposal_preflight_rejection(&self, proposal: &ClientProposalInput) -> Option<Output> {
        if self.role() != Role::Leader {
            return Some(Output::RejectProposal {
                proposal_id: proposal.proposal_id,
                reason: ProposalRejection::NotLeader {
                    role: self.role(),
                    term: self.current_term(),
                    payload_len: proposal.payload.len(),
                },
            });
        }
        self.leader
            .pending_transfer
            .as_ref()
            .map(|transfer| Output::RejectProposal {
                proposal_id: proposal.proposal_id,
                reason: ProposalRejection::LeadershipTransferInProgress {
                    target: transfer.target,
                },
            })
    }
}
