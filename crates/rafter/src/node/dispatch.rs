//! The public input boundary and message dispatcher for [`Node`].
//!
//! All external protocol activity enters through `step` or `step_batch`. The
//! returned `Output` order is load-bearing: embedders must persist state before
//! releasing dependent sends, applies, or read grants.

use crate::{Message, NodeId, ReadId};

use super::{ClientProposalInput, Input, Node, Output};

impl Node {
    /// Applies one input event and returns ordered side effects.
    #[must_use]
    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::Tick => self.tick(),
            Input::Message { from, message } => self.receive(from, message),
            Input::ClientProposal { payload } => self.step_client_proposal(ClientProposalInput {
                proposal_id: None,
                payload,
            }),
            Input::TrackedClientProposal {
                proposal_id,
                payload,
            } => self.step_client_proposal(ClientProposalInput {
                proposal_id: Some(proposal_id),
                payload,
            }),
            Input::AddLearner { learner_id } => self.add_learner(learner_id),
            Input::PromoteLearner {
                learner_id,
                promotion_barrier,
            } => self.promote_learner(learner_id, promotion_barrier),
            Input::RemoveVoter { voter_id } => self.remove_voter(voter_id),
            Input::EnterJoint {
                target,
                promotion_barriers,
            } => self.enter_joint(target, &promotion_barriers),
            Input::LeaveJoint => self.leave_joint(),
            Input::ChangeMembership {
                target,
                promotion_barriers,
            } => self.change_membership(target, &promotion_barriers),
            Input::DangerousRawConfigurationProposal {
                configuration,
                promotion_barriers,
            } => self.dangerous_raw_configuration_proposal(configuration, &promotion_barriers),
            Input::TransferLeadership { target } => self.transfer_leadership(target),
            Input::ReadIndex { read_id } => self.read_index(read_id),
        }
    }

    /// Applies several input events while coalescing adjacent client
    /// proposals into one deterministic proposal batch and adjacent read
    /// barriers into one deterministic confirmation round.
    ///
    /// Messages, membership changes, ticks, and leadership transfer requests
    /// retain their original one-step semantics. They also form batch
    /// boundaries because they can change the term, role, quorum, heartbeat
    /// sequencing, or output ordering obligations that deterministic batching
    /// must not cross.
    #[must_use]
    pub fn step_batch(&mut self, inputs: Vec<Input>) -> Vec<Output> {
        let mut inputs = inputs.into_iter();
        let Some(first) = inputs.next() else {
            return Vec::new();
        };
        let Some(second) = inputs.next() else {
            return self.step(first);
        };

        let mut outputs = Vec::new();
        let mut pending = PendingBatch::default();

        pending.accept(self, first, &mut outputs);
        pending.accept(self, second, &mut outputs);
        for input in inputs {
            pending.accept(self, input, &mut outputs);
        }

        pending.flush(self, &mut outputs);
        outputs
    }

    fn receive(&mut self, from: NodeId, message: Message) -> Vec<Output> {
        // Membership does not gate message processing wholesale: servers
        // outside the receiver's configuration may still carry relevant
        // terms, log probes, or snapshot state during membership changes.
        // Vote handlers apply candidate-voter fencing before granting.
        if !message_sender_matches(from, &message) {
            return Vec::new();
        }

        match message {
            Message::AppendEntries(request) => self.handle_append_entries(from, &request),
            Message::AppendEntriesResponse(response) => {
                self.handle_append_entries_response(from, response)
            }
            Message::InstallSnapshot(request) => self.handle_install_snapshot(from, request),
            Message::InstallSnapshotChunk(request) => {
                self.handle_install_snapshot_chunk(from, request)
            }
            Message::InstallSnapshotResponse(response) => {
                self.handle_install_snapshot_response(from, response)
            }
            Message::PreVote(request) => self.handle_pre_vote(from, request),
            Message::PreVoteResponse(response) => self.handle_pre_vote_response(from, response),
            Message::TimeoutNow(request) => self.handle_timeout_now(request.term),
            Message::RequestVote(request) => self.handle_request_vote(from, request),
            Message::RequestVoteResponse(response) => {
                self.handle_request_vote_response(from, response)
            }
        }
    }
}

/// Adjacent inputs awaiting one ordered batched transition.
///
/// The enum makes the batching invariant structural: proposals and reads can
/// never be pending at the same time. Changing input kind flushes the previous
/// batch before recording the next one.
#[derive(Default)]
enum PendingBatch {
    #[default]
    Empty,
    Proposals(Vec<ClientProposalInput>),
    Reads(Vec<ReadId>),
}

impl PendingBatch {
    fn accept(&mut self, node: &mut Node, input: Input, outputs: &mut Vec<Output>) {
        match input {
            Input::ClientProposal { payload } => self.push_proposal(
                node,
                outputs,
                ClientProposalInput {
                    proposal_id: None,
                    payload,
                },
            ),
            Input::TrackedClientProposal {
                proposal_id,
                payload,
            } => self.push_proposal(
                node,
                outputs,
                ClientProposalInput {
                    proposal_id: Some(proposal_id),
                    payload,
                },
            ),
            Input::ReadIndex { read_id } => self.push_read(node, outputs, read_id),
            input => {
                self.flush(node, outputs);
                outputs.extend(node.step(input));
            }
        }
    }

    fn push_proposal(
        &mut self,
        node: &mut Node,
        outputs: &mut Vec<Output>,
        proposal: ClientProposalInput,
    ) {
        *self = match std::mem::take(self) {
            Self::Empty => Self::Proposals(vec![proposal]),
            Self::Proposals(mut proposals) => {
                proposals.push(proposal);
                Self::Proposals(proposals)
            }
            Self::Reads(reads) => {
                outputs.extend(node.read_index_batch(reads));
                Self::Proposals(vec![proposal])
            }
        };
    }

    fn push_read(&mut self, node: &mut Node, outputs: &mut Vec<Output>, read_id: ReadId) {
        *self = match std::mem::take(self) {
            Self::Empty => Self::Reads(vec![read_id]),
            Self::Reads(mut reads) => {
                reads.push(read_id);
                Self::Reads(reads)
            }
            Self::Proposals(proposals) => {
                outputs.extend(node.step_proposal_batch(proposals));
                Self::Reads(vec![read_id])
            }
        };
    }

    fn flush(&mut self, node: &mut Node, outputs: &mut Vec<Output>) {
        match std::mem::take(self) {
            Self::Empty => {}
            Self::Proposals(proposals) => outputs.extend(node.step_proposal_batch(proposals)),
            Self::Reads(reads) => outputs.extend(node.read_index_batch(reads)),
        }
    }
}

fn message_sender_matches(from: NodeId, message: &Message) -> bool {
    match message {
        Message::AppendEntries(request) => request.leader_id == from,
        Message::AppendEntriesResponse(response) => response.follower_id == from,
        Message::InstallSnapshot(request) => request.leader_id == from,
        Message::InstallSnapshotChunk(request) => request.leader_id == from,
        Message::InstallSnapshotResponse(response) => response.follower_id == from,
        Message::PreVote(request) => request.candidate_id == from,
        Message::PreVoteResponse(response) => response.voter_id == from,
        Message::TimeoutNow(request) => request.leader_id == from,
        Message::RequestVote(request) => request.candidate_id == from,
        Message::RequestVoteResponse(response) => response.voter_id == from,
    }
}
