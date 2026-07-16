//! Top-level version 1 message dispatch and fixed-field payload grammar.

use rafter::{
    Message, NodeId, PreVote, PreVoteResponse, RequestVote, RequestVoteResponse, Term, TimeoutNow,
};

use crate::{
    v1::{append, snapshot, tags::MessageTag},
    wire::{Reader, Sink, Writer},
    DecodePeerMessageError, EncodePeerMessageError,
};

pub(crate) fn encode_payload<S: Sink>(
    writer: &mut Writer<S>,
    message: &Message,
) -> Result<(), EncodePeerMessageError> {
    let tag = match message {
        Message::RequestVote(_) => MessageTag::RequestVote,
        Message::RequestVoteResponse(_) => MessageTag::RequestVoteResponse,
        Message::AppendEntries(_) => MessageTag::AppendEntries,
        Message::AppendEntriesResponse(_) => MessageTag::AppendEntriesResponse,
        Message::InstallSnapshot(_) => {
            return Err(EncodePeerMessageError::UnsupportedMessage {
                message: "InstallSnapshot",
                reason: "use InstallSnapshotChunk for peer transport",
            });
        }
        Message::InstallSnapshotResponse(_) => MessageTag::InstallSnapshotResponse,
        Message::InstallSnapshotChunk(_) => MessageTag::InstallSnapshotChunk,
        Message::PreVote(_) => MessageTag::PreVote,
        Message::PreVoteResponse(_) => MessageTag::PreVoteResponse,
        Message::TimeoutNow(_) => MessageTag::TimeoutNow,
    };
    writer.u8(tag.into());

    match message {
        Message::RequestVote(request) => {
            writer.u64(request.term.0);
            writer.u64(request.candidate_id.0);
            writer.u64(request.last_log_index.0);
            writer.u64(request.last_log_term.0);
        }
        Message::RequestVoteResponse(response) => {
            writer.u64(response.term.0);
            writer.u64(response.voter_id.0);
            writer.bool(response.vote_granted);
        }
        Message::AppendEntries(request) => append::encode_request(writer, request)?,
        Message::AppendEntriesResponse(response) => append::encode_response(writer, response),
        Message::InstallSnapshot(_) => unreachable!("unsupported message returned before writing"),
        Message::InstallSnapshotResponse(response) => snapshot::encode_response(writer, response),
        Message::InstallSnapshotChunk(request) => snapshot::encode_chunk(writer, request)?,
        Message::PreVote(request) => {
            writer.u64(request.term.0);
            writer.u64(request.candidate_id.0);
            writer.u64(request.last_log_index.0);
            writer.u64(request.last_log_term.0);
        }
        Message::PreVoteResponse(response) => {
            writer.u64(response.term.0);
            writer.u64(response.voter_id.0);
            writer.bool(response.vote_granted);
        }
        Message::TimeoutNow(request) => {
            writer.u64(request.term.0);
            writer.u64(request.leader_id.0);
        }
    }
    Ok(())
}

pub(crate) fn decode_payload(reader: &mut Reader<'_>) -> Result<Message, DecodePeerMessageError> {
    match MessageTag::try_from(reader.u8()?)? {
        MessageTag::RequestVote => Ok(Message::RequestVote(RequestVote {
            term: Term(reader.u64()?),
            candidate_id: NodeId(reader.u64()?),
            last_log_index: rafter::LogIndex(reader.u64()?),
            last_log_term: Term(reader.u64()?),
        })),
        MessageTag::RequestVoteResponse => Ok(Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(reader.u64()?),
            voter_id: NodeId(reader.u64()?),
            vote_granted: reader.bool()?,
        })),
        MessageTag::AppendEntries => append::decode_request(reader).map(Message::AppendEntries),
        MessageTag::AppendEntriesResponse => {
            append::decode_response(reader).map(Message::AppendEntriesResponse)
        }
        MessageTag::InstallSnapshotResponse => {
            snapshot::decode_response(reader).map(Message::InstallSnapshotResponse)
        }
        MessageTag::InstallSnapshotChunk => {
            snapshot::decode_chunk(reader).map(Message::InstallSnapshotChunk)
        }
        MessageTag::PreVote => Ok(Message::PreVote(PreVote {
            term: Term(reader.u64()?),
            candidate_id: NodeId(reader.u64()?),
            last_log_index: rafter::LogIndex(reader.u64()?),
            last_log_term: Term(reader.u64()?),
        })),
        MessageTag::PreVoteResponse => Ok(Message::PreVoteResponse(PreVoteResponse {
            term: Term(reader.u64()?),
            voter_id: NodeId(reader.u64()?),
            vote_granted: reader.bool()?,
        })),
        MessageTag::TimeoutNow => Ok(Message::TimeoutNow(TimeoutNow {
            term: Term(reader.u64()?),
            leader_id: NodeId(reader.u64()?),
        })),
    }
}
