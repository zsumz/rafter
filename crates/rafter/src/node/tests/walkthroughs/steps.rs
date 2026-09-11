//! Explicit inputs and replies correlated with append frames already emitted.

use super::support::*;

pub(super) struct Step {
    pub name: &'static str,
    pub why: &'static str,
    pub inputs: Vec<Stimulus>,
}

pub(super) enum Stimulus {
    Input(Box<Input>),
    Reply(AppendReply),
}

pub(super) enum ReplyOutcome {
    Accepted,
    Rejected { term: Term },
}

pub(super) struct AppendReply {
    pub follower: NodeId,
    pub through: LogIndex,
    /// None selects the latest matching append. Some selects a delayed echo.
    pub sequence: Option<u64>,
    pub outcome: ReplyOutcome,
}

impl Step {
    pub fn inputs(name: &'static str, why: &'static str, inputs: Vec<Input>) -> Self {
        Self {
            name,
            why,
            inputs: inputs
                .into_iter()
                .map(Box::new)
                .map(Stimulus::Input)
                .collect(),
        }
    }

    pub fn replies(name: &'static str, why: &'static str, replies: Vec<AppendReply>) -> Self {
        Self {
            name,
            why,
            inputs: replies.into_iter().map(Stimulus::Reply).collect(),
        }
    }
}

impl AppendReply {
    pub fn accepted(follower: NodeId, through: LogIndex) -> Self {
        Self {
            follower,
            through,
            sequence: None,
            outcome: ReplyOutcome::Accepted,
        }
    }

    fn resolve(&self, sent: &[Output]) -> Input {
        let request = sent
            .iter()
            .rev()
            .find_map(|output| match output {
                Output::Send {
                    to,
                    message: Message::AppendEntries(request),
                } if *to == self.follower
                    && LogIndex(request.prev_log_index.0 + request.entries.len() as u64)
                        == self.through
                    && self
                        .sequence
                        .is_none_or(|sequence| sequence == request.sequence) =>
                {
                    Some(request)
                }
                _ => None,
            })
            .expect("no emitted append matches this walkthrough reply");
        let (term, success, match_index) = match self.outcome {
            ReplyOutcome::Accepted => (request.term, true, self.through),
            ReplyOutcome::Rejected { term } => {
                assert!(
                    term >= request.term,
                    "a rejection cannot report an older term"
                );
                (term, false, LogIndex::ZERO)
            }
        };
        Input::Message {
            from: self.follower,
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                follower_id: self.follower,
                term,
                success,
                match_index,
                sequence: request.sequence,
            }),
        }
    }
}

impl Stimulus {
    pub fn resolve(&self, sent: &[Output]) -> Input {
        match self {
            Self::Input(input) => input.as_ref().clone(),
            Self::Reply(reply) => reply.resolve(sent),
        }
    }
}
