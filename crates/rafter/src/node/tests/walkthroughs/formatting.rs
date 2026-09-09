//! Deliberately selective prose for the messages exercised by these stories.

use std::fmt::Write;

use super::support::*;

pub(super) fn role(role: Role) -> &'static str {
    match role {
        Role::Follower => "follower",
        Role::PreCandidate => "pre-candidate",
        Role::Candidate => "candidate",
        Role::Leader => "leader",
    }
}

pub(super) fn vote(vote: Option<NodeId>) -> String {
    vote.map_or_else(|| "none".into(), |id| format!("node {}", id.0))
}

pub(super) fn ids(nodes: &[NodeId]) -> String {
    nodes
        .iter()
        .map(|id| id.0.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn membership(membership: &MembershipConfig) -> String {
    let set = |set: &MembershipSet| {
        let mut text = format!("voters {}", ids(set.voters()));
        if !set.learners().is_empty() {
            write!(text, "; learners {}", ids(set.learners())).unwrap();
        }
        text
    };
    match membership {
        MembershipConfig::Stable(members) => set(members),
        MembershipConfig::Joint(joint) => format!(
            "joint membership: old {}; new {}",
            set(joint.old()),
            set(joint.new_membership())
        ),
    }
}

fn command(payload: &[u8]) -> &str {
    let text = std::str::from_utf8(payload).expect("use readable UTF-8 commands in walkthroughs");
    assert!(
        !text.chars().any(char::is_control),
        "use printable commands in walkthroughs"
    );
    text
}

fn message(message: &Message) -> String {
    match message {
        Message::PreVote(vote) => format!(
            "pre-vote request for prospective term {}; candidate {}; last entry {} / term {}",
            vote.term.0, vote.candidate_id.0, vote.last_log_index.0, vote.last_log_term.0
        ),
        Message::PreVoteResponse(vote) => {
            if vote.vote_granted {
                format!("pre-vote granted for prospective term {}", vote.term.0)
            } else {
                format!("pre-vote rejected; responder term {}", vote.term.0)
            }
        }
        Message::RequestVote(vote) => format!(
            "vote request for term {}; candidate {}; last entry {} / term {}",
            vote.term.0, vote.candidate_id.0, vote.last_log_index.0, vote.last_log_term.0
        ),
        Message::RequestVoteResponse(vote) => format!(
            "vote {}; term {}",
            if vote.vote_granted {
                "granted"
            } else {
                "rejected"
            },
            vote.term.0
        ),
        Message::AppendEntries(append) => {
            let end = append.prev_log_index.0 + append.entries.len() as u64;
            let entries = if append.entries.is_empty() {
                "no entries".into()
            } else if append.entries.len() == 1 {
                format!("entry {end}")
            } else {
                format!("entries {}–{end}", append.prev_log_index.0 + 1)
            };
            let entry_terms = append.entries.first().map_or_else(String::new, |first| {
                if append.entries.iter().all(|entry| entry.term == first.term) {
                    format!(" (term {})", first.term.0)
                } else {
                    let terms = append
                        .entries
                        .iter()
                        .map(|entry| entry.term.0.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(" (terms {terms})")
                }
            });
            format!(
                "append #{}; term {}; previous {} / term {}; {}{}; leader commit {}",
                append.sequence,
                append.term.0,
                append.prev_log_index.0,
                append.prev_log_term.0,
                entries,
                entry_terms,
                append.leader_commit.0
            )
        }
        Message::AppendEntriesResponse(response) => {
            if response.success {
                format!(
                    "append #{} accepted; term {}; matching prefix through {}",
                    response.sequence, response.term.0, response.match_index.0
                )
            } else {
                format!(
                    "append #{} rejected; term {}; no matching-prefix acknowledgement",
                    response.sequence, response.term.0
                )
            }
        }
        _ => panic!("add a prose description for this walkthrough message"),
    }
}

pub(super) fn inputs(inputs: &[Input]) -> Vec<String> {
    let mut lines = Vec::new();
    let mut remaining = inputs;
    while let Some((input, tail)) = remaining.split_first() {
        if matches!(input, Input::Tick) {
            let count = remaining
                .iter()
                .take_while(|input| matches!(input, Input::Tick))
                .count();
            lines.push(format!(
                "Advance {count} tick{}.",
                if count == 1 { "" } else { "s" }
            ));
            remaining = &remaining[count..];
            continue;
        }
        lines.push(match input {
            Input::Message {
                from,
                message: frame,
            } => format!("From node {}: {}.", from.0, message(frame)),
            Input::TrackedClientProposal {
                proposal_id,
                payload,
            } => format!(
                "Propose “{}” with local id {}.",
                command(payload),
                proposal_id.0
            ),
            Input::ReadIndex { read_id } => format!("Register read {}.", read_id.0),
            _ => panic!("add a prose description for this walkthrough input"),
        });
        remaining = tail;
    }
    lines
}

pub(super) fn outputs(outputs: &[Output]) -> Vec<String> {
    let mut lines = Vec::new();
    let mut remaining = outputs;
    while let Some((output, tail)) = remaining.split_first() {
        if let Output::Send { message: frame, .. } = output {
            let recipients: Vec<_> = remaining
                .iter()
                .take_while(
                    |output| matches!(output, Output::Send { message, .. } if message == frame),
                )
                .map(|output| match output {
                    Output::Send { to, .. } => *to,
                    _ => unreachable!(),
                })
                .collect();
            let destination = if recipients.len() == 1 {
                format!("node {}", recipients[0].0)
            } else {
                format!("nodes {} (in that order)", ids(&recipients))
            };
            lines.push(format!("Send to {destination}: {}.", message(frame)));
            remaining = &remaining[recipients.len()..];
            continue;
        }
        lines.push(match output {
            Output::LocalProposalAppended {
                proposal_id,
                index,
                term,
            } => format!(
                "Assign proposal {} to index {} / term {}.",
                proposal_id.0, index.0, term.0
            ),
            Output::Apply { index, payload, .. } => format!(
                "Dispatch command “{}” at index {}.",
                command(payload),
                index.0
            ),
            Output::ConfigurationCommitted {
                index,
                configuration,
                ..
            } => {
                let (phase, id) = match configuration {
                    ConfigurationEntry::Stable { config_id, .. } => ("stable", config_id.0),
                    ConfigurationEntry::Joint { config_id, .. } => ("joint", config_id.0),
                };
                format!(
                    "Emit committed {phase} configuration {id} at index {}.",
                    index.0
                )
            }
            Output::ReadIndexGranted {
                read_id,
                read_index,
            } => format!("Grant read {} at barrier {}.", read_id.0, read_index.0),
            Output::ReadIndexCanceled { read_id, reason } => {
                let reason = match reason {
                    ReadIndexCancelReason::LeadershipLost => "leadership ended".into(),
                    ReadIndexCancelReason::LeaderStateReset => "leader read state was reset".into(),
                    ReadIndexCancelReason::LeadershipTransfer { target } => {
                        format!("leadership transfer to node {} began", target.0)
                    }
                };
                format!("Cancel read {}: {reason}.", read_id.0)
            }
            _ => panic!("add a prose description for this walkthrough output"),
        });
        remaining = tail;
    }
    lines
}
