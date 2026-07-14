//! Criterion micro-benchmarks for the kernel's hot paths: proposal steps,
//! follower append steps, and acknowledgement-paced window fills (which is
//! where batch construction and payload sharing live).

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rafter::{
    AppendEntries, AppendEntriesResponse, Input, LogEntry, LogIndex, Message, Node, NodeConfig,
    NodeId, Output, Role, Term,
};

const PAYLOAD_BYTES: usize = 256;

/// Elects node 1 leader of {1, 2, 3} through the public API by scripting
/// node 2's vote.
fn elected_leader() -> Node {
    let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 1)
        .expect("bench config is valid")
        .with_pre_vote(false);
    let mut node = Node::new(config);
    let outputs = node.step(Input::Tick);
    let term = outputs
        .iter()
        .find_map(|output| match output {
            Output::Send {
                message: Message::RequestVote(request),
                ..
            } => Some(request.term),
            _ => None,
        })
        .expect("tick starts an election");
    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(rafter::RequestVoteResponse {
            term,
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    assert_eq!(node.role(), Role::Leader);
    node
}

/// A leader whose log holds `entries` committed-term entries and whose
/// followers have confirmed positions, so an acknowledgement triggers a
/// window fill over the pending suffix.
fn leader_with_suffix(entries: u64) -> Node {
    let mut leader = elected_leader();
    for _ in 0..entries {
        let _ = leader.step(Input::ClientProposal {
            payload: vec![0xA5; PAYLOAD_BYTES],
        });
    }
    leader
}

fn ack(from: u64, match_index: u64) -> Input {
    Input::Message {
        from: NodeId(from),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(1),
            follower_id: NodeId(from),
            success: true,
            match_index: LogIndex(match_index),
            sequence: 0,
        }),
    }
}

fn proposal_step(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("proposal_step");
    group.throughput(Throughput::Elements(64));
    group.bench_function("64_proposals_256b", |bencher| {
        bencher.iter_batched_ref(
            elected_leader,
            |leader| {
                for _ in 0..64 {
                    std::hint::black_box(leader.step(Input::ClientProposal {
                        payload: vec![0xA5; PAYLOAD_BYTES],
                    }));
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn follower_append_step(criterion: &mut Criterion) {
    let entries: Vec<LogEntry> = (0..16)
        .map(|_| LogEntry::application(Term(1), vec![0xA5; PAYLOAD_BYTES]))
        .collect();
    let message = Message::AppendEntries(AppendEntries {
        term: Term(1),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(0),
        prev_log_term: Term(0),
        sequence: 1,
        entries: entries.into(),
        leader_commit: LogIndex(0),
    });
    let follower_config = NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 1_000_000)
        .expect("bench config is valid");

    let mut group = criterion.benchmark_group("follower_append_step");
    group.throughput(Throughput::Elements(16));
    group.bench_function("16_entries_256b", |bencher| {
        bencher.iter_batched_ref(
            || Node::new(follower_config.clone()),
            |follower| {
                std::hint::black_box(follower.step(Input::Message {
                    from: NodeId(1),
                    message: message.clone(),
                }));
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn window_fill_on_ack(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("window_fill_on_ack");
    group.throughput(Throughput::Elements(1));
    group.bench_function("512_entry_suffix", |bencher| {
        bencher.iter_batched_ref(
            || leader_with_suffix(512),
            |leader| {
                // The probe acknowledgement flips the follower to Replicate
                // and fills the whole in-flight window from the suffix:
                // batch construction over shared payloads is the cost here.
                std::hint::black_box(leader.step(ack(2, 0)));
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    kernel,
    proposal_step,
    follower_append_step,
    window_fill_on_ack
);
criterion_main!(kernel);
