//! The reply path: who answers a committed command, and what a flush retires
//! against.
//!
//! Every replica applies every committed command and computes the identical
//! result, so `command.origin` alone cannot say who mails the answer — it reads
//! the same on the node that accepted the client's request and on the ones that
//! merely replicated the entry. These pin the rule that separates them, in both
//! directions: a node holding the obligation answers though it does not lead, a
//! node holding none stays silent though it applied. The `client` module header
//! carries the argument, and `read_tests` pins the other rule, for reads.
//!
//! The [`obligation`] submodule holds the other half of the story: that
//! somebody answers at all, however far the request travelled and whatever
//! became of the entry behind it.

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{
    AppendEntries, AppendEntriesResponse, Input, LogEntry, LogIndex, Message, NodeId,
    PreVoteResponse, RequestVoteResponse, Role, Term,
};
use rafter_codec::{decode_message, encode_message};
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};
use serde_json::{json, Value};

use crate::{
    protocol::{body_type, decode_hex, encode_hex, Envelope},
    InitializedNode, MaelstromNode, PendingRead,
};

mod dispatch;
mod funnel;
mod obligation;
mod scope;

static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

/// A node that only replicated somebody else's write does not answer for it.
///
/// `apply_command` reaches the reply path on every node that applies a
/// committed entry, and only the local obligation record stops it sending.
/// Nothing in the payload distinguishes this node — which never saw the client
/// or the forward — from the one that accepted the request, so without that
/// record a plain follower mails an answer on another node's behalf.
#[test]
fn a_replicating_follower_does_not_answer_for_the_node_that_accepted_the_request() {
    let root = test_root("reply-follower-apply");
    let mut process = fresh_cluster_member(&root, "n3", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");
    oracle_assert!(node.node.role() != Role::Leader, "n3 is a plain follower");

    // n1 leads. c1's write reached n2, which forwarded it; n1 proposed it with
    // origin = "n2". n3 only replicates it, and holds no obligation for it.
    node.step(replicate(
        &[forwarded_write("n2", "c1", 5, "counter", 7)],
        1,
    ));

    oracle_assert_eq!(
        node.app.applied,
        LogIndex(1),
        "the follower applied the replicated command"
    );
    oracle_assert!(node.node.role() != Role::Leader, "still a follower");
    oracle_assert_eq!(
        forwarded_answer(node, "n2", 5),
        None,
        "a follower that merely replicated the write must not mail an answer to \
         the node that accepted the request"
    );
    remove_test_root(root);
}

/// The node that accepted a peer's forward answers it, though it does not lead.
///
/// The counterpart of the test above, on the same node applying the same entry:
/// what changes is the obligation, not the role. This is the interleaving that
/// rules role out as the test — a node that accepted a forward, proposed it,
/// and lost leadership before the entry committed under the next leader is the
/// only node that owes that peer an answer, and it is not the leader. A role
/// gate silences exactly this node and lets the new leader, which owes nothing,
/// speak instead.
#[test]
fn a_node_that_accepted_a_forward_answers_it_though_it_does_not_lead() {
    let root = test_root("reply-accepted-forward");
    let mut process = fresh_cluster_member(&root, "n3", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");
    oracle_assert!(node.node.role() != Role::Leader, "n3 does not lead");

    // The state `propose` leaves behind on a node that accepted n2's forward,
    // written through the same call `propose` makes. The restart test below
    // drives it in end to end, from a real `client_forward` on a real leader.
    node.accept_answer_obligation("n2", "c1", 5);

    // The entry commits under whoever leads now, and reaches n3 by replication.
    node.step(replicate(
        &[forwarded_write("n2", "c1", 5, "counter", 7)],
        1,
    ));

    oracle_assert!(
        node.node.role() != Role::Leader,
        "the answer must be produced while this node is not the leader"
    );
    oracle_assert_eq!(
        forwarded_answer(node, "n2", 5),
        Some(json!({ "kind": "write_ok" })),
        "the node that accepted the forward owes that peer the answer and mails it"
    );
    oracle_assert!(
        node.owed_answers.is_empty(),
        "the obligation is discharged, so a second apply cannot mail it again"
    );
    remove_test_root(root);
}

/// A restart does not re-mail answers for the entries recovery replays.
///
/// The reply-dedupe set does not survive a restart, so nothing downstream
/// suppresses a second answer for an entry this process applies again. Nothing
/// is owed: the peer that was waiting on this process is not waiting on the one
/// that replaced it. Keeping the obligation out of the command payload is what
/// makes the distinction survivable — a proposer field would replay right along
/// with the entry.
///
/// The relaying peer is `n2` of a three-node cluster rather than a name a
/// single-node cluster has never heard of. That was the shape of this test
/// until the dispatch gated `client_forward`, and under the gate it would have
/// been a test of the forgery path: the entry whose origin the restart must not
/// answer for was lodged by a message no peer sent.
#[test]
fn a_restart_does_not_remail_answers_for_replayed_entries() {
    let root = test_root("reply-recovery-replay");

    // Phase 1, the production path in: a leader accepts a real forward,
    // proposes it, and answers the node that forwarded it exactly once.
    {
        let (mut process, mut peers) = elected_cluster_leader(&root);
        let node = process.initialized.as_mut().expect("node initializes");
        node.handle_envelope(Envelope {
            src: "n2".to_owned(),
            dest: "n1".to_owned(),
            body: json!({
                "type": "client_forward",
                "client": "c1",
                "in_reply_to": 5,
                "request": { "type": "write", "key": "counter", "value": 7 },
            }),
        });
        let answered = peers.settle(&mut process, |node| {
            forwarded_answer(node, "n2", 5).is_some()
        });
        let node = process
            .initialized
            .as_mut()
            .expect("node stays initialized");
        oracle_assert!(
            answered,
            "the leader that accepted the forward answers the forwarding node; \
             emitted = {:#?}",
            node.emitted
        );
        oracle_assert_eq!(
            forwarded_answer(node, "n2", 5),
            Some(json!({ "kind": "write_ok" })),
            "and answers it with the committed outcome"
        );
        oracle_assert_eq!(node.app.applied, LogIndex(2));
    }

    // Roll the application's applied cursor back one entry, the way a crash
    // between the raft persist and the app persist leaves it.
    let app_path = root.join("app.json");
    let mut app: Value =
        serde_json::from_slice(&std::fs::read(&app_path).expect("app state exists"))
            .expect("app state parses");
    app["applied"] = json!(1);
    app["kv"] = json!({});
    std::fs::write(
        &app_path,
        serde_json::to_vec(&app).expect("app state serializes"),
    )
    .expect("app state rewrites");

    // Phase 2: restart, and the recovered process applies index 2 again — here
    // by winning the next election, which commits everything below its own
    // term's noop. The volatile ledger did not survive, so nothing on this node
    // records that n2 was ever waiting.
    let mut process = MaelstromNode::default();
    process
        .initialize_at_root(&init_envelope("n1", &["n1", "n2", "n3"]), root.clone())
        .expect("production Maelstrom initialization reopens the node");
    let mut peers = PeerCluster { answered: 0 };
    let replayed = peers.settle(&mut process, |node| node.app.applied >= LogIndex(2));
    let node = process.initialized.as_mut().expect("node reinitializes");
    oracle_assert!(replayed, "the recovered process applied the entry again");
    oracle_assert_eq!(node.app.applied, LogIndex(2), "recovery replayed the entry");
    oracle_assert_eq!(
        node.app.kv.get("\"counter\""),
        Some(&json!(7)),
        "and the mutation reached the state machine a second time"
    );
    oracle_assert_eq!(
        forwarded_answer(node, "n2", 5),
        None,
        "a restart must not re-mail an answer for a request it no longer owes"
    );
    remove_test_root(root);
}

/// A follower does not amplify the leader's own client answers back to it.
///
/// The origin recorded in a command a leader proposed for its own client is the
/// leader itself, so an origin-only rule makes every follower mail the leader a
/// `client_result` for every committed write in the cluster: `N - 1` redundant
/// envelopes per request.
#[test]
fn followers_do_not_amplify_client_results_to_the_leader() {
    let root = test_root("reply-amplification");
    let mut process = fresh_cluster_member(&root, "n3", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");

    // n1 leads and served c1 directly, so each committed command's origin is n1.
    let entries = (1..=3)
        .map(|n| forwarded_write("n1", "c1", 200 + n, &format!("k{n}"), n))
        .collect::<Vec<_>>();
    node.step(replicate(&entries, 3));

    let amplified = node
        .emitted
        .iter()
        .filter(|envelope| body_type(&envelope.body) == Some("client_result"))
        .count();
    oracle_assert_eq!(
        amplified,
        0,
        "a follower that only replicated the leader's own client commands must \
         not mail the leader an answer for each of them; emitted = {:#?}",
        node.emitted
    );
    remove_test_root(root);
}

/// A committed write is answered even when the application checkpoint cannot be
/// written.
///
/// `apply_committed_command` mutates `app.kv` and advances `app.applied` before
/// it writes the checkpoint, so a checkpoint failure leaves the command applied
/// as far as every later reader on this node is concerned. Durability was never
/// the checkpoint's job — the Raft log holds the committed entry — so the answer
/// is honest and owed. Discarding it strands the write with nothing on the node
/// recording that one is still due, which is the failure the `client` module
/// header rejects for reads.
#[test]
fn a_committed_write_is_answered_even_when_the_checkpoint_cannot_be_written() {
    let root = test_root("reply-persist-error");
    let mut process = fresh_cluster_member(&root, "n3", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");

    // c1 reached n3, which forwarded to the leader; the command carries n3 as
    // its origin, so n3 owes c1 the answer when the entry comes back committed.
    let entry = forwarded_write("n3", "c1", 101, "alpha", 1);

    // The entry commits while the application store cannot be written.
    make_read_only(&root, true);
    node.step(replicate(&[entry], 1));
    make_read_only(&root, false);

    // The apply happened: the in-memory state machine carries the mutation and
    // the applied cursor moved. Only the checkpoint did not.
    oracle_assert_eq!(
        node.app.applied,
        LogIndex(1),
        "the in-memory applied cursor advanced past the failed checkpoint"
    );
    oracle_assert_eq!(
        node.app.kv.get("\"alpha\""),
        Some(&json!(1)),
        "the in-memory state machine carries the mutation"
    );
    oracle_assert!(
        direct_answer(node, "c1", 101).is_some(),
        "a command applied into the state machine must produce an answer for the \
         client whose request this node accepted"
    );
    remove_test_root(root);
}

/// A flush retires a waiter only against an answer the client holds.
///
/// `flush_reads` retires unconditionally, which is sound only because
/// `deliver_result` discharges the obligation on every call. Its one
/// non-sending arm is its own `completed_replies` dedupe, and this pins that
/// arm's reading: it is reached with a real prior emit behind it, and leaves the
/// client holding an answer rather than none. If the arm ever becomes a genuine
/// drop, the retirement above it has to become conditional in the same change.
#[test]
fn a_flush_retires_a_waiter_only_against_an_answer_the_client_holds() {
    let root = test_root("reply-flush-retires-against-delivery");
    let mut process = fresh_cluster_member(&root, "n1", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");

    // (c1, 21) is answered once through the production path: the
    // temporarily-unavailable error a node sends when no leader is known yet.
    node.handle_envelope(read_envelope("c1", 21, "counter"));
    oracle_assert_eq!(
        direct_answers(node, "c1", 21),
        1,
        "the request already left this node holding an answer"
    );
    oracle_assert!(node.completed_replies.contains(&("c1".to_owned(), 21)));

    // A waiter for that same request now reaches its floor, so the flush
    // delivers into the dedupe arm and retires against it.
    node.pending_reads.insert(
        1,
        PendingRead {
            origin: node.name.clone(),
            client: "c1".to_owned(),
            in_reply_to: 21,
            key: json!("counter"),
            application_floor: Some(LogIndex::ZERO),
        },
    );
    node.flush_reads();

    oracle_assert!(node.pending_reads.is_empty(), "the waiter was retired");
    oracle_assert_eq!(
        direct_answers(node, "c1", 21),
        1,
        "the retired waiter's client holds an answer — the arm that declined to \
         send a second one is a suppressed duplicate, not a dropped read"
    );
    remove_test_root(root);
}

fn make_read_only(root: &Path, read_only: bool) {
    use std::os::unix::fs::PermissionsExt;
    let mode = if read_only { 0o555 } else { 0o755 };
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(mode))
        .expect("test root permissions change");
}

/// A client's `write` arriving straight at `dest`.
fn client_write(dest: &str, client: &str, msg_id: u64, key: &str, value: u64) -> Envelope {
    Envelope {
        src: client.to_owned(),
        dest: dest.to_owned(),
        body: json!({ "type": "write", "msg_id": msg_id, "key": key, "value": value }),
    }
}

/// How many `client_forward` envelopes this node put on the wire.
fn client_forwards(node: &InitializedNode) -> usize {
    node.emitted
        .iter()
        .filter(|envelope| body_type(&envelope.body) == Some("client_forward"))
        .count()
}

/// One committed application entry for a write `origin` accepted from `client`.
fn forwarded_write(
    origin: &str,
    client: &str,
    in_reply_to: u64,
    key: &str,
    value: u64,
) -> LogEntry {
    let payload = serde_json::to_vec(&json!({
        "origin": origin,
        "client": client,
        "in_reply_to": in_reply_to,
        "request": { "op": "write", "key": key, "value": value },
    }))
    .expect("command serializes");
    LogEntry::application(Term(1), payload)
}

/// The leader's replication of `entries`, committed through `leader_commit`.
fn replicate(entries: &[LogEntry], leader_commit: u64) -> Input {
    Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            term: Term(1),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term(0),
            sequence: 1,
            entries: entries.to_vec().into(),
            leader_commit: LogIndex(leader_commit),
        }),
    }
}

/// The result this node handed back to `origin` for a request `origin`
/// forwarded to it.
fn forwarded_answer(node: &InitializedNode, origin: &str, in_reply_to: u64) -> Option<Value> {
    node.emitted
        .iter()
        .find(|envelope| {
            envelope.dest == origin
                && body_type(&envelope.body) == Some("client_result")
                && envelope.body.get("in_reply_to").and_then(Value::as_u64) == Some(in_reply_to)
        })
        .and_then(|envelope| envelope.body.get("result").cloned())
}

/// The reply this node sent straight to `client`.
fn direct_answer(node: &InitializedNode, client: &str, in_reply_to: u64) -> Option<Value> {
    node.emitted
        .iter()
        .find(|envelope| {
            envelope.dest == client
                && envelope.body.get("in_reply_to").and_then(Value::as_u64) == Some(in_reply_to)
        })
        .map(|envelope| envelope.body.clone())
}

/// How many replies this node sent straight to `client` for one request.
fn direct_answers(node: &InitializedNode, client: &str, in_reply_to: u64) -> usize {
    node.emitted
        .iter()
        .filter(|envelope| {
            envelope.dest == client
                && envelope.body.get("in_reply_to").and_then(Value::as_u64) == Some(in_reply_to)
        })
        .count()
}

fn fresh_cluster_member(root: &Path, node_name: &str, node_names: &[&str]) -> MaelstromNode {
    let mut process = MaelstromNode::default();
    process
        .initialize_at_root(&init_envelope(node_name, node_names), root.to_path_buf())
        .expect("production Maelstrom initialization opens a fresh node");
    process
}

/// The other two members of a three-node cluster, standing in for the network.
///
/// Every test that needs a *peer* needs one of these, and until the dispatch
/// gated `client_forward` none of them had one. They used
/// [`elected_single_node_process`] — whose membership holds exactly `n1` — and
/// sent their forwards from `"n2"`, a name that cluster has never heard of. So
/// the peer path was being read while a non-peer's message was being sent, and
/// the tests passed because nothing looked. Widening the gate and leaving them
/// alone would have meant keeping four tests that assert the peer contract
/// through the forgery path.
///
/// This answers what the node under test says to its peers, in the terms the
/// wire carries: the frames are decoded, replied to, and handed back through
/// [`InitializedNode::handle_envelope`], so the responses go through the same
/// dispatch and the same membership gate as production traffic. Peers grant
/// votes and acknowledge appends; nothing here models a peer that disagrees,
/// because these tests are about the reply path and not about consensus.
struct PeerCluster {
    /// How far into `emitted` this cluster has already answered. A single
    /// cursor, because answering a frame produces more frames and the next
    /// round has to start after them rather than at the top.
    answered: usize,
}

impl PeerCluster {
    /// Runs rounds of "answer everything outstanding, then tick" until `settled`
    /// holds, and reports whether it did.
    ///
    /// Bounded rather than looping until quiet: a leader heartbeats forever, so
    /// "no traffic left" is not a state this reaches. Callers assert on the
    /// return value, which keeps a fixture that stopped converging from reading
    /// as a passing test.
    fn settle(
        &mut self,
        process: &mut MaelstromNode,
        settled: impl Fn(&InitializedNode) -> bool,
    ) -> bool {
        const ROUNDS: usize = 64;
        for _ in 0..ROUNDS {
            if settled(
                process
                    .initialized
                    .as_ref()
                    .expect("node stays initialized"),
            ) {
                return true;
            }
            self.answer(
                process
                    .initialized
                    .as_mut()
                    .expect("node stays initialized"),
            );
            process.tick();
        }
        settled(
            process
                .initialized
                .as_ref()
                .expect("node stays initialized"),
        )
    }

    /// Runs a fixed number of rounds, for a caller checking that something does
    /// *not* happen.
    ///
    /// A predicate would have to be the negation, and [`Self::settle`] returning
    /// early on a negation is exactly the reading that makes "nothing happened"
    /// mean "we did not wait".
    fn exchange(&mut self, process: &mut MaelstromNode, rounds: usize) {
        for _ in 0..rounds {
            self.answer(
                process
                    .initialized
                    .as_mut()
                    .expect("node stays initialized"),
            );
            process.tick();
        }
    }

    /// Answers every framed message this node has sent a peer since the last
    /// round.
    fn answer(&mut self, node: &mut InitializedNode) {
        let outstanding = node
            .emitted
            .iter()
            .skip(self.answered)
            .cloned()
            .collect::<Vec<_>>();
        self.answered = node.emitted.len();
        for envelope in outstanding {
            let Some(reply) = peer_reply(node, &envelope) else {
                continue;
            };
            node.handle_envelope(reply);
        }
    }
}

/// One peer's answer to one framed message, as an envelope from that peer.
fn peer_reply(node: &InitializedNode, envelope: &Envelope) -> Option<Envelope> {
    if body_type(&envelope.body) != Some("raft") {
        return None;
    }
    let frame = envelope.body.get("frame").and_then(Value::as_str)?;
    let bytes = decode_hex(frame).expect("this node frames valid hex");
    let message = decode_message(&bytes).expect("this node frames a valid message");
    let peer = *node
        .name_to_id
        .get(&envelope.dest)
        .expect("a raft frame is addressed to a node of this cluster");
    let answer = match message {
        Message::PreVote(poll) => Message::PreVoteResponse(PreVoteResponse {
            term: poll.term,
            voter_id: peer,
            vote_granted: true,
        }),
        Message::RequestVote(request) => Message::RequestVoteResponse(RequestVoteResponse {
            term: request.term,
            voter_id: peer,
            vote_granted: true,
        }),
        Message::AppendEntries(append) => Message::AppendEntriesResponse(AppendEntriesResponse {
            term: append.term,
            follower_id: peer,
            success: true,
            match_index: LogIndex(
                append.prev_log_index.0
                    + u64::try_from(append.entries.len()).expect("entry count fits u64"),
            ),
            sequence: append.sequence,
        }),
        _ => return None,
    };
    let frame = encode_message(&answer).expect("response encodes");
    Some(Envelope {
        src: envelope.dest.clone(),
        dest: node.name.clone(),
        body: json!({ "type": "raft", "frame": encode_hex(&frame) }),
    })
}

/// A leader of a three-node cluster, elected by peers that answered it.
///
/// The fixture for anything involving a relaying peer, and the reason it is
/// worth its weight: `n2` is a node of this cluster here, so a `client_forward`
/// from `n2` is the message a peer sends rather than the forgery the dispatch
/// now refuses.
fn elected_cluster_leader(root: &Path) -> (MaelstromNode, PeerCluster) {
    let mut process = fresh_cluster_member(root, "n1", &["n1", "n2", "n3"]);
    let mut peers = PeerCluster { answered: 0 };
    let elected = peers.settle(&mut process, |node| {
        node.node.role() == Role::Leader && node.node.commit_index() > LogIndex::ZERO
    });
    oracle_assert!(elected, "n1 stands for election and its two peers elect it");
    (process, peers)
}

fn elected_single_node_process(root: &Path) -> MaelstromNode {
    let mut process = MaelstromNode::default();
    process
        .initialize_at_root(&init_envelope("n1", &["n1"]), root.to_path_buf())
        .expect("production Maelstrom initialization opens a fresh node");
    for _ in 0..20 {
        if process
            .initialized
            .as_ref()
            .expect("node initializes")
            .node
            .commit_index()
            > LogIndex::ZERO
        {
            break;
        }
        process.tick();
    }
    process
}

fn init_envelope(node_name: &str, node_names: &[&str]) -> Envelope {
    Envelope {
        src: "controller".to_owned(),
        dest: node_name.to_owned(),
        body: json!({
            "type": "init",
            "msg_id": 1,
            "node_id": node_name,
            "node_ids": node_names,
        }),
    }
}

fn read_envelope(client: &str, msg_id: u64, key: &str) -> Envelope {
    Envelope {
        src: client.to_owned(),
        dest: "n1".to_owned(),
        body: json!({ "type": "read", "msg_id": msg_id, "key": key }),
    }
}

fn test_root(name: &str) -> PathBuf {
    let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "rafter-maelstrom-{name}-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    root
}

fn remove_test_root(root: PathBuf) {
    let _ = std::fs::remove_dir_all(root);
}
