//! Who may say what, and the arm that used to decide it for itself.
//!
//! The sibling `funnel` module pins the two gates that closed
//! `handle_client_result`. These pin the arm beside it, and then the whole
//! table, because the arm beside it is how the same defect survived that fix.
//!
//! `handle_envelope`'s own account of itself read:
//!
//! > The three named arms are the whole of the harness's own vocabulary, and
//! > each decides for itself which senders it will hear from — `handle_raft`
//! > and `handle_client_result` both require a node this cluster knows.
//!
//! Three arms, two gates. `client_forward` was matched *above* the catch-all,
//! so the catch-all's `!name_to_id.contains_key(src)` test never saw it, and
//! `handle_forward` took `envelope.src` as the node a client's answer would be
//! addressed to with no test of what that src was:
//!
//! ```text
//! {"src":"n1","dest":"c2","body":{"client":"c1","in_reply_to":5,"result":{"kind":"write_ok"},"type":"client_result"}}
//! ```
//!
//! That is a write executed on `c1`'s behalf and its answer mailed to `c2` —
//! whoever asked. The cost was the same one `handle_client_result`'s gate names
//! as the reason it exists: `completed_replies` gained `("c1", 5)`, so `c1`'s
//! genuine request for that key was refused *above* the accept, with no record,
//! no deadline and no answer.
//!
//! So no arm decides for itself now. The dispatch resolves the sender once,
//! into a `Peer` only a membership hit can produce, and matches on the pair
//! `(HarnessMessage, sender)`. The first test below is the forgery; the second
//! is what it cost; the last walks every row of that pair, because "each arm
//! decides which senders it hears from" is a claim about a table, and the way
//! it failed was a row nobody had written down.

use rafter::{AppendEntries, LogIndex, Message, NodeId, Term};
use rafter_codec::encode_message;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};
use serde_json::{json, Value};

use crate::{
    protocol::{body_type, encode_hex, Envelope},
    InitializedNode,
};

use super::{
    client_write, direct_answer, elected_single_node_process, remove_test_root, test_root,
};

/// A `client_forward` from a non-node lodges nothing and answers nobody.
///
/// This is the `client_result` hole with one word changed. The sender gate
/// added for that arm was not reached here, and `handle_forward` derived the
/// record's recipient from `envelope.src` with no test of what that src was.
///
/// The assertions are the ones
/// `funnel::a_client_result_for_a_key_no_record_exists_for_is_not_acted_on`
/// makes for the gated arm, on the arm beside it.
#[test]
fn a_client_forward_from_a_client_lodges_no_record_and_reaches_no_state_machine() {
    let root = test_root("dispatch-forward-sender");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");

    oracle_assert!(node.owed_answers.is_empty(), "nothing accepted yet");
    oracle_assert!(node.completed_replies.is_empty(), "nothing answered yet");

    node.handle_envelope(forged_forward("c2", "c1", 5, "counter", 7));

    oracle_assert!(
        !node.completed_replies.contains(&("c1".to_owned(), 5)),
        "the at-most-once set may not gain a key on the word of a non-node — it \
         is read above the accept, so a key in it refuses the client's own \
         request; emitted = {:#?}",
        node.emitted
    );
    oracle_assert!(
        node.owed_answers.is_empty(),
        "and no obligation to a non-node may be accepted either"
    );
    oracle_assert_eq!(
        node.app.kv.get("\"counter\""),
        None,
        "and no mutation may reach the state machine on a non-node's say-so"
    );
    remove_test_root(root);
}

/// The consequence, which is the one `handle_client_result`'s gate exists to
/// prevent: the client's genuine request swallowed, and its answer mailed to
/// the forger.
///
/// `funnel::a_stray_result_does_not_swallow_the_clients_real_request` pins
/// exactly this outcome for the `client_result` arm. The same two-message
/// sequence through `client_forward` still reached it until the dispatch
/// resolved the sender for every arm rather than for two of three.
#[test]
fn a_forged_forward_does_not_swallow_the_clients_real_request() {
    let root = test_root("dispatch-forward-swallow");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");

    // A non-node claims to be relaying c1's request for msg_id 5.
    node.handle_envelope(forged_forward("c2", "c1", 5, "counter", 7));
    for _ in 0..8 {
        process.tick();
    }

    // c1's own write for that key now arrives at the same leader.
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");
    node.handle_envelope(client_write("n1", "c1", 5, "counter", 7));
    for _ in 0..8 {
        process.tick();
    }
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");

    oracle_assert_eq!(
        direct_answer(node, "c1", 5)
            .as_ref()
            .and_then(|body| body.get("type").and_then(Value::as_str)),
        Some("write_ok"),
        "the client's own write must draw its own answer; emitted = {:#?}",
        node.emitted
    );
    oracle_assert_eq!(
        answers_to(node, "c2", 5),
        0,
        "and no answer for a client's request may be mailed to a non-node that \
         asked for it; emitted = {:#?}",
        node.emitted
    );
    remove_test_root(root);
}

/// Every row of the dispatch's table, walked.
///
/// "Each arm decides which senders it will hear from" was a claim about a
/// table, checked one arm at a time and never as a table — which is how it came
/// to name two arms of three and be believed. The table is now
/// `(HarnessMessage, sender)` and rustc keeps it exhaustive; this walks it, so
/// that the rows with nothing else standing behind them are checked somewhere.
///
/// | Message | Sender | Outcome | Also pinned by |
/// | --- | --- | --- | --- |
/// | `raft` | peer | stepped | `obligation::a_stale_leader_announcement_does_not_replace_the_known_leader`, and every fixture that elects a leader |
/// | `client_forward` | peer | accepted | the whole `obligation` module |
/// | `client_result` | peer | pays a record | `funnel::a_client_result_from_a_peer_still_pays_the_record_it_names` |
/// | any of the three | non-peer | refused | here, and the two tests above |
/// | anything else | non-peer | a client request | `funnel::an_operation_the_dispatch_does_not_name_is_answered_rather_than_dropped` |
/// | anything else | peer | ignored | here |
///
/// The last row is what the catch-all's sender test used to hold on its own,
/// and it is why the client arm may be a catch-all at all: a message from a
/// cluster node that is none of the three above is a harness message this build
/// does not know, not a client request, and accepting it as one would lodge an
/// obligation to a node.
#[test]
fn every_row_of_the_dispatch_decides_which_senders_it_hears_from() {
    let root = test_root("dispatch-table");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");

    // A non-node may say none of the three harness types. Each is checked for
    // the effect it would have had rather than only for silence: a refusal that
    // let the message through and happened to emit nothing would pass a wire
    // check and be no refusal at all.
    for (label, body) in [
        (
            "raft",
            json!({ "type": "raft", "frame": encode_hex(&usurping_heartbeat()) }),
        ),
        (
            "client_result",
            json!({
                "type": "client_result",
                "client": "c1",
                "in_reply_to": 11,
                "result": { "kind": "write_ok" },
            }),
        ),
        (
            "client_forward",
            json!({
                "type": "client_forward",
                "client": "c1",
                "in_reply_to": 12,
                "request": { "type": "write", "msg_id": 12, "key": "counter", "value": 1 },
            }),
        ),
    ] {
        let emitted_before = node.emitted.len();
        let term_before = node.node.current_term();
        node.handle_envelope(Envelope {
            src: "c2".to_owned(),
            dest: "n1".to_owned(),
            body,
        });
        oracle_assert_eq!(
            node.emitted.len(),
            emitted_before,
            "a {label} from a non-node must put nothing on the wire; \
             emitted = {:#?}",
            node.emitted
        );
        oracle_assert_eq!(
            node.node.current_term(),
            term_before,
            "and a {label} from a non-node must not reach the kernel"
        );
    }
    oracle_assert!(
        !node.owed_answers.is_owed(&("c1".to_owned(), 12)),
        "and the forward among them must lodge no record: the arm beside the \
         gate is a gate on who may speak for a client, not on one message type"
    );
    oracle_assert!(
        node.completed_replies.is_empty(),
        "nor may any of them mark a client's request answered"
    );

    // A peer saying something that is not one of the three is a harness message
    // this build does not know. It is not a client request, and treating it as
    // one would lodge an obligation to a node.
    let emitted_before = node.emitted.len();
    node.handle_envelope(Envelope {
        src: "n1".to_owned(),
        dest: "n1".to_owned(),
        body: json!({ "type": "increment", "msg_id": 13, "key": "counter" }),
    });
    oracle_assert_eq!(
        node.emitted.len(),
        emitted_before,
        "an unknown type from a peer is ignored rather than answered as a \
         client request; emitted = {:#?}",
        node.emitted
    );
    oracle_assert!(
        node.owed_answers.is_empty(),
        "and no obligation to a node is accepted for it"
    );

    // The control, so that "closed" cannot mean "refuses everything": the same
    // shape of request, from an actual client, is served.
    node.handle_envelope(client_write("n1", "c1", 14, "counter", 3));
    for _ in 0..8 {
        process.tick();
    }
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");
    oracle_assert_eq!(
        direct_answer(node, "c1", 14)
            .as_ref()
            .and_then(|body| body.get("type").and_then(Value::as_str)),
        Some("write_ok"),
        "a client's own request is still served; emitted = {:#?}",
        node.emitted
    );
    remove_test_root(root);
}

/// A `client_forward` as a non-node would forge it.
fn forged_forward(src: &str, client: &str, in_reply_to: u64, key: &str, value: u64) -> Envelope {
    Envelope {
        src: src.to_owned(),
        dest: "n1".to_owned(),
        body: json!({
            "type": "client_forward",
            "client": client,
            "in_reply_to": in_reply_to,
            "request": { "type": "write", "msg_id": in_reply_to, "key": key, "value": value },
        }),
    }
}

/// An empty `AppendEntries` from a term far above this node's, framed the way
/// the wire carries it.
///
/// A term the kernel could not ignore if it reached it, so "the term did not
/// move" is evidence that the dispatch refused the message rather than evidence
/// that the kernel found it uninteresting.
fn usurping_heartbeat() -> Vec<u8> {
    let message = Message::AppendEntries(AppendEntries {
        term: Term(99),
        leader_id: NodeId(1),
        prev_log_index: LogIndex::ZERO,
        prev_log_term: Term(0),
        sequence: 1,
        entries: Vec::new().into(),
        leader_commit: LogIndex::ZERO,
    });
    encode_message(&message).expect("message encodes")
}

/// How many `client_result` envelopes this node mailed to `dest` for one key.
fn answers_to(node: &InitializedNode, dest: &str, in_reply_to: u64) -> usize {
    node.emitted
        .iter()
        .filter(|envelope| {
            envelope.dest == dest
                && body_type(&envelope.body) == Some("client_result")
                && envelope.body.get("in_reply_to").and_then(Value::as_u64) == Some(in_reply_to)
        })
        .count()
}
