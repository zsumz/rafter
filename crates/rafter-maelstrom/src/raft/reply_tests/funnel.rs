//! The seam beside the funnel, and the two gates that closed it.
//!
//! The `client` module header's first fact is:
//!
//! > **Acting on a client request requires a record for it.**
//! > [`InitializedNode::handle_client_request`] is the single funnel: both entry
//! > points … call it and **nothing else acts on a request**.
//!
//! and the `answers` module says the token makes *acted on implies recorded*
//! "rustc's to keep … rather than a list of accepting paths a reader has to
//! certify".
//!
//! That was false for one path. `handle_client_result` is a third entry point
//! for a client request key, and it took no token: it called `deliver_result`
//! directly with `origin = self.name`, which (a) put a client-addressed answer
//! on the wire and (b) wrote `completed_replies`, the at-most-once set
//! `has_accepted` reads. Neither needed a record to exist.
//!
//! ```text
//! {"src":"n1","dest":"c1","body":{"in_reply_to":5,"msg_id":1,"type":"write_ok"}}
//! {"src":"n1","dest":"c1","body":{"code":11,...,"type":"error"}}
//! ```
//!
//! The first line is an answer for a request this node never accepted. The
//! second is what it cost: with the key already in `completed_replies`, the
//! client's genuine `write` for it was refused *above* the accept — no record,
//! no waiter, no deadline, no answer, and the mutation never reached the state
//! machine.
//!
//! Two gates close it, and they are independent. The record gate makes the path
//! read the ledger instead of writing it, so it can only pay an obligation this
//! node already holds. The sender gate refuses a `client_result` from anyone
//! but a node of this cluster, because only the node a request was relayed to
//! can report what became of it. The last two tests are the controls that keep
//! "closed" from meaning "refuses everything".

use rafter_invariant_test::{oracle_assert, oracle_assert_eq};
use serde_json::{json, Value};

use crate::protocol::Envelope;

use super::{
    client_write, direct_answer, direct_answers, elected_single_node_process, fresh_cluster_member,
    remove_test_root, test_root,
};

/// A `client_result` naming a key this node never accepted is not acted on.
///
/// The sender is this node's own name so that the *record* gate is what the
/// test exercises: a sender the peer gate would refuse cannot tell the two
/// apart, and each gate needs its own case.
#[test]
fn a_client_result_for_a_key_no_record_exists_for_is_not_acted_on() {
    let root = test_root("funnel-no-record");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");

    oracle_assert!(node.owed_answers.is_empty(), "nothing accepted yet");
    oracle_assert!(node.completed_replies.is_empty(), "nothing answered yet");

    node.handle_envelope(stray_result("n1", &json!({ "kind": "write_ok" })));

    oracle_assert_eq!(
        direct_answers(node, "c1", 5),
        0,
        "no answer may go to a client for a request this node never accepted; \
         emitted = {:#?}",
        node.emitted
    );
    oracle_assert!(
        !node.completed_replies.contains(&("c1".to_owned(), 5)),
        "and the at-most-once set may not gain a key no funnel ever saw"
    );
    oracle_assert!(
        node.owed_answers.is_empty(),
        "no record was invented either"
    );
    remove_test_root(root);
}

/// And the consequence that made it matter: the client's own request survives.
///
/// `has_accepted` reads `completed_replies` above the accept, so a key marked
/// answered off the funnel used to swallow the genuine request for it. The
/// write now reaches the state machine and draws its own answer.
#[test]
fn a_stray_result_does_not_swallow_the_clients_real_request() {
    let root = test_root("funnel-swallow");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");

    node.handle_envelope(stray_result(
        "n1",
        &json!({ "kind": "error", "code": 11, "text": "stale" }),
    ));

    // Now the client's actual write for that key reaches this leader.
    node.handle_envelope(client_write("n1", "c1", 5, "counter", 7));
    for _ in 0..8 {
        process.tick();
    }
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");

    oracle_assert_eq!(
        node.app.kv.get("\"counter\""),
        Some(&json!(7)),
        "the client's write reached the state machine"
    );
    oracle_assert_eq!(
        direct_answer(node, "c1", 5)
            .as_ref()
            .and_then(|body| body.get("type").and_then(Value::as_str)),
        Some("write_ok"),
        "and drew its own answer; emitted = {:#?}",
        node.emitted
    );
    remove_test_root(root);
}

/// The sender gate, on a key this node genuinely owes an answer for.
///
/// The record gate passes here — the record exists — so this is the case only
/// the sender gate refuses. Without it a client could answer its own
/// outstanding request early, with a result of its choosing, and retire the
/// record that would otherwise have paid it honestly.
#[test]
fn a_client_result_from_a_client_is_refused_though_the_record_exists() {
    let root = test_root("funnel-sender");
    let mut process = fresh_cluster_member(&root, "n1", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");
    node.accept_answer_obligation("n1", "c1", 5);

    node.handle_envelope(Envelope {
        src: "c1".to_owned(),
        dest: "n1".to_owned(),
        body: result_body(&json!({ "kind": "write_ok" })),
    });

    oracle_assert_eq!(
        direct_answers(node, "c1", 5),
        0,
        "a client may not answer its own request; emitted = {:#?}",
        node.emitted
    );
    oracle_assert!(
        node.owed_answers.is_owed(&("c1".to_owned(), 5)),
        "and the record it would have retired still stands, so the deadline \
         still covers the client"
    );
    remove_test_root(root);
}

/// The control both gates need: the legitimate relay still pays its record.
///
/// A node that accepted a client's request and handed it to the leader holds
/// the record, and the leader's `client_result` is what pays it early. If this
/// stopped working, "closed" would mean "refuses everything" and the two tests
/// above would prove nothing.
#[test]
fn a_client_result_from_a_peer_still_pays_the_record_it_names() {
    let root = test_root("funnel-control");
    let mut process = fresh_cluster_member(&root, "n1", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");
    // The record a client's request lodges on the node it reached directly.
    node.accept_answer_obligation("n1", "c1", 5);

    node.handle_envelope(Envelope {
        src: "n2".to_owned(),
        dest: "n1".to_owned(),
        body: result_body(&json!({ "kind": "write_ok" })),
    });

    oracle_assert_eq!(
        direct_answer(node, "c1", 5)
            .as_ref()
            .and_then(|body| body.get("type").and_then(Value::as_str)),
        Some("write_ok"),
        "the leader's answer reached the client; emitted = {:#?}",
        node.emitted
    );
    oracle_assert!(
        node.owed_answers.is_empty(),
        "and paying it retired the record"
    );
    remove_test_root(root);
}

/// The other list: an operation type the dispatch does not name is answered.
///
/// `handle_envelope` used to match `"read" | "write" | "cas"` and drop anything
/// else, while `parse_client_request` matched the same three below the accept.
/// Nothing checked the two against each other, so a fifth operation added to
/// one and not the other would have been dropped in silence — no record, no
/// answer, and a client waiting on a request this node did read. The dispatch
/// now names no operations at all, which leaves `parse_client_request` the
/// single list and makes an unknown operation an error *answer*.
#[test]
fn an_operation_the_dispatch_does_not_name_is_answered_rather_than_dropped() {
    let root = test_root("funnel-op-list");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");

    node.handle_envelope(Envelope {
        src: "c1".to_owned(),
        dest: "n1".to_owned(),
        body: json!({ "type": "increment", "msg_id": 9, "key": "counter" }),
    });

    let answer = direct_answer(node, "c1", 9).unwrap_or_else(|| {
        panic!(
            "an operation this build does not implement is still a request this \
             node read, and it is answered; emitted = {:#?}",
            node.emitted
        )
    });
    oracle_assert_eq!(
        answer.get("type").and_then(Value::as_str),
        Some("error"),
        "answered as an error rather than acted on: {answer:#?}"
    );
    oracle_assert!(
        node.owed_answers.is_empty(),
        "and the record the funnel lodged for it was retired by that answer"
    );
    remove_test_root(root);
}

/// A `client_result` for `("c1", 5)`, as `src` would have sent it.
fn stray_result(src: &str, result: &Value) -> Envelope {
    Envelope {
        src: src.to_owned(),
        dest: "n1".to_owned(),
        body: result_body(result),
    }
}

fn result_body(result: &Value) -> Value {
    json!({
        "type": "client_result",
        "client": "c1",
        "in_reply_to": 5,
        "result": result,
    })
}
