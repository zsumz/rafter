//! What the obligation ledger is total over, and what falls outside it.
//!
//! The sibling `obligation` module pins that a record is paid — that whatever
//! is in the ledger is answered by its deadline. These pin the other half, the
//! one four rounds of this reply path asserted rather than built: that every
//! client request this node acts on is *in* the ledger, reads included.
//!
//! The mechanism is one funnel with one accept above it, and a token type only
//! the ledger can mint that every acting path requires. Its scope is "every
//! client request this node acts on", in the direction the sweep needs — acted
//! on implies recorded. The second half of this file is the boundary: what the
//! funnel does not reach, tested separately rather than folded into a claim one
//! step wider than the mechanism. That folding is the defect itself.

use rafter::{LogIndex, NodeId};
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};
use serde_json::{json, Value};

use crate::{protocol::Envelope, InitializedNode};

use super::{
    client_forwards, client_write, direct_answer, direct_answers, elected_cluster_leader,
    elected_single_node_process, fresh_cluster_member, remove_test_root, test_root,
};

// ---------------------------------------------------------------------------
// A read enters the ledger like every other request.
// ---------------------------------------------------------------------------

/// A read the leader serves is recorded exactly like one it forwards.
///
/// The fourth-generation defect, and the one that named the pattern behind all
/// four: `start_read` created a `PendingRead` and never called
/// `accept_answer_obligation`, so the ledger was total over its own contents
/// and not over the requests this node acted on. Whether the deadline covered a
/// client request depended on which node the client happened to reach — a
/// follower recorded it before relaying, the leader recorded nothing at all.
///
/// Two legs of the identical request, so the asymmetry is the subject rather
/// than the setup: the forwarding leg is the construction working, and the
/// serving leg is what it did not reach.
#[test]
fn a_read_the_leader_serves_is_recorded_like_one_it_forwards() {
    // Leg one: a follower forwards the read, and records the obligation.
    let follower_root = test_root("obligation-read-forwarded");
    let mut follower_process = fresh_cluster_member(&follower_root, "n2", &["n1", "n2", "n3"]);
    let follower = follower_process
        .initialized
        .as_mut()
        .expect("n2 initializes");
    follower.known_leader = Some(NodeId(1));
    follower.handle_envelope(client_read("n2", "c1", 5, "counter"));
    oracle_assert!(
        follower.owed_answers.is_owed(&("c1".to_owned(), 5)),
        "forwarding a read records the obligation, which is the construction \
         working"
    );
    oracle_assert_eq!(client_forwards(follower), 1, "and the read went to n1");
    remove_test_root(follower_root);

    // Leg two: a leader serves the very same request out of its own state, and
    // stalls below the floor the barrier granted. Only the ledger is left.
    let leader_root = test_root("obligation-read-served");
    let mut process = elected_single_node_process(&leader_root);
    let node = process.initialized.as_mut().expect("n1 initializes");
    stall_the_applied_state_below_the_next_grant(node);

    node.handle_envelope(client_read("n1", "c1", 5, "counter"));

    oracle_assert!(
        node.pending_reads.len() == 1,
        "the read is parked below its floor, so no fast path can pay it"
    );
    oracle_assert_eq!(
        node.owed_answers.answer_to(&("c1".to_owned(), 5)),
        Some(node.name.as_str()),
        "a read this node accepted from a client must leave the same record a \
         forwarded one does — `start_read` reaching the ledger is the whole of \
         `every accepted request is answered` for a read served here"
    );
    remove_test_root(leader_root);
}

/// A granted read whose floor the state machine never reaches is answered at
/// its deadline.
///
/// The general statement for reads, and the case the old text could not cover:
/// its argument ran through `ReadIndexRejected` and `ReadIndexCanceled`
/// arriving "exactly when leadership is absent or lost", which is the converse
/// of what was proved. A barrier that granted and a floor the applied state
/// never reaches emits neither output, holds a waiter forever, and is invisible
/// to everything except the ledger.
#[test]
fn a_granted_read_whose_floor_is_never_reached_is_answered_at_its_deadline() {
    let root = test_root("obligation-read-deadline-sweep");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");
    stall_the_applied_state_below_the_next_grant(node);

    node.handle_envelope(client_read("n1", "c1", 5, "counter"));
    oracle_assert_eq!(
        direct_answers(node, "c1", 5),
        0,
        "nothing is owed yet — the read may still reach its floor"
    );

    let sweeps = node.answer_deadline_ticks + 2;
    for _ in 0..sweeps {
        process.tick();
    }
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");

    let answer = direct_answer(node, "c1", 5).expect("the deadline answers the client");
    oracle_assert_eq!(
        answer.get("code").and_then(Value::as_u64),
        Some(0),
        "and answers with Maelstrom's indefinite `timeout`, because a read the \
         sweep pays is one this node cannot describe; answer = {answer:#?}"
    );
    oracle_assert!(
        node.owed_answers.is_empty(),
        "the swept record is retired by the answer, not merely dropped"
    );
    oracle_assert!(
        node.pending_reads.is_empty(),
        "and the waiter goes with it, so a swept read cannot accumulate — \
         `pending_reads` is bounded by the barriers still unresolved, not by \
         every read this process ever gave up on"
    );
    remove_test_root(root);
}

/// A read the deadline answered is not answered again when its floor arrives.
///
/// The double-answer the new record could have introduced. The sweep and the
/// flush are two paths to the same client, and `completed_replies` is what
/// makes the second one a suppressed duplicate — the property was checked for
/// writes and had never been driven for a read that the sweep reached first.
#[test]
fn a_read_the_deadline_answered_is_not_answered_again_when_its_floor_arrives() {
    let root = test_root("obligation-read-no-double-answer");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");
    let stalled_at = stall_the_applied_state_below_the_next_grant(node);

    node.handle_envelope(client_read("n1", "c1", 5, "counter"));
    let sweeps = node.answer_deadline_ticks + 2;
    for _ in 0..sweeps {
        process.tick();
    }
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");
    oracle_assert_eq!(
        direct_answers(node, "c1", 5),
        1,
        "the deadline answered it once"
    );

    // The apply the read was waiting for finally lands, and every flush hook
    // there is runs over it.
    node.app.applied = stalled_at;
    node.flush_reads();
    process.tick();
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");
    node.flush_reads();

    oracle_assert_eq!(
        direct_answers(node, "c1", 5),
        1,
        "a request the sweep already answered must not be answered a second \
         time when its slow path completes; emitted = {:#?}",
        node.emitted
    );
    remove_test_root(root);
}

// ---------------------------------------------------------------------------
// What the funnel does not cover.
//
// The mechanism's scope is "every client request this node acts on", and these
// are the things that are outside it. They are tested separately and named
// separately because a scope claimed one step wider than the mechanism reaches
// is the defect this reply path has now grown four times.
// ---------------------------------------------------------------------------

/// An envelope that never names a request leaves no obligation, and no answer.
///
/// Outside the funnel by construction: `handle_client` needs a `msg_id` and
/// `handle_forward` needs a `client`, an `in_reply_to` and a `request`, and
/// without them there is no `(client, in_reply_to)` for a record to be keyed by
/// or for an answer to be addressed to. Recording something here would be
/// recording an obligation to nobody, which the sweep would then mail into the
/// void; dropping it is the only honest outcome, and this is what pins that the
/// boundary is the one described rather than one step further in.
///
/// The forwards come from a peer of a real cluster. Sent from a name the
/// membership does not hold they would be refused by the dispatch instead, and
/// this test would pass while checking nothing about the field tests it names —
/// the vacuous form of exactly the boundary it exists to pin.
#[test]
fn an_envelope_that_never_names_a_request_leaves_no_obligation() {
    let root = test_root("obligation-unnamed-envelope");
    let (mut process, _peers) = elected_cluster_leader(&root);
    let node = process.initialized.as_mut().expect("node initializes");
    let emitted_before = node.emitted.len();

    for body in [
        json!({ "type": "read", "key": "counter" }),
        json!({ "type": "write", "key": "counter", "value": 7 }),
        json!({ "type": "cas", "key": "counter", "from": 1, "to": 2 }),
    ] {
        node.handle_envelope(Envelope {
            src: "c1".to_owned(),
            dest: "n1".to_owned(),
            body,
        });
    }
    for body in [
        json!({ "type": "client_forward", "in_reply_to": 5, "request": {} }),
        json!({ "type": "client_forward", "client": "c1", "request": {} }),
        json!({ "type": "client_forward", "client": "c1", "in_reply_to": 5 }),
    ] {
        node.handle_envelope(Envelope {
            src: "n2".to_owned(),
            dest: "n1".to_owned(),
            body,
        });
    }

    oracle_assert!(
        node.owed_answers.is_empty(),
        "an envelope with no request key in it must not lodge a record no \
         answer could ever be addressed to"
    );
    oracle_assert_eq!(
        node.emitted.len(),
        emitted_before,
        "and nothing goes on the wire for it; emitted = {:#?}",
        node.emitted
    );
    remove_test_root(root);
}

/// A second copy of a read lodges no second record, and opens no second
/// barrier.
///
/// Outside the funnel because `has_accepted` returns above the accept: the
/// first copy's record is what covers this client, and its deadline stands.
/// This is the other half of the `start_read` defect — with a read in neither
/// the ledger nor the dedupe set while its barrier was outstanding, a duplicate
/// delivery was accepted a second time, parking a second waiter and opening a
/// second barrier for a request the client issued once.
#[test]
fn a_second_copy_of_a_read_lodges_no_second_record() {
    let root = test_root("obligation-duplicate-read");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");
    stall_the_applied_state_below_the_next_grant(node);

    for _ in 0..3 {
        node.handle_envelope(client_read("n1", "c1", 5, "counter"));
    }

    oracle_assert_eq!(
        node.pending_reads.len(),
        1,
        "one request opens one barrier however many copies of it arrive"
    );
    oracle_assert!(
        node.owed_answers.is_owed(&("c1".to_owned(), 5)),
        "and the record the first copy lodged is what covers the client"
    );

    // The deadline the first copy set is the one that governs: a repeat must
    // not be able to push it out and hold the client indefinitely.
    let sweeps = node.answer_deadline_ticks + 2;
    for _ in 0..sweeps {
        process.tick();
    }
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");
    oracle_assert_eq!(
        direct_answers(node, "c1", 5),
        1,
        "the first copy's deadline answers the client exactly once"
    );
    remove_test_root(root);
}

// ---------------------------------------------------------------------------
// What bounds the one collection that is never pruned.
// ---------------------------------------------------------------------------

/// The dedupe set grows with distinct answered requests and with nothing else.
///
/// `completed_replies` is the one collection here that is never pruned, and the
/// decision to leave it that way rests on a claim about *what* makes it grow
/// rather than on a claim about how big it gets. Every pruning rule would be an
/// assumption about how late a duplicate can arrive, and forgetting a request
/// one tick early re-applies its mutation — so the honest alternative is to
/// state the growth law and check it. This is the check: one entry per distinct
/// `(client, msg_id)` answered, none for repeats, and none for the passage of
/// time.
#[test]
fn the_dedupe_set_grows_with_distinct_answered_requests_and_nothing_else() {
    let root = test_root("obligation-dedupe-growth");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");
    oracle_assert!(node.completed_replies.is_empty(), "nothing answered yet");

    // Distinct requests: one entry each.
    for msg_id in 1..=5 {
        node.handle_envelope(client_write("n1", "c1", msg_id, "counter", msg_id));
    }
    oracle_assert_eq!(
        node.completed_replies.len(),
        5,
        "five distinct requests answered leave five entries"
    );

    // Repeats of one already-answered request: no growth, and no second answer.
    for _ in 0..4 {
        node.handle_envelope(client_write("n1", "c1", 3, "counter", 3));
    }
    oracle_assert_eq!(
        node.completed_replies.len(),
        5,
        "a duplicate delivery is refused above the accept, so it adds nothing"
    );
    oracle_assert_eq!(
        direct_answers(node, "c1", 3),
        1,
        "and is answered no second time"
    );

    // Time alone: no growth. The sweep adds an entry for a request it answers,
    // never one per tick.
    let ticks = node.answer_deadline_ticks + 8;
    for _ in 0..ticks {
        process.tick();
    }
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");
    oracle_assert_eq!(
        node.completed_replies.len(),
        5,
        "{ticks} ticks with no client traffic add nothing: the set is bounded by \
         the requests this process answered, not by how long it ran"
    );

    // And the other side of leaving it unpruned: a duplicate that arrives long
    // after its answer went out is still refused. Any pruning rule is a bound on
    // how late the network may be, and this is what it would buy the memory
    // with — c1's third write re-applying over the two that linearized after it.
    node.handle_envelope(client_write("n1", "c1", 3, "counter", 3));

    oracle_assert_eq!(
        node.app.kv.get("\"counter\""),
        Some(&json!(5)),
        "a duplicate delivered {ticks} ticks after its answer must not re-apply \
         its mutation over the writes that linearized after it"
    );
    oracle_assert_eq!(
        direct_answers(node, "c1", 3),
        1,
        "and must not draw a second answer"
    );
    remove_test_root(root);
}
// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// A client's `read` arriving straight at `dest`.
fn client_read(dest: &str, client: &str, msg_id: u64, key: &str) -> Envelope {
    Envelope {
        src: client.to_owned(),
        dest: dest.to_owned(),
        body: json!({ "type": "read", "msg_id": msg_id, "key": key }),
    }
}

/// Leaves this leader's applied state one application entry below the floor the
/// next granted barrier will resolve to, and reports that floor.
///
/// A read issued afterwards grants, parks below its floor, and stays there:
/// nothing else will apply, so no flush hook can pay it and no error output is
/// coming either. That is the stranded read in the small — a barrier that
/// neither resolves nor fails — and the only thing left holding it is the
/// ledger record.
///
/// Rolling the cursor back is how `read_tests` builds the same stall; the
/// production shape it stands for is a floor the state machine has not reached
/// and an apply that never arrives to move it.
fn stall_the_applied_state_below_the_next_grant(node: &mut InitializedNode) -> LogIndex {
    node.handle_envelope(client_write("n1", "c0", 1, "counter", 7));
    let floor = node.app.applied;
    oracle_assert!(
        floor > LogIndex::ZERO,
        "the write must have applied for there to be a floor above zero"
    );
    node.app.applied = LogIndex(floor.0 - 1);
    floor
}
