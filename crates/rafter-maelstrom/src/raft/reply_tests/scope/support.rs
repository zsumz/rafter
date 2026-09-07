//! The read envelope the scope scenarios send, and the stall they send it into.
//!
//! One constructor for a client's read, and one fixture that parks this
//! leader's applied state just below the floor the next granted barrier will
//! resolve to. The stall's own assertion guards the fixture only; whether a
//! stranded read is recorded and answered is the scenarios' claim to make.

use rafter::LogIndex;
use rafter_invariant_test::oracle_assert;
use serde_json::json;

use crate::{protocol::Envelope, InitializedNode};

use super::super::client_write;

/// A client's `read` arriving straight at `dest`.
pub(super) fn client_read(dest: &str, client: &str, msg_id: u64, key: &str) -> Envelope {
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
pub(super) fn stall_the_applied_state_below_the_next_grant(node: &mut InitializedNode) -> LogIndex {
    node.handle_envelope(client_write("n1", "c0", 1, "counter", 7));
    let floor = node.app.applied;
    oracle_assert!(
        floor > LogIndex::ZERO,
        "the write must have applied for there to be a floor above zero"
    );
    node.app.applied = LogIndex(floor.0 - 1);
    floor
}
