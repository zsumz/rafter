//! Delayed echoes, rejection contact, and term changes in a read barrier.

use super::support::*;

pub(super) fn scenario() -> Scenario {
    let mut steps = campaign(1);
    steps.extend([
        Step::replies("Commit the leader's no-op",
            "Read barriers require an entry committed in this leader's term. The no-op provides that floor without an application command.",
            vec![AppendReply::accepted(NodeId(2), LogIndex(1))]),
        Step::inputs("Two reads register together",
            "Each read needs quorum contact after registration. A batch can register both reads for the same outgoing round.",
            vec![Input::ReadIndex { read_id: ReadId(1) }, Input::ReadIndex { read_id: ReadId(2) }]),
        Step::replies("A delayed echo cannot confirm the new reads",
            "This repeats the actual no-op response, from a round sent before either read registered. Contact alone cannot satisfy a later read round.",
            vec![AppendReply { follower: NodeId(2), through: LogIndex(1), sequence: Some(1), outcome: ReplyOutcome::Accepted }]),
        Step::replies("A fresh rejection confirms contact",
            "This scripted rejection echoes the emitted round 2. It establishes same-term contact without acknowledging a replicated prefix. Only reads covered by that round can be granted.",
            vec![AppendReply { follower: NodeId(2), through: LogIndex(1), sequence: Some(2), outcome: ReplyOutcome::Rejected { term: Term(1) } }]),
        Step::inputs("A later read needs another confirmation",
            "An earlier quorum round cannot be reused by a read registered afterward.",
            vec![Input::ReadIndex { read_id: ReadId(3) }]),
        Step::replies("A higher term cancels pending reads",
            "The scripted peer has advanced to term 2 and rejects the latest emitted append. Learning that newer term ends local leadership and cancels its remaining read barriers.",
            vec![AppendReply { follower: NodeId(2), through: LogIndex(1), sequence: None, outcome: ReplyOutcome::Rejected { term: Term(2) } }]),
    ]);
    Scenario {
        name: "Read barriers and acknowledgement meanings",
        explanation: "Follow [response handling](src/node/replication/response.rs) and [read barriers](src/node/read_index.rs). RD-02 requires sequence-qualified confirmation; see the [delayed-round regression](src/node/tests/read/barrier.rs). Application reads still wait for execution of application entries through the barrier. A no-op has no application callback to wait for. Rejections here are scripted peer outcomes whose sequences come from real outgoing requests.",
        node: Node::new(config(1, &[2, 3])), steps,
        expected: (Term(2), Role::Follower, LogIndex(1), LogIndex(1), 0),
    }
}
