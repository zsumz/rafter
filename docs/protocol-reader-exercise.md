# Reader exercise

This is a reproducible exercise, not a completed study or a claim of superiority.
Record the exact revisions and relevant feature settings for each implementation.
Use readers comfortable with its implementation language. Record prior Raft and
repository familiarity, alternate implementation order, and use equivalent
feature paths. Keep the answer rubric separate from the question sheet.

Give each reader the production source and its normal documentation. Measure
elapsed time until a correct explanation, definitions inspected, missed
assumptions, and incorrect changes. A preference rating alone does not establish
understanding. Preserve the explanations and patches so correctness can be
reviewed independently of elapsed time.

## Question sheet

1. A node at term 7 receives a term-8 vote request with an older log. Explain the
   resulting term, vote, role, response, and durability obligation.
2. A leader learns that a quorum stores an older-term entry. Explain why its
   commit index may remain unchanged and what later evidence can advance it.
3. Old voters are 1,2,3; new voters are 3,4,5. Explain which acknowledgements
   satisfy the current quorum and where the implementation selects that view.
4. A step returns `Apply` followed by a send. Explain what the core has done,
   what the embedding still owes, and which cursor includes an intervening no-op.
5. A same-term append rejection echoes a fresh heartbeat sequence. Explain its
   effect on authority, replication progress, and pending reads.

Then make one small change: move acknowledgement contact handling under the
success branch, or remove the current-term commit condition. Before running
tests, identify the changed behavior and the existing tests and invariant IDs
that should detect it. Submit the explanation and patch, then revert the
experimental change. Do not merge the intentionally weakened condition.

## Answer rubric

| Question | Required facts | Production route and evidence |
| --- | --- | --- |
| 1 | Advance term, clear prior vote, become follower, reject the older log; persist authority before releasing the response. | [Vote authority](protocol-rules.md#vote-authority), EL-01/03/04 |
| 2 | Replication is a candidate; a current-term committed point commits its prefix. | [Current-term commitment](protocol-rules.md#current-term-commitment), CM-03 |
| 3 | A majority of each side; 1,2,3 alone is insufficient; effective membership selects the rule. | [Joint majorities](protocol-rules.md#joint-majorities), CM-02/MB-02 |
| 4 | State mutated and effects prepared; persistence and application execution remain embedding work; dispatch includes no-ops and configurations. | [Walkthroughs](../crates/rafter/PROTOCOL_WALKTHROUGHS.md), PS-01/RD-04 |
| 5 | Contact may count, read confirmation additionally needs sequence and quorum, rejection does not advance match progress. | [Read sequences](protocol-rules.md#read-sequences), EL-07/RD-02 |

Use the same rubric for other implementations after checking their embedding
contract and feature choices. Report the sample size and unfamiliarity controls.
Do not generalize a small exercise to all implementations or workloads.

## Results sheet

| Reader | Revision | Language / Raft / repository familiarity | Question | Time to correct explanation | Definitions inspected | Missed assumptions | Patch mistakes |
| --- | --- | --- | --- | --- | --- | --- | --- |

No reader measurements have been collected in this change.
