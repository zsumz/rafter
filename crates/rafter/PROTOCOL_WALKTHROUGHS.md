# Production-core walkthroughs

These stories run the actual `Node::step` and `Node::step_batch` with default
pre-vote and check-quorum enabled and lease reads disabled. Each starts from the
state shown below. Read the single-input story once, then its observed batch differences.

**Raw-core boundary:** returning an output neither persists Raft state nor executes
application code. The embedding persists dependent state before releasing sends,
read grants, or application effects in their returned order. Dispatch includes
no-ops, configurations, and the recovery floor; it does not establish execution
or durable application progress.

This is a focused node replay. Ordinary append successes echo the term, sequence,
and confirmed end of an actual emitted request. A separate real-follower test
checks the write exchange. Synthetic guard probes and scripted peer outcomes are
labeled. The human view selects state changes and combines only consecutive
identical sends, retaining recipient order. The [full regression artifact](
src/node/tests/walkthroughs/observations.txt) separately records every resolved
input, ordered output, and internal state field after every input or batch.

## Election and committed write

Follow [election](src/node/election.rs), [acknowledgements](src/node/replication/response.rs), and [commit authorization](src/node/commit/advance.rs). CM-03 separates a replicated threshold from permission to commit it. The [prior-term commit detector](../rafter-sim/src/model_check/invariants/tests/commit_history.rs) provides the counterexample evidence. The two injected guard probes below are labeled; the ordinary successes are correlated with emitted requests and also checked against a real follower.

Membership: voters 1, 2, 3.

### Election timeout starts pre-vote

**Before:** Node 1 is a follower in term 1, voted for none. Commit 0, dispatch 0, last index 1 / term 1; 0 pending reads.

**Input**

1. Advance 10 ticks.

**Changes**

1. Role: follower → pre-candidate.

**Ordered outputs**

1. Send to nodes 2, 3 (in that order): pre-vote request for prospective term 2; candidate 1; last entry 1 / term 1.

**Why:** Pre-voting asks whether an election could succeed without changing the durable term or vote.

### Pre-vote quorum starts a binding election

**Before:** Node 1 is a pre-candidate in term 1, voted for none. Commit 0, dispatch 0, last index 1 / term 1; 0 pending reads.

**Input**

1. From node 2: pre-vote granted for prospective term 2.

**Changes**

1. Term: 1 → 2.
2. Role: pre-candidate → candidate.
3. Voted for: none → node 1.

**Ordered outputs**

1. Send to nodes 2, 3 (in that order): vote request for term 2; candidate 1; last entry 1 / term 1.

**Why:** A pre-vote quorum permits a real election: advance the term, vote for self, and request binding votes.

### Binding quorum elects a leader and appends its no-op

**Before:** Node 1 is a candidate in term 2, voted for node 1. Commit 0, dispatch 0, last index 1 / term 1; 0 pending reads.

**Input**

1. From node 2: vote granted; term 2.

**Changes**

1. Role: candidate → leader.
2. Last log index: 1 → 2.
3. Last log term: 1 → 2.
4. Acknowledged follower prefixes: none → node 2: 0; node 3: 0.

**Ordered outputs**

1. Send to nodes 2, 3 (in that order): append #1; term 2; previous 1 / term 1; entry 2 (term 2); leader commit 0.

**Why:** The binding quorum establishes leadership. The new-term no-op provides an entry that can authorize commitment of its prefix.

### Synthetic guard probe: an older-term quorum threshold

**Before:** Node 1 is a leader in term 2, voted for node 1. Commit 0, dispatch 0, last index 2 / term 2; 0 pending reads.

**Input**

1. From node 2: append #1 accepted; term 2; matching prefix through 1.

**Changes**

1. Acknowledged follower prefixes: node 2: 0; node 3: 0 → node 2: 1; node 3: 0.

**Ordered outputs**

1. Send to node 2: append #1; term 2; previous 1 / term 1; entry 2 (term 2); leader commit 0.

**Why:** This injected partial success is not a follower response to the emitted no-op frame, which ends at index 2. It isolates CM-03: even quorum replication of index 1 cannot directly commit that older-term entry.

### Current-term no-op commits the preceding write

**Before:** Node 1 is a leader in term 2, voted for node 1. Commit 0, dispatch 0, last index 2 / term 2; 0 pending reads.

**Input**

1. From node 2: append #1 accepted; term 2; matching prefix through 2.

**Changes**

1. Commit index: 0 → 2.
2. Dispatch index: 0 → 2.
3. Acknowledged follower prefixes: node 2: 1; node 3: 0 → node 2: 2; node 3: 0.

**Ordered outputs**

1. Dispatch command “older write” at index 1.

**Why:** This success echoes the actual no-op append. Its current-term entry authorizes commitment of the preceding write. The no-op advances dispatch without an application-command output.

### Two tracked writes enter the log

**Before:** Node 1 is a leader in term 2, voted for node 1. Commit 2, dispatch 2, last index 2 / term 2; 0 pending reads.

**Input**

1. Propose “first” with local id 41.
2. Propose “second” with local id 42.

**Changes**

1. Last log index: 2 → 4.

**Ordered outputs**

1. Assign proposal 41 to index 3 / term 2.
2. Send to node 2: append #2; term 2; previous 2 / term 2; entry 3 (term 2); leader commit 2.
3. Send to node 3: append #2; term 2; previous 1 / term 1; no entries; leader commit 2.
4. Assign proposal 42 to index 4 / term 2.
5. Send to node 2: append #3; term 2; previous 3 / term 2; entry 4 (term 2); leader commit 2.
6. Send to node 3: append #3; term 2; previous 1 / term 1; no entries; leader commit 2.

**Why:** Appending a local proposal only assigns its index. A later quorum acknowledgement permits commitment and command dispatch.

### New matching prefix advances commitment

**Before:** Node 1 is a leader in term 2, voted for node 1. Commit 2, dispatch 2, last index 4 / term 2; 0 pending reads.

**Input**

1. From node 2: append #3 accepted; term 2; matching prefix through 4.

**Changes**

1. Commit index: 2 → 4.
2. Dispatch index: 2 → 4.
3. Acknowledged follower prefixes: node 2: 2; node 3: 0 → node 2: 4; node 3: 0.

**Ordered outputs**

1. Dispatch command “first” at index 3.
2. Dispatch command “second” at index 4.

**Why:** The reply echoes the emitted frame that actually ends at index 4. Its sequence is selected from the traffic, so single steps and batching may echo different rounds.

### Injected stale response cannot regress progress

**Before:** Node 1 is a leader in term 2, voted for node 1. Commit 4, dispatch 4, last index 4 / term 2; 0 pending reads.

**Input**

1. From node 2: append #1 accepted; term 1; matching prefix through 1.

**Changes:** the displayed state is unchanged.

**Ordered outputs**

None.

**Why:** This old-term response represents delayed traffic from before the recorded election. Term validation discards it before replication progress can change.

### Batched inputs: what changes

Each checkpoint becomes one `step_batch` call; the primary story calls `step`
once per input. The full artifact keeps both sets of boundaries.

**Two tracked writes enter the log**

**Single-call ordered outputs**

1. Assign proposal 41 to index 3 / term 2.
2. Send to node 2: append #2; term 2; previous 2 / term 2; entry 3 (term 2); leader commit 2.
3. Send to node 3: append #2; term 2; previous 1 / term 1; no entries; leader commit 2.
4. Assign proposal 42 to index 4 / term 2.
5. Send to node 2: append #3; term 2; previous 3 / term 2; entry 4 (term 2); leader commit 2.
6. Send to node 3: append #3; term 2; previous 1 / term 1; no entries; leader commit 2.

**Batch ordered outputs**

1. Assign proposal 41 to index 3 / term 2.
2. Assign proposal 42 to index 4 / term 2.
3. Send to node 2: append #2; term 2; previous 2 / term 2; entries 3–4 (term 2); leader commit 2.
4. Send to node 3: append #2; term 2; previous 1 / term 1; no entries; leader commit 2.

**New matching prefix advances commitment**

**Single-input exchange**

1. From node 2: append #3 accepted; term 2; matching prefix through 4.

**Batched exchange**

1. From node 2: append #2 accepted; term 2; matching prefix through 4.

## A rejected vote still changes authority

Follow [handle_request_vote](src/node/election.rs). Term adoption precedes vote eligibility (EL-01, EL-03). The [durable vote rejection case](../rafter-runtime/src/tests/hard_state/voting.rs) checks the same order through persistence.

Membership: voters 1, 2, 3.

### A newer term, but an older log

**Before:** Node 1 is a follower in term 7, voted for node 3. Commit 0, dispatch 0, last index 1 / term 7; 0 pending reads.

**Input**

1. From node 2: vote request for term 8; candidate 2; last entry 0 / term 0.

**Changes**

1. Term: 7 → 8.
2. Voted for: node 3 → none.

**Ordered outputs**

1. Send to node 2: vote rejected; term 8.

**Why:** Learning term 8 happens before comparing logs. The rejection must carry term 8, and the term-7 vote must be cleared even though no new vote is granted.

### An older term cannot regain authority

**Before:** Node 1 is a follower in term 8, voted for none. Commit 0, dispatch 0, last index 1 / term 7; 0 pending reads.

**Input**

1. From node 3: vote request for term 7; candidate 3; last entry 1 / term 7.

**Changes:** the displayed state is unchanged.

**Ordered outputs**

1. Send to node 3: vote rejected; term 8.

**Why:** The node remembers term 8 from the rejected request. Moving term adoption after log eligibility would incorrectly leave term-7 authority in place.

### Batched inputs: what changes

Each checkpoint becomes one `step_batch` call; the primary story calls `step`
once per input. The full artifact keeps both sets of boundaries.

The displayed checkpoint states and ordered outputs match in both modes.

## Read barriers and acknowledgement meanings

Follow [response handling](src/node/replication/response.rs) and [read barriers](src/node/read_index.rs). RD-02 requires sequence-qualified confirmation; see the [delayed-round regression](src/node/tests/read/barrier.rs). Application reads still wait for execution of application entries through the barrier. A no-op has no application callback to wait for. Rejections here are scripted peer outcomes whose sequences come from real outgoing requests.

Membership: voters 1, 2, 3.

### Election timeout starts pre-vote

**Before:** Node 1 is a follower in term 0, voted for none. Commit 0, dispatch 0, last index 0 / term 0; 0 pending reads.

**Input**

1. Advance 10 ticks.

**Changes**

1. Role: follower → pre-candidate.

**Ordered outputs**

1. Send to nodes 2, 3 (in that order): pre-vote request for prospective term 1; candidate 1; last entry 0 / term 0.

**Why:** Pre-voting asks whether an election could succeed without changing the durable term or vote.

### Pre-vote quorum starts a binding election

**Before:** Node 1 is a pre-candidate in term 0, voted for none. Commit 0, dispatch 0, last index 0 / term 0; 0 pending reads.

**Input**

1. From node 2: pre-vote granted for prospective term 1.

**Changes**

1. Term: 0 → 1.
2. Role: pre-candidate → candidate.
3. Voted for: none → node 1.

**Ordered outputs**

1. Send to nodes 2, 3 (in that order): vote request for term 1; candidate 1; last entry 0 / term 0.

**Why:** A pre-vote quorum permits a real election: advance the term, vote for self, and request binding votes.

### Binding quorum elects a leader and appends its no-op

**Before:** Node 1 is a candidate in term 1, voted for node 1. Commit 0, dispatch 0, last index 0 / term 0; 0 pending reads.

**Input**

1. From node 2: vote granted; term 1.

**Changes**

1. Role: candidate → leader.
2. Last log index: 0 → 1.
3. Last log term: 0 → 1.
4. Acknowledged follower prefixes: none → node 2: 0; node 3: 0.

**Ordered outputs**

1. Send to nodes 2, 3 (in that order): append #1; term 1; previous 0 / term 0; entry 1 (term 1); leader commit 0.

**Why:** The binding quorum establishes leadership. The new-term no-op provides an entry that can authorize commitment of its prefix.

### Commit the leader's no-op

**Before:** Node 1 is a leader in term 1, voted for node 1. Commit 0, dispatch 0, last index 1 / term 1; 0 pending reads.

**Input**

1. From node 2: append #1 accepted; term 1; matching prefix through 1.

**Changes**

1. Commit index: 0 → 1.
2. Dispatch index: 0 → 1.
3. Acknowledged follower prefixes: node 2: 0; node 3: 0 → node 2: 1; node 3: 0.

**Ordered outputs**

None.

**Why:** Read barriers require an entry committed in this leader's term. The no-op provides that floor without an application command.

### Two reads register together

**Before:** Node 1 is a leader in term 1, voted for node 1. Commit 1, dispatch 1, last index 1 / term 1; 0 pending reads.

**Input**

1. Register read 1.
2. Register read 2.

**Changes**

1. Pending reads: 0 → 2.

**Ordered outputs**

1. Send to node 2: append #2; term 1; previous 1 / term 1; no entries; leader commit 1.
2. Send to node 3: append #2; term 1; previous 0 / term 0; no entries; leader commit 1.
3. Send to node 2: append #3; term 1; previous 1 / term 1; no entries; leader commit 1.
4. Send to node 3: append #3; term 1; previous 0 / term 0; no entries; leader commit 1.

**Why:** Each read needs quorum contact after registration. A batch can register both reads for the same outgoing round.

### A delayed echo cannot confirm the new reads

**Before:** Node 1 is a leader in term 1, voted for node 1. Commit 1, dispatch 1, last index 1 / term 1; 2 pending reads.

**Input**

1. From node 2: append #1 accepted; term 1; matching prefix through 1.

**Changes:** the displayed state is unchanged.

**Ordered outputs**

None.

**Why:** This repeats the actual no-op response, from a round sent before either read registered. Contact alone cannot satisfy a later read round.

### A fresh rejection confirms contact

**Before:** Node 1 is a leader in term 1, voted for node 1. Commit 1, dispatch 1, last index 1 / term 1; 2 pending reads.

**Input**

1. From node 2: append #2 rejected; term 1; no matching-prefix acknowledgement.

**Changes**

1. Pending reads: 2 → 1.

**Ordered outputs**

1. Grant read 1 at barrier 1.
2. Send to node 2: append #3; term 1; previous 1 / term 1; no entries; leader commit 1.

**Why:** This scripted rejection echoes the emitted round 2. It establishes same-term contact without acknowledging a replicated prefix. Only reads covered by that round can be granted.

### A later read needs another confirmation

**Before:** Node 1 is a leader in term 1, voted for node 1. Commit 1, dispatch 1, last index 1 / term 1; 1 pending reads.

**Input**

1. Register read 3.

**Changes**

1. Pending reads: 1 → 2.

**Ordered outputs**

1. Send to node 2: append #4; term 1; previous 1 / term 1; no entries; leader commit 1.
2. Send to node 3: append #4; term 1; previous 0 / term 0; no entries; leader commit 1.

**Why:** An earlier quorum round cannot be reused by a read registered afterward.

### A higher term cancels pending reads

**Before:** Node 1 is a leader in term 1, voted for node 1. Commit 1, dispatch 1, last index 1 / term 1; 2 pending reads.

**Input**

1. From node 2: append #4 rejected; term 2; no matching-prefix acknowledgement.

**Changes**

1. Term: 1 → 2.
2. Role: leader → follower.
3. Voted for: node 1 → none.
4. Pending reads: 2 → 0.
5. Acknowledged follower prefixes: node 2: 1; node 3: 0 → none.

**Ordered outputs**

1. Cancel read 2: leadership ended.
2. Cancel read 3: leadership ended.

**Why:** The scripted peer has advanced to term 2 and rejects the latest emitted append. Learning that newer term ends local leadership and cancels its remaining read barriers.

### Batched inputs: what changes

Each checkpoint becomes one `step_batch` call; the primary story calls `step`
once per input. The full artifact keeps both sets of boundaries.

**Two reads register together**

**Single-call ordered outputs**

1. Send to node 2: append #2; term 1; previous 1 / term 1; no entries; leader commit 1.
2. Send to node 3: append #2; term 1; previous 0 / term 0; no entries; leader commit 1.
3. Send to node 2: append #3; term 1; previous 1 / term 1; no entries; leader commit 1.
4. Send to node 3: append #3; term 1; previous 0 / term 0; no entries; leader commit 1.

**Batch ordered outputs**

1. Send to node 2: append #2; term 1; previous 1 / term 1; no entries; leader commit 1.
2. Send to node 3: append #2; term 1; previous 0 / term 0; no entries; leader commit 1.

**A fresh rejection confirms contact**

**Different end state**

1. Pending reads at this checkpoint: 1 after single calls; 0 after batching.

**Single-call ordered outputs**

1. Grant read 1 at barrier 1.
2. Send to node 2: append #3; term 1; previous 1 / term 1; no entries; leader commit 1.

**Batch ordered outputs**

1. Grant read 1 at barrier 1.
2. Grant read 2 at barrier 1.
3. Send to node 2: append #2; term 1; previous 1 / term 1; no entries; leader commit 1.

**A later read needs another confirmation**

**Different end state**

1. Pending reads at this checkpoint: 2 after single calls; 1 after batching.

**Single-call ordered outputs**

1. Send to node 2: append #4; term 1; previous 1 / term 1; no entries; leader commit 1.
2. Send to node 3: append #4; term 1; previous 0 / term 0; no entries; leader commit 1.

**Batch ordered outputs**

1. Send to node 2: append #3; term 1; previous 1 / term 1; no entries; leader commit 1.
2. Send to node 3: append #3; term 1; previous 0 / term 0; no entries; leader commit 1.

**A higher term cancels pending reads**

**Single-input exchange**

1. From node 2: append #4 rejected; term 2; no matching-prefix acknowledgement.

**Batched exchange**

1. From node 2: append #3 rejected; term 2; no matching-prefix acknowledgement.

**Single-call ordered outputs**

1. Cancel read 2: leadership ended.
2. Cancel read 3: leadership ended.

**Batch ordered outputs**

1. Cancel read 3: leadership ended.

## The frame confirms a prefix, not the whole local suffix

Follow [follower append reception](src/node/replication/receive.rs). Five matching entries above 100 confirm only 105, even with leader_commit=110 and local last index=120. Using the local last index would dispatch unrelated suffix entries (CM-01, LG-04). A conflict at an already committed entry is rejected; a later conflict replaces only the uncommitted suffix. See [empty_append_never_commits_the_followers_unconfirmed_suffix and committed-conflict rejection](src/node/tests/replication/follower.rs).

Membership: voters 1, 2, 3.

### A matching frame confirms only through 105

**Before:** Node 1 is a follower in term 1, voted for none. Commit 100, dispatch 100, last index 120 / term 1; 0 pending reads.

**Input**

1. From node 2: append #1; term 3; previous 100 / term 1; entries 101–105 (term 1); leader commit 110.

**Changes**

1. Term: 1 → 3.
2. Commit index: 100 → 105.
3. Dispatch index: 100 → 105.

**Ordered outputs**

1. Dispatch command “record 101” at index 101.
2. Dispatch command “record 102” at index 102.
3. Dispatch command “record 103” at index 103.
4. Dispatch command “record 104” at index 104.
5. Dispatch command “record 105” at index 105.
6. Send to node 2: append #1 accepted; term 3; matching prefix through 105.

**Why:** The frame confirms 105, while the leader knows commitment through 110. Only the confirmed prefix may be committed here; the unrelated local suffix through 120 proves nothing further.

### A committed conflict is rejected

**Before:** Node 1 is a follower in term 3, voted for none. Commit 105, dispatch 105, last index 120 / term 1; 0 pending reads.

**Input**

1. From node 2: append #1; term 3; previous 104 / term 1; entry 105 (term 3); leader commit 110.

**Changes:** the displayed state is unchanged.

**Ordered outputs**

1. Send to node 2: append #1 rejected; term 3; no matching-prefix acknowledgement.

**Why:** Index 105 is already committed. A different entry there must be rejected without changing that committed history.

### An uncommitted conflict replaces the tail

**Before:** Node 1 is a follower in term 3, voted for none. Commit 105, dispatch 105, last index 120 / term 1; 0 pending reads.

**Input**

1. From node 2: append #1; term 3; previous 105 / term 1; entry 106 (term 3); leader commit 106.

**Changes**

1. Commit index: 105 → 106.
2. Dispatch index: 105 → 106.
3. Last log index: 120 → 106.
4. Last log term: 1 → 3.

**Ordered outputs**

1. Dispatch command “replacement” at index 106.
2. Send to node 2: append #1 accepted; term 3; matching prefix through 106.

**Why:** The conflict now starts at uncommitted index 106. Replace that suffix, then advance commitment only through the new frame's confirmed end.

### Batched inputs: what changes

Each checkpoint becomes one `step_batch` call; the primary story calls `step`
once per input. The full artifact keeps both sets of boundaries.

The displayed checkpoint states and ordered outputs match in both modes.

## Joint membership requires both majorities

Follow [election quorum](src/node/election.rs) and [commit quorum](src/node/commit/advance.rs). The old voters are 1,2,3; the new voters are 3,4,5. Grants or acknowledgements from 1,2,3 give an old majority and a majority of the union, but only one new vote. Node 4 supplies the missing new majority (MB-02). The [recovered joint-configuration tests](src/node/tests/bootstrap/configuration_progress.rs) check both election and commitment with the same rule.

Membership: joint membership: old voters 1, 2, 3; new voters 3, 4, 5.

### Election timeout starts pre-vote

**Before:** Node 1 is a follower in term 2, voted for none. Commit 0, dispatch 0, last index 1 / term 2; 0 pending reads.

**Input**

1. Advance 10 ticks.

**Changes**

1. Role: follower → pre-candidate.

**Ordered outputs**

1. Send to nodes 2, 3, 4, 5 (in that order): pre-vote request for prospective term 3; candidate 1; last entry 1 / term 2.

**Why:** The pending joint configuration determines both constituent election quorums.

### Old majority alone cannot finish pre-vote

**Before:** Node 1 is a pre-candidate in term 2, voted for none. Commit 0, dispatch 0, last index 1 / term 2; 0 pending reads.

**Input**

1. From node 2: pre-vote granted for prospective term 3.
2. From node 3: pre-vote granted for prospective term 3.

**Changes:** the displayed state is unchanged.

**Ordered outputs**

None.

**Why:** Nodes 1, 2, 3 include all old voters but only new voter 3. A majority of the combined node set would incorrectly permit progress.

### New majority completes pre-vote

**Before:** Node 1 is a pre-candidate in term 2, voted for none. Commit 0, dispatch 0, last index 1 / term 2; 0 pending reads.

**Input**

1. From node 4: pre-vote granted for prospective term 3.

**Changes**

1. Term: 2 → 3.
2. Role: pre-candidate → candidate.
3. Voted for: none → node 1.

**Ordered outputs**

1. Send to nodes 2, 3, 4, 5 (in that order): vote request for term 3; candidate 1; last entry 1 / term 2.

**Why:** Node 4 joins node 3 to satisfy the new majority as well as the old one.

### Old majority alone cannot elect

**Before:** Node 1 is a candidate in term 3, voted for node 1. Commit 0, dispatch 0, last index 1 / term 2; 0 pending reads.

**Input**

1. From node 2: vote granted; term 3.
2. From node 3: vote granted; term 3.

**Changes:** the displayed state is unchanged.

**Ordered outputs**

None.

**Why:** Binding votes still require both majorities; pre-vote grants do not replace them.

### New majority completes election

**Before:** Node 1 is a candidate in term 3, voted for node 1. Commit 0, dispatch 0, last index 1 / term 2; 0 pending reads.

**Input**

1. From node 4: vote granted; term 3.

**Changes**

1. Role: candidate → leader.
2. Last log index: 1 → 2.
3. Last log term: 2 → 3.
4. Acknowledged follower prefixes: none → node 2: 0; node 3: 0; node 4: 0; node 5: 0.

**Ordered outputs**

1. Send to nodes 2, 3, 4, 5 (in that order): append #1; term 3; previous 1 / term 2; entry 2 (term 3); leader commit 0.

**Why:** The new majority permits leadership and its current-term no-op.

### Old majority alone cannot commit

**Before:** Node 1 is a leader in term 3, voted for node 1. Commit 0, dispatch 0, last index 2 / term 3; 0 pending reads.

**Input**

1. From node 2: append #1 accepted; term 3; matching prefix through 2.
2. From node 3: append #1 accepted; term 3; matching prefix through 2.

**Changes**

1. Acknowledged follower prefixes: node 2: 0; node 3: 0; node 4: 0; node 5: 0 → node 2: 2; node 3: 2; node 4: 0; node 5: 0.

**Ordered outputs**

None.

**Why:** Acknowledgements from nodes 2 and 3 plus the local log satisfy only the old replication majority.

### New majority commits the configuration and no-op

**Before:** Node 1 is a leader in term 3, voted for node 1. Commit 0, dispatch 0, last index 2 / term 3; 0 pending reads.

**Input**

1. From node 4: append #1 accepted; term 3; matching prefix through 2.

**Changes**

1. Commit index: 0 → 2.
2. Dispatch index: 0 → 2.
3. Acknowledged follower prefixes: node 2: 2; node 3: 2; node 4: 0; node 5: 0 → node 2: 2; node 3: 2; node 4: 2; node 5: 0.

**Ordered outputs**

1. Emit committed joint configuration 1 at index 1.

**Why:** Node 4 supplies the missing new majority. The current-term no-op authorizes commitment and the earlier joint configuration is emitted first.

### Batched inputs: what changes

Each checkpoint becomes one `step_batch` call; the primary story calls `step`
once per input. The full artifact keeps both sets of boundaries.

The displayed checkpoint states and ordered outputs match in both modes.
