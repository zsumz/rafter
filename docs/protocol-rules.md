# From a failed invariant to its protocol rule

The simulator's failure message comes from the checker that failed. Its reading
links explain the associated rule; they do not infer which production branch
ran. Keep the original failure classification: a missing witness is incomplete
coverage, and a harness error is not evidence of a protocol violation.

Start with the exact revision, feature configuration, bounds or soak seed, and
recorded actions. `Action::Deliver` includes the scheduled tick and ordinal of
the queued envelope; replacing it with an arbitrary message of the same kind
can change the execution. Run `replay_raft_trace` with the original configuration,
check, and `ReplayExpectation::FailureInvariant(failure.invariant())`.

`ReplayReport::states()` contains the initial state summary followed by one
summary per action actually replayed. Compare adjacent summaries for term,
role, commitment, and log-boundary changes. These summaries omit vote grants,
replication windows, and application state; use the production walkthrough's
complete observations when those are relevant to a refactor.

Replay checks invariants after each action and stops on the first violation.
Neither replay nor failure formatting minimizes a trace. A shorter reproduced
prefix is not a globally minimal execution. If manually reducing a trace,
record the deleted actions, preserve envelope identities, and require the same
checker to fail again; an unreplayable trace is not a reduction.

## Vote authority

EL-01 and EL-03 distinguish learning a term from granting a vote. In
[`handle_request_vote`](../crates/rafter/src/node/election.rs), a higher term
resets the prior vote before log eligibility is evaluated. Moving that reset
under `vote_granted` leaves obsolete authority after a legitimate rejection.

Run the existing
[`higher_term_vote_rejection_persists_term_before_response_escapes`](../crates/rafter-runtime/src/tests/hard_state/voting.rs).
The [generated rejected-vote walkthrough](../crates/rafter/PROTOCOL_WALKTHROUGHS.md#a-rejected-vote-still-changes-authority)
shows term 7 becoming 8 while the response remains a rejection.

## Acknowledgement authority

EL-07 fences acknowledgements in
[`handle_append_entries_response`](../crates/rafter/src/node/replication/response.rs),
after sender validation in [`dispatch`](../crates/rafter/src/node/dispatch.rs).
Newer terms end leadership; old terms and nonleader roles cannot update leader
evidence. Contact, sequence-qualified read confirmation, and acknowledged
replication are separate facts. Putting contact handling inside `success`
would discard legitimate rejection evidence.

The existing
[`authority_fencing_oracle_rejects_unfenced_higher_term_response`](../crates/rafter-sim/src/model_check/invariants/tests/election.rs)
is the detector control. The walkthrough preserves stale acknowledgement state
and read grants from a fresh same-term rejection.

## Current-term commitment

CM-03 guards
[`advance_commit_index_into`](../crates/rafter/src/node/commit/advance.rs).
Quorum replication supplies a candidate. Only a current-term candidate may
advance the leader's commit index directly; its preceding entries then commit
with it. Removing the term condition can commit an older history that a later
leader may replace.

Run
[`prior_term_quorum_candidate_waits_for_current_term_entry`](../crates/rafter/src/node/tests/membership/commit.rs)
and the existing detector
[`commit_certificate_detects_prior_term_candidate_commit`](../crates/rafter-sim/src/model_check/invariants/tests/commit_history.rs).
The walkthrough shows an older write remaining uncommitted until the leader's
current-term no-op is acknowledged.

## Joint majorities

CM-02 and MB-02 require both constituent majorities in
[`quorum_replicated_index`](../crates/rafter/src/node/commit/advance.rs).
With old voters 1,2,3 and new voters 3,4,5, acknowledgements from 1,2,3 satisfy
the old majority and a majority of the union, but leave the new set with only
one vote. The union shortcut can decide without a new majority.

The [joint recovery case](../crates/rafter/src/node/tests/bootstrap/configuration_progress.rs)
checks both elections and commits. The existing
[`commit_certificate_uses_pre_transition_joint_quorum_for_candidate_below_config`](../crates/rafter-sim/src/model_check/invariants/tests/commit_history.rs)
detects use of the wrong membership. The
[sorting reference](../crates/rafter/src/node/commit/quorum_test.rs) independently
checks selection across stable and joint memberships, including missing reports
and learners. Production continues to use selection.

## Read sequences

RD-02 is enforced by
[`acknowledge_read_barriers`](../crates/rafter/src/node/read_index.rs).
A quorum echo from before registration proves no authority after registration.
Ignoring the sequence gate could grant a read on a deposed leader.

Run
[`delayed_ack_from_an_older_round_never_confirms_a_barrier`](../crates/rafter/src/node/tests/read/barrier.rs).
The [read walkthrough](../crates/rafter/PROTOCOL_WALKTHROUGHS.md#read-barriers-and-acknowledgement-meanings)
shows pending reads surviving old echoes, fresh contact granting them, and a
higher term cancelling a later request. A grant still requires the embedding
to execute the application's committed prefix before serving the read.

The full [invariant catalog](raft-invariants.md) remains the authority for
registered IDs, claims, detectors, and evidence requirements. These reading
paths add no evidence credit and change no checker predicate.
