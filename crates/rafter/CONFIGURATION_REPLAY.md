# Committed configuration replay

`Output::ConfigurationCommitted` reports each configuration crossed by the
dispatch cursor, in log order. A lagging replica may learn commitment across
several configurations in one append. Sampling `committed_membership()` only
after that step loses the intermediate transitions, even when they admitted
or retired identities. The output's index and term identify its own entry,
not the final commit index reached by the step.

Each output carries `previous`, the membership immediately before that entry.
Consider this addition-only history:

```text
C1: voters 1,2,3; learners none
C2: voters 1,2,3; learners 4
C3: voters 1,2,3; learners 4,5
```

A consumer already holding C3 can replay the C1-to-C2 transition after a
restart. Comparing its present C3 against historical C2 would falsely classify
learner 5 as removed. Comparing the output's historical C1 against C2 correctly
records the admission of 4 and no removal. In a system where removal permanently
retires an identity, that distinction cannot be repaired by a later addition.

The production [output walk](src/node/commit/emit.rs) finds the predecessor in
the retained log, then snapshot membership, then bootstrap membership. The last
two need not correspond to a retained configuration entry, so `previous` is a
membership value rather than an optional entry. A consumer folding permanent
admissions and retirements can use these historical differences idempotently.
This does not relax the raw API's requirement to deliver outputs in order.

A committed transition is permanent. Effective, uncommitted configuration
changes may still be truncated and must not be mistaken for committed facts.
Snapshot installation emits no historical `ConfigurationCommitted` outputs:
only the boundary membership survives compaction, so intermediate transitions
cannot be reconstructed. The embedding handles that boundary through
`ApplySnapshot` and its snapshot recovery contract.

The existing [addition-only history test](../rafter-service/tests/transport_committed_transition.rs)
(`an_addition_only_history_retires_nobody_against_a_later_record`) checks this
example. The [restart case](../rafter-service/tests/transport_recovery_replay.rs)
(`a_restart_does_not_retire_a_member_the_replayed_history_only_ever_added`)
runs the production recovery path. The
[two-configuration append case](../rafter-service/tests/transport_configuration_history.rs)
checks preservation of an intermediate identity within one step.
