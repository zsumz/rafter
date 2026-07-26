---- MODULE RaftTraceSample ----
EXTENDS Raft

CONSTANTS n1, n2, n3, v1, v2, r1, r2

VARIABLE traceStep

traceVars == << currentTerm, votedFor, role, log, commitIndex,
               snapshotIndex, snapshotPrefix, compactionPending, snapshotTransfer,
               applied, applicationBases, applicationTransitions,
               messages, readRequests, readBarrierViolationSeen,
               electedLeaders, logicalPrefixLedger, committedLedger,
               commitWitnesses,
               higherTermStepDownFailed,
               staleAuthorityAccepted, frozenAppendAuthorityFailed, traceStep >>

TraceInit == Init /\ traceStep = 0

TraceAction0 ==
  /\ traceStep = 0
  /\ Timeout(n1)
  /\ traceStep' = 1

TraceAction1 ==
  /\ traceStep = 1
  /\ Restart(n1)
  /\ traceStep' = 2

TraceAction2 ==
  /\ traceStep = 2
  /\ Timeout(n2)
  /\ traceStep' = 3

TraceNext ==
  \/ TraceAction0
  \/ TraceAction1
  \/ TraceAction2
  \/ /\ traceStep = 3
     /\ UNCHANGED traceVars

TraceSpec ==
  /\ TraceInit
  /\ [][TraceNext]_traceVars
  /\ WF_traceVars(TraceNext)

TraceComplete == traceStep = 3

TraceCompletes == <>TraceComplete

====
