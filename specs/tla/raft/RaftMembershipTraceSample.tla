---- MODULE RaftMembershipTraceSample ----
EXTENDS Raft

CONSTANTS n1, n2, n3, v1, r1

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

TraceApplicationState(applications, node) ==
  applications[node].state

TraceAppliedThrough(applications, node) ==
  applications[node].through

RemovedVoters == {n1, n2}
RemoveJoint == JointMembership(Nodes, RemovedVoters)
RemoveStable == StableMembership(RemovedVoters)
AddJoint == JointMembership(RemovedVoters, Nodes)
AddStable == StableMembership(Nodes)

RemoveJointEntry == ConfigurationEntry(1, RemoveJoint)
RemoveStableEntry == ConfigurationEntry(1, RemoveStable)
AddJointEntry == ConfigurationEntry(1, AddJoint)
AddStableEntry == ConfigurationEntry(1, AddStable)
CommandEntry == Entry(1, v1)

TraceAppendMessage(to, entries, leaderCommit) ==
  [type |-> AppendEntries,
   term |-> 1,
   from |-> n1,
   to |-> to,
   entries |-> entries,
   leaderCommit |-> leaderCommit,
   senderMembership |-> LatestConfigurationIn(entries).config,
   senderPendingSelfRemoval |-> FALSE]

TraceAction0 ==
  /\ traceStep = 0
  /\ Timeout(n1)
  /\ traceStep' = 1

TraceAction1 ==
  /\ traceStep = 1
  /\ SendRequestVote(n1, n2)
  /\ traceStep' = 2

TraceAction2 ==
  /\ traceStep = 2
  /\ DeliverRequestVote(
       [type |-> RequestVote, term |-> 1, from |-> n1, to |-> n2])
  /\ traceStep' = 3

TraceAction3 ==
  /\ traceStep = 3
  /\ BecomeLeader(n1)
  /\ traceStep' = 4

TraceAction4 ==
  /\ traceStep = 4
  /\ EnterJoint(n1, {n1, n2})
  /\ traceStep' = 5

TraceAction5 ==
  /\ traceStep = 5
  /\ SendAppend(n1, n2)
  /\ traceStep' = 6

TraceAction6 ==
  /\ traceStep = 6
  /\ DeliverAppend(TraceAppendMessage(n2, <<RemoveJointEntry>>, 0))
  /\ traceStep' = 7

TraceAction7 ==
  /\ traceStep = 7
  /\ Commit(n1, 1)
  /\ traceStep' = 8

TraceAction8 ==
  /\ traceStep = 8
  /\ Apply(n1)
  /\ traceStep' = 9

TraceAction9 ==
  /\ traceStep = 9
  /\ LeaveJoint(n1)
  /\ traceStep' = 10

TraceAction10 ==
  /\ traceStep = 10
  /\ SendAppend(n1, n2)
  /\ traceStep' = 11

TraceAction11 ==
  /\ traceStep = 11
  /\ DeliverAppend(
       TraceAppendMessage(
         n2, <<RemoveJointEntry, RemoveStableEntry>>, 1))
  /\ traceStep' = 12

TraceAction12 ==
  /\ traceStep = 12
  /\ Commit(n1, 2)
  /\ traceStep' = 13

TraceAction13 ==
  /\ traceStep = 13
  /\ Apply(n1)
  /\ traceStep' = 14

TraceAction14 ==
  /\ traceStep = 14
  /\ EnterJoint(n1, Nodes)
  /\ traceStep' = 15

TraceAction15 ==
  /\ traceStep = 15
  /\ SendAppend(n1, n2)
  /\ traceStep' = 16

TraceAction16 ==
  /\ traceStep = 16
  /\ DeliverAppend(
       TraceAppendMessage(
         n2, <<RemoveJointEntry, RemoveStableEntry, AddJointEntry>>, 2))
  /\ traceStep' = 17

TraceAction17 ==
  /\ traceStep = 17
  /\ SendAppend(n1, n3)
  /\ traceStep' = 18

TraceAction18 ==
  /\ traceStep = 18
  /\ DeliverAppend(
       TraceAppendMessage(
         n3, <<RemoveJointEntry, RemoveStableEntry, AddJointEntry>>, 2))
  /\ traceStep' = 19

TraceAction19 ==
  /\ traceStep = 19
  /\ Commit(n1, 3)
  /\ traceStep' = 20

TraceAction20 ==
  /\ traceStep = 20
  /\ Apply(n1)
  /\ traceStep' = 21

TraceAction21 ==
  /\ traceStep = 21
  /\ LeaveJoint(n1)
  /\ traceStep' = 22

TraceAction22 ==
  /\ traceStep = 22
  /\ SendAppend(n1, n2)
  /\ traceStep' = 23

TraceAction23 ==
  /\ traceStep = 23
  /\ DeliverAppend(
       TraceAppendMessage(
         n2,
         <<RemoveJointEntry, RemoveStableEntry, AddJointEntry, AddStableEntry>>,
         3))
  /\ traceStep' = 24

TraceAction24 ==
  /\ traceStep = 24
  /\ SendAppend(n1, n3)
  /\ traceStep' = 25

TraceAction25 ==
  /\ traceStep = 25
  /\ DeliverAppend(
       TraceAppendMessage(
         n3,
         <<RemoveJointEntry, RemoveStableEntry, AddJointEntry, AddStableEntry>>,
         3))
  /\ traceStep' = 26

TraceAction26 ==
  /\ traceStep = 26
  /\ Commit(n1, 4)
  /\ traceStep' = 27

ReaddCheckpointReady ==
  /\ \A n \in Nodes :
       log[n] =
         <<RemoveJointEntry, RemoveStableEntry, AddJointEntry, AddStableEntry>>
  /\ commitIndex[n1] = 4
  /\ AppliedThrough(n1) = 3
  /\ \A n \in Nodes : EffectiveConfiguration(n).config = AddStable
  /\ LogicalEntry(n1, 4) = AddStableEntry
  /\ ApplyEntry(ApplicationState(n1), AddStableEntry).membership = AddStable

ReaddCheckpointReached ==
  /\ \A n \in Nodes :
       log'[n] =
         <<RemoveJointEntry, RemoveStableEntry, AddJointEntry, AddStableEntry>>
  /\ commitIndex'[n1] = 4
  /\ TraceAppliedThrough(applied', n1) = 4
  /\ TraceApplicationState(applied', n1).membership = AddStable

TraceAction27 ==
  /\ traceStep = 27
  /\ ReaddCheckpointReady
  /\ Apply(n1)
  /\ ReaddCheckpointReached
  /\ traceStep' = 28

TraceAction28 ==
  /\ traceStep = 28
  /\ ClientAppend(n1, v1)
  /\ traceStep' = 29

TraceAction29 ==
  /\ traceStep = 29
  /\ SendAppend(n1, n2)
  /\ traceStep' = 30

TraceAction30 ==
  /\ traceStep = 30
  /\ DeliverAppend(
       TraceAppendMessage(
         n2,
         <<RemoveJointEntry, RemoveStableEntry, AddJointEntry, AddStableEntry,
           CommandEntry>>,
         4))
  /\ traceStep' = 31

TraceAction31 ==
  /\ traceStep = 31
  /\ Commit(n1, 5)
  /\ traceStep' = 32

TraceAction32 ==
  /\ traceStep = 32
  /\ Apply(n1)
  /\ traceStep' = 33

TraceAction33 ==
  /\ traceStep = 33
  /\ RegisterRead(n1, r1)
  /\ traceStep' = 34

TraceAction34 ==
  /\ traceStep = 34
  /\ GrantRead(n1, r1)
  /\ traceStep' = 35

TraceAction35 ==
  /\ traceStep = 35
  /\ CreateSnapshot(n1)
  /\ traceStep' = 36

TraceAction36 ==
  /\ traceStep = 36
  /\ TransferSnapshot(n1, n3)
  /\ traceStep' = 37

TraceAction37 ==
  /\ traceStep = 37
  /\ InstallSnapshot
  /\ traceStep' = 38

TraceAction38 ==
  /\ traceStep = 38
  /\ CompactSnapshot(n1)
  /\ traceStep' = 39

TraceAction39 ==
  /\ traceStep = 39
  /\ ApplicationStateLoss(n1)
  /\ traceStep' = 40

TraceAction40 ==
  /\ traceStep = 40
  /\ Restart(n1)
  /\ traceStep' = 41

TraceAction41 ==
  /\ traceStep = 41
  /\ Timeout(n1)
  /\ traceStep' = 42

TraceAction42 ==
  /\ traceStep = 42
  /\ SendRequestVote(n1, n2)
  /\ traceStep' = 43

TraceAction43 ==
  /\ traceStep = 43
  /\ DeliverRequestVote(
       [type |-> RequestVote, term |-> 2, from |-> n1, to |-> n2])
  /\ traceStep' = 44

TraceAction44 ==
  /\ traceStep = 44
  /\ BecomeLeader(n1)
  /\ traceStep' = 45

TraceNext ==
  \/ TraceAction0
  \/ TraceAction1
  \/ TraceAction2
  \/ TraceAction3
  \/ TraceAction4
  \/ TraceAction5
  \/ TraceAction6
  \/ TraceAction7
  \/ TraceAction8
  \/ TraceAction9
  \/ TraceAction10
  \/ TraceAction11
  \/ TraceAction12
  \/ TraceAction13
  \/ TraceAction14
  \/ TraceAction15
  \/ TraceAction16
  \/ TraceAction17
  \/ TraceAction18
  \/ TraceAction19
  \/ TraceAction20
  \/ TraceAction21
  \/ TraceAction22
  \/ TraceAction23
  \/ TraceAction24
  \/ TraceAction25
  \/ TraceAction26
  \/ TraceAction27
  \/ TraceAction28
  \/ TraceAction29
  \/ TraceAction30
  \/ TraceAction31
  \/ TraceAction32
  \/ TraceAction33
  \/ TraceAction34
  \/ TraceAction35
  \/ TraceAction36
  \/ TraceAction37
  \/ TraceAction38
  \/ TraceAction39
  \/ TraceAction40
  \/ TraceAction41
  \/ TraceAction42
  \/ TraceAction43
  \/ TraceAction44
  \/ /\ traceStep = 45
     /\ UNCHANGED traceVars

TraceSpec ==
  /\ TraceInit
  /\ [][TraceNext]_traceVars
  /\ WF_traceVars(TraceNext)

TraceComplete == traceStep = 45

TraceCompletes == <>TraceComplete

====
