---- MODULE RafterInvariantDetectorNegative ----
EXTENDS Raft

CONSTANTS TargetPredicate, FixtureMode, FixtureA, FixtureB, FixtureC,
          FixtureValueA, FixtureValueB, FixtureRead

PredicateNames == {
    "ElectionSafety",
    "LogMatching",
    "LeaderCompleteness",
    "CommittedPrefixStability",
    "StateMachineSafety",
    "StaleLeaderFencing",
    "CommittedEntriesHaveQuorum",
    "ReadBarrierLinearizability"
}

DefaultFixtureMode == "Default"
ElectionRecorderMode == "ElectionRecorderOnly"
LogMatchingRecorderMode == "LogMatchingRecorderOnly"
SnapshotPrefixRecorderMode == "SnapshotPrefixRecorderOnly"
LeaderCompletenessRecorderMode == "LeaderCompletenessRecorderOnly"
CommittedPrefixRecorderMode == "CommittedPrefixRecorderOnly"
HigherTermRecorderMode == "HigherTermRecorderOnly"
StaleAuthorityRecorderMode == "StaleAuthorityRecorderOnly"
ApplicationRecorderMode == "ApplicationRecorderOnly"
ApplicationEpochRecorderMode == "ApplicationEpochRecorderOnly"
CommitQuorumRecorderMode == "CommitQuorumRecorderOnly"
ReadBarrierRecorderMode == "ReadBarrierRecorderOnly"

FixtureModes == {
    DefaultFixtureMode,
    ElectionRecorderMode,
    LogMatchingRecorderMode,
    SnapshotPrefixRecorderMode,
    LeaderCompletenessRecorderMode,
    CommittedPrefixRecorderMode,
    HigherTermRecorderMode,
    StaleAuthorityRecorderMode,
    ApplicationRecorderMode,
    ApplicationEpochRecorderMode,
    CommitQuorumRecorderMode,
    ReadBarrierRecorderMode
}

FixtureConstantsOK ==
  /\ Nodes = {FixtureA, FixtureB, FixtureC}
  /\ FixtureValueA \in Values
  /\ FixtureValueB \in Values
  /\ FixtureValueA # FixtureValueB
  /\ FixtureRead \in ReadRequests
  /\ FixtureMode \in FixtureModes
  /\ MaxTerm >= 2
  /\ MaxLogLen >= 2

BaseTerm == [n \in Nodes |-> 1]
BaseVote == [n \in Nodes |-> NoVote]
BaseRole == [n \in Nodes |-> Follower]
BaseLog == [n \in Nodes |-> <<>>]
BaseCommit == [n \in Nodes |-> 0]
BaseApplied == [n \in Nodes |->
  <<AppliedEpoch(0, InitialApplicationState, <<>>)>>]
BaseSnapshotIndex == [n \in Nodes |-> 0]
BaseSnapshotPrefix == [n \in Nodes |-> <<>>]
BaseCompactionPending == [n \in Nodes |-> FALSE]

ApplicationConfig ==
  JointMembership(Nodes, {FixtureA, FixtureB})

CorruptedApplicationConfig ==
  JointMembership(Nodes, {FixtureA, FixtureC})

RegressionStableConfig == StableMembership({FixtureA, FixtureB})

\* Detector scenarios historically name FixtureA's configuration view directly.
\* These are derived expressions, not protocol variables; the production model
\* therefore permits genuinely divergent per-node membership views.
membership == AppliedConfiguration(FixtureA).config
appliedConfigIndex == AppliedConfiguration(FixtureA).configIndex
effectiveMembership == EffectiveConfiguration(FixtureA).config
effectiveConfigIndex == EffectiveConfiguration(FixtureA).configIndex

DivergentLogs ==
  [n \in Nodes |->
    IF n = FixtureA THEN <<Entry(1, FixtureValueA)>>
    ELSE IF n = FixtureB THEN <<Entry(1, FixtureValueB)>>
    ELSE <<>>]

SingleAEntryLogs ==
  [n \in Nodes |->
    IF n = FixtureA THEN <<Entry(1, FixtureValueA)>> ELSE <<>>]

SingleBEntryLogs ==
  [n \in Nodes |->
    IF n = FixtureB THEN <<Entry(1, FixtureValueB)>> ELSE <<>>]

IsMode(predicate, mode) ==
  TargetPredicate = predicate /\ FixtureMode = mode

InitialTerm ==
  IF IsMode("StaleLeaderFencing", StaleAuthorityRecorderMode)
  THEN [n \in Nodes |-> IF n = FixtureA THEN 2 ELSE 1]
  ELSE IF IsMode("LeaderCompleteness", LeaderCompletenessRecorderMode)
  THEN [n \in Nodes |-> IF n = FixtureB THEN 2 ELSE 1]
  ELSE BaseTerm

InitialVote ==
  IF TargetPredicate = "ElectionSafety"
  THEN [n \in Nodes |-> FixtureA]
  ELSE BaseVote

InitialRole ==
  CASE TargetPredicate = "ElectionSafety" ->
         [n \in Nodes |-> IF n = FixtureA THEN Candidate ELSE Follower]
    [] IsMode("LeaderCompleteness", LeaderCompletenessRecorderMode) ->
         [n \in Nodes |-> IF n = FixtureB THEN Leader ELSE Follower]
    [] IsMode("LogMatching", SnapshotPrefixRecorderMode) ->
         [n \in Nodes |-> IF n = FixtureA THEN Leader ELSE Follower]
    [] TargetPredicate = "StaleLeaderFencing" /\
         FixtureMode # StaleAuthorityRecorderMode ->
         [n \in Nodes |-> IF n = FixtureA THEN Leader ELSE Follower]
    [] OTHER -> BaseRole

InitialLog ==
  CASE TargetPredicate = "StateMachineSafety" /\
         FixtureMode # ApplicationEpochRecorderMode ->
         [n \in Nodes |-> <<ConfigurationEntry(1, ApplicationConfig)>>]
    [] IsMode("StateMachineSafety", ApplicationEpochRecorderMode) ->
         [n \in Nodes |->
           IF n = FixtureA THEN <<Entry(1, FixtureValueA)>> ELSE <<>>]
    [] IsMode("LogMatching", SnapshotPrefixRecorderMode) -> SingleAEntryLogs
    [] OTHER -> BaseLog

InitialCommit ==
  CASE TargetPredicate = "StateMachineSafety" /\
         FixtureMode # ApplicationEpochRecorderMode ->
         [n \in Nodes |-> 1]
    [] IsMode("StateMachineSafety", ApplicationEpochRecorderMode) ->
         [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
    [] IsMode("LogMatching", SnapshotPrefixRecorderMode) ->
         [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
    [] OTHER -> BaseCommit

InitialSnapshotIndex ==
  IF IsMode("LogMatching", SnapshotPrefixRecorderMode)
  THEN [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
  ELSE BaseSnapshotIndex

InitialSnapshotPrefix ==
  IF IsMode("LogMatching", SnapshotPrefixRecorderMode)
  THEN [n \in Nodes |->
    IF n = FixtureA THEN <<Entry(1, FixtureValueA)>> ELSE <<>>]
  ELSE BaseSnapshotPrefix

InitialCompactionPending == BaseCompactionPending

InitialEffectiveMembership ==
  IF TargetPredicate = "StateMachineSafety" /\
       FixtureMode # ApplicationEpochRecorderMode
  THEN ApplicationConfig
  ELSE StableMembership(Nodes)

InitialEffectiveConfigIndex ==
  IF TargetPredicate = "StateMachineSafety" /\
       FixtureMode # ApplicationEpochRecorderMode
  THEN 1
  ELSE 0

InitialReadRequests ==
  IF IsMode("ReadBarrierLinearizability", ReadBarrierRecorderMode)
  THEN {[node |-> FixtureA, request |-> FixtureRead, committedFloor |-> 1]}
  ELSE {}

InitialCommittedLedger ==
  IF IsMode("CommittedPrefixStability", CommittedPrefixRecorderMode)
  THEN {CommittedEntry(1, Entry(1, FixtureValueA), 1)}
  ELSE {}

InitialElectedLeaders ==
  [t \in 1..MaxTerm |->
    IF /\ t = 1
       /\ (IsMode("LogMatching", SnapshotPrefixRecorderMode)
            \/ (TargetPredicate = "StaleLeaderFencing" /\
                 FixtureMode # StaleAuthorityRecorderMode))
    THEN {FixtureA}
    ELSE IF /\ t = 2
            /\ IsMode("LeaderCompleteness", LeaderCompletenessRecorderMode)
    THEN {FixtureB}
    ELSE {}]

FixtureInit ==
  /\ TargetPredicate \in PredicateNames
  /\ FixtureConstantsOK
  /\ currentTerm = InitialTerm
  /\ votedFor = InitialVote
  /\ role = InitialRole
  /\ log = InitialLog
  /\ commitIndex = InitialCommit
  /\ snapshotIndex = InitialSnapshotIndex
  /\ snapshotPrefix = InitialSnapshotPrefix
  /\ compactionPending = InitialCompactionPending
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = BaseApplied
  /\ messages = {}
  /\ readRequests = InitialReadRequests
  /\ readBarrierViolationSeen = FALSE
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = InitialEffectiveMembership
  /\ effectiveConfigIndex = InitialEffectiveConfigIndex
  /\ electedLeaders = InitialElectedLeaders
  /\ logicalPrefixLedger = {}
  /\ committedLedger = InitialCommittedLedger
  /\ commitWitnesses = EmptyCommitWitnessHistory
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE
  /\ lastAppendAccepted = FALSE

ElectionFirstLeader ==
  /\ role[FixtureA] = Candidate
  /\ electedLeaders[1] = {}
  /\ BecomeLeader(FixtureA)

PrepareSequentialCandidate ==
  /\ role[FixtureA] = Leader
  /\ votedFor' = [n \in Nodes |-> FixtureB]
  /\ role' = [n \in Nodes |-> IF n = FixtureB THEN Candidate ELSE Follower]
  /\ UNCHANGED <<currentTerm, log, commitIndex, messages,
                  readRequests, readBarrierViolationSeen,
                  electedLeaders>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars
  /\ UNCHANGED authorityVars

FaultySequentialLeader ==
  /\ role[FixtureB] = Candidate
  /\ role' = [n \in Nodes |-> IF n = FixtureB THEN Leader ELSE Follower]
  /\ RecordElection(FixtureB)
  /\ RecordAuthorityAcceptance(1, 1, TRUE)
  /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages,
                  readRequests, readBarrierViolationSeen,
                  higherTermStepDownFailed>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars

FaultyHigherTermAndAuthority ==
  /\ currentTerm[FixtureA] = 1
  /\ role[FixtureA] = Leader
  /\ ~higherTermStepDownFailed
  /\ ~staleAuthorityAccepted
  /\ currentTerm' = [currentTerm EXCEPT ![FixtureA] = 2]
  /\ votedFor' = [votedFor EXCEPT ![FixtureA] = NoVote]
  /\ role' = [role EXCEPT ![FixtureA] = Candidate]
  /\ RecordHigherTermOutcome(FixtureA, 2, TRUE)
  /\ RecordAuthorityAcceptance(
       1, 2, FixtureMode # HigherTermRecorderMode)
  /\ UNCHANGED <<log, commitIndex, messages, readRequests,
                  readBarrierViolationSeen, electedLeaders>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars

FaultyStaleAuthorityOnly ==
  /\ currentTerm[FixtureA] = 2
  /\ role[FixtureA] = Follower
  /\ ~higherTermStepDownFailed
  /\ ~staleAuthorityAccepted
  /\ RecordHigherTermOutcome(FixtureA, currentTerm[FixtureA], FALSE)
  /\ RecordAuthorityAcceptance(1, currentTerm[FixtureA], TRUE)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readBarrierViolationSeen,
                  electedLeaders>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars

ApplicationFirstApply ==
  /\ Len(ApplicationObservations(FixtureA)) = 0
  /\ Len(ApplicationObservations(FixtureB)) = 0
  /\ Apply(FixtureA)

FaultyApplicationResult ==
  LET entry == log[FixtureB][1]
      corruptedResult ==
        [referenceState |-> <<>>, membership |-> CorruptedApplicationConfig]
  IN
    /\ Len(ApplicationObservations(FixtureA)) = 1
    /\ Len(ApplicationObservations(FixtureB)) = 0
    /\ RecordApplication(FixtureB, entry, corruptedResult)
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    messages, readRequests, readBarrierViolationSeen,
                    electedLeaders>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED historyVars
    /\ UNCHANGED authorityVars

ApplicationEpochFirstApply ==
  /\ ApplicationEpoch(FixtureA) = 0
  /\ AppliedThrough(FixtureA) = 0
  /\ Apply(FixtureA)

ApplicationEpochLoss ==
  /\ ApplicationEpoch(FixtureA) = 0
  /\ AppliedThrough(FixtureA) = 1
  /\ ApplicationStateLoss(FixtureA)

FaultyApplicationEpochReplay ==
  LET entry == Entry(1, FixtureValueB)
      resultState == ApplyEntry(InitialApplicationState, entry)
  IN
    /\ ApplicationEpoch(FixtureA) = 1
    /\ AppliedThrough(FixtureA) = 0
    /\ RecordApplication(FixtureA, entry, resultState)
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    messages, readRequests, readBarrierViolationSeen,
                    electedLeaders>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED historyVars
    /\ UNCHANGED authorityVars

FaultyLogMatchingRecorder ==
  /\ logicalPrefixLedger = {}
  /\ RecordLogicalPrefixes(
       DivergentLogs, BaseSnapshotIndex, BaseSnapshotPrefix)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readBarrierViolationSeen,
                  electedLeaders,
                  committedLedger, commitWitnesses>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED authorityVars

ObserveSnapshotSource ==
  /\ logicalPrefixLedger = {}
  /\ RecordLogicalPrefixes(log, snapshotIndex, snapshotPrefix)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readBarrierViolationSeen,
                  electedLeaders,
                  committedLedger, commitWitnesses>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED authorityVars

FaultySnapshotTransfer ==
  /\ logicalPrefixLedger # {}
  /\ ~snapshotTransfer.active
  /\ snapshotIndex[FixtureB] = 0
  /\ snapshotTransfer' =
       [active |-> TRUE, term |-> 1,
        from |-> FixtureA, to |-> FixtureB, index |-> 1,
        prefix |-> <<Entry(1, FixtureValueB)>>]
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  snapshotIndex, snapshotPrefix, compactionPending,
                  messages, readRequests, readBarrierViolationSeen,
                  electedLeaders>>
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars
  /\ UNCHANGED authorityVars

FaultyLeaderCompletenessRecorder ==
  /\ committedLedger = {}
  /\ RecordCommittedEntries(
       SingleAEntryLogs, BaseSnapshotIndex, BaseSnapshotPrefix,
       FixtureA, 0, 1, 1)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readBarrierViolationSeen,
                  electedLeaders,
                  logicalPrefixLedger, commitWitnesses>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED authorityVars

FaultyCommittedPrefixRecorder ==
  /\ Cardinality(committedLedger) = 1
  /\ RecordCommittedEntries(
       SingleBEntryLogs, BaseSnapshotIndex, BaseSnapshotPrefix,
       FixtureB, 0, 1, 1)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readBarrierViolationSeen,
                  electedLeaders,
                  logicalPrefixLedger, commitWitnesses>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED authorityVars

FaultyCommitQuorumRecorder ==
  LET witness ==
        [index |-> 1,
         entry |-> Entry(1, FixtureValueA),
         leader |-> FixtureA,
         leaderRole |-> Leader,
         leaderTerm |-> 1,
         membership |-> StableMembership(Nodes),
         authorityMembership |-> StableMembership(Nodes),
         derivedMembership |-> ApplicationConfig,
         configIndex |-> 1,
         replicas |-> {FixtureA}]
  IN
    /\ commitWitnesses = EmptyCommitWitnessHistory
    /\ RecordCommitWitnesses({witness})
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    messages, readRequests, readBarrierViolationSeen,
                    electedLeaders,
                    logicalPrefixLedger, committedLedger>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED authorityVars

FaultyReadBarrierRecorder ==
  LET grant ==
        [node |-> FixtureA, request |-> FixtureRead, readIndex |-> 0]
  IN
    /\ readBarrierViolationSeen = FALSE
    /\ RecordReadGrant(grant)
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    messages, readRequests, electedLeaders>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED historyVars
    /\ UNCHANGED authorityVars

FixtureDone == UNCHANGED vars

FixtureNext ==
  \/ /\ TargetPredicate = "ElectionSafety"
     /\ (ElectionFirstLeader
          \/ PrepareSequentialCandidate
          \/ FaultySequentialLeader
          \/ FixtureDone)
  \/ /\ TargetPredicate = "StaleLeaderFencing"
     /\ (IF FixtureMode = StaleAuthorityRecorderMode
          THEN FaultyStaleAuthorityOnly
          ELSE FaultyHigherTermAndAuthority)
  \/ /\ TargetPredicate = "StaleLeaderFencing"
     /\ FixtureDone
  \/ /\ TargetPredicate = "StateMachineSafety"
     /\ FixtureMode # ApplicationEpochRecorderMode
     /\ (ApplicationFirstApply \/ FaultyApplicationResult \/ FixtureDone)
  \/ /\ IsMode("StateMachineSafety", ApplicationEpochRecorderMode)
     /\ (ApplicationEpochFirstApply
          \/ ApplicationEpochLoss
          \/ FaultyApplicationEpochReplay
          \/ FixtureDone)
  \/ /\ IsMode("LogMatching", LogMatchingRecorderMode)
     /\ (FaultyLogMatchingRecorder \/ FixtureDone)
  \/ /\ IsMode("LogMatching", SnapshotPrefixRecorderMode)
     /\ (ObserveSnapshotSource
          \/ FaultySnapshotTransfer
          \/ InstallSnapshot
          \/ FixtureDone)
  \/ /\ IsMode("LeaderCompleteness", LeaderCompletenessRecorderMode)
     /\ (FaultyLeaderCompletenessRecorder \/ FixtureDone)
  \/ /\ IsMode("CommittedPrefixStability", CommittedPrefixRecorderMode)
     /\ (FaultyCommittedPrefixRecorder \/ FixtureDone)
  \/ /\ IsMode("CommittedEntriesHaveQuorum", CommitQuorumRecorderMode)
     /\ (FaultyCommitQuorumRecorder \/ FixtureDone)
  \/ /\ IsMode("ReadBarrierLinearizability", ReadBarrierRecorderMode)
     /\ (FaultyReadBarrierRecorder \/ FixtureDone)

FixtureSpec == FixtureInit /\ [][FixtureNext]_vars

ShorterConflictEntry == Entry(1, FixtureValueA)

ShorterConflictVote(from, to, term) ==
  [type |-> RequestVote, term |-> term, from |-> from, to |-> to]

ShorterConflictAppend ==
  [type |-> AppendEntries,
   term |-> 2,
   from |-> FixtureB,
   to |-> FixtureA,
   entries |-> <<>>,
   leaderCommit |-> 0,
   senderMembership |-> StableMembership(Nodes),
   senderPendingSelfRemoval |-> FALSE]

ShorterConflictRepairInit ==
  /\ FixtureConstantsOK
  /\ Init

ShorterConflictRepairDone ==
  /\ role[FixtureB] = Leader
  /\ currentTerm[FixtureA] = 2
  /\ log[FixtureA] = <<>>

ShorterConflictRepairNext ==
  \/ /\ currentTerm[FixtureA] = 0
     /\ Timeout(FixtureA)
  \/ /\ currentTerm[FixtureA] = 1
     /\ role[FixtureA] = Candidate
     /\ votedFor[FixtureB] = NoVote
     /\ SendRequestVote(FixtureA, FixtureB)
  \/ /\ ShorterConflictVote(FixtureA, FixtureB, 1) \in messages
     /\ DeliverRequestVote(ShorterConflictVote(FixtureA, FixtureB, 1))
  \/ /\ role[FixtureA] = Candidate
     /\ votedFor[FixtureB] = FixtureA
     /\ BecomeLeader(FixtureA)
  \/ /\ role[FixtureA] = Leader
     /\ log[FixtureA] = <<>>
     /\ ClientAppend(FixtureA, FixtureValueA)
  \/ /\ log[FixtureA] = <<ShorterConflictEntry>>
     /\ currentTerm[FixtureB] = 1
     /\ role[FixtureB] = Follower
     /\ Timeout(FixtureB)
  \/ /\ currentTerm[FixtureB] = 2
     /\ role[FixtureB] = Candidate
     /\ votedFor[FixtureC] = NoVote
     /\ SendRequestVote(FixtureB, FixtureC)
  \/ /\ ShorterConflictVote(FixtureB, FixtureC, 2) \in messages
     /\ DeliverRequestVote(ShorterConflictVote(FixtureB, FixtureC, 2))
  \/ /\ role[FixtureB] = Candidate
     /\ votedFor[FixtureC] = FixtureB
     /\ BecomeLeader(FixtureB)
  \/ /\ role[FixtureB] = Leader
     /\ log[FixtureB] = <<>>
     /\ SendAppend(FixtureB, FixtureA)
  \/ /\ ShorterConflictAppend \in messages
     /\ DeliverAppend(ShorterConflictAppend)
  \/ /\ ShorterConflictRepairDone
     /\ UNCHANGED vars

ShorterConflictRepairSpec ==
  /\ ShorterConflictRepairInit
  /\ [][ShorterConflictRepairNext]_vars
  /\ WF_vars(ShorterConflictRepairNext)

ShorterConflictRepairCompletes == <>ShorterConflictRepairDone

ShorterConflictRepairInvariant ==
  /\ TypeOK
  /\ ElectionSafety
  /\ LogMatching
  /\ LeaderCompleteness
  /\ CommittedPrefixStability

FrozenAuthorityNewVoters == {FixtureB, FixtureC}

FrozenAuthorityJoint == JointMembership(Nodes, FrozenAuthorityNewVoters)

FrozenAuthorityStable == StableMembership(FrozenAuthorityNewVoters)

FrozenAuthorityJointEntry ==
  ConfigurationEntry(1, FrozenAuthorityJoint)

FrozenAuthorityStableEntry ==
  ConfigurationEntry(1, FrozenAuthorityStable)

FrozenAuthorityLog ==
  <<FrozenAuthorityJointEntry, FrozenAuthorityStableEntry>>

FrozenAuthorityAppend(
    to, entries, leaderCommit, frozenMembership, pendingRemoval) ==
  [type |-> AppendEntries,
   term |-> 1,
   from |-> FixtureA,
   to |-> to,
   entries |-> entries,
   leaderCommit |-> leaderCommit,
   senderMembership |-> frozenMembership,
   senderPendingSelfRemoval |-> pendingRemoval]

FrozenAuthorityDelayedAppend ==
  FrozenAuthorityAppend(
    FixtureC, FrozenAuthorityLog, 1,
    FrozenAuthorityStable, TRUE)

FrozenAppendAuthorityInit ==
  /\ FixtureConstantsOK
  /\ Init

FrozenAppendAuthorityDelivered ==
  /\ role[FixtureA] = Follower
  /\ commitIndex[FixtureA] = 2
  /\ FrozenAuthorityDelayedAppend \notin messages
  /\ FixtureA \notin ActiveVoters(
       EffectiveConfiguration(FixtureA).config)
  /\ ~PendingSelfRemoval(FixtureA)

FrozenAppendAuthorityDone ==
  /\ FrozenAppendAuthorityDelivered
  /\ lastAppendAccepted

FrozenAppendAuthorityNext ==
  \/ /\ currentTerm[FixtureA] = 0
     /\ Timeout(FixtureA)
  \/ /\ currentTerm[FixtureA] = 1
     /\ role[FixtureA] = Candidate
     /\ votedFor[FixtureB] = NoVote
     /\ SendRequestVote(FixtureA, FixtureB)
  \/ /\ ShorterConflictVote(FixtureA, FixtureB, 1) \in messages
     /\ DeliverRequestVote(ShorterConflictVote(FixtureA, FixtureB, 1))
  \/ /\ role[FixtureA] = Candidate
     /\ votedFor[FixtureB] = FixtureA
     /\ BecomeLeader(FixtureA)
  \/ /\ role[FixtureA] = Leader
     /\ log[FixtureA] = <<>>
     /\ EnterJoint(FixtureA, FrozenAuthorityNewVoters)
  \/ /\ log[FixtureA] = <<FrozenAuthorityJointEntry>>
     /\ log[FixtureB] = <<>>
     /\ SendAppend(FixtureA, FixtureB)
  \/ /\ FrozenAuthorityAppend(
          FixtureB, <<FrozenAuthorityJointEntry>>, 0,
          FrozenAuthorityJoint, FALSE) \in messages
     /\ DeliverAppend(FrozenAuthorityAppend(
          FixtureB, <<FrozenAuthorityJointEntry>>, 0,
          FrozenAuthorityJoint, FALSE))
  \/ /\ log[FixtureA] = <<FrozenAuthorityJointEntry>>
     /\ log[FixtureC] = <<>>
     /\ SendAppend(FixtureA, FixtureC)
  \/ /\ FrozenAuthorityAppend(
          FixtureC, <<FrozenAuthorityJointEntry>>, 0,
          FrozenAuthorityJoint, FALSE) \in messages
     /\ DeliverAppend(FrozenAuthorityAppend(
          FixtureC, <<FrozenAuthorityJointEntry>>, 0,
          FrozenAuthorityJoint, FALSE))
  \/ /\ commitIndex[FixtureA] = 0
     /\ log[FixtureB] = <<FrozenAuthorityJointEntry>>
     /\ log[FixtureC] = <<FrozenAuthorityJointEntry>>
     /\ Commit(FixtureA, 1)
  \/ /\ AppliedThrough(FixtureA) = 0
     /\ commitIndex[FixtureA] = 1
     /\ Apply(FixtureA)
  \/ /\ AppliedThrough(FixtureA) = 1
     /\ Len(log[FixtureA]) = 1
     /\ LeaveJoint(FixtureA)
  \/ /\ log[FixtureA] = FrozenAuthorityLog
     /\ Len(log[FixtureB]) = 1
     /\ SendAppend(FixtureA, FixtureB)
  \/ /\ FrozenAuthorityAppend(
          FixtureB, FrozenAuthorityLog, 1,
          FrozenAuthorityStable, TRUE) \in messages
     /\ DeliverAppend(FrozenAuthorityAppend(
          FixtureB, FrozenAuthorityLog, 1,
          FrozenAuthorityStable, TRUE))
  \/ /\ log[FixtureA] = FrozenAuthorityLog
     /\ Len(log[FixtureC]) = 1
     /\ SendAppend(FixtureA, FixtureC)
  \/ /\ FrozenAuthorityAppend(
          FixtureC, FrozenAuthorityLog, 1,
          FrozenAuthorityStable, TRUE) \in messages
     /\ Len(log[FixtureC]) = 1
     /\ DeliverAppend(FrozenAuthorityAppend(
          FixtureC, FrozenAuthorityLog, 1,
          FrozenAuthorityStable, TRUE))
  \/ /\ role[FixtureA] = Leader
     /\ commitIndex[FixtureA] = 1
     /\ log[FixtureB] = FrozenAuthorityLog
     /\ log[FixtureC] = FrozenAuthorityLog
     /\ FrozenAuthorityDelayedAppend \notin messages
     /\ SendAppend(FixtureA, FixtureC)
  \/ /\ role[FixtureA] = Leader
     /\ commitIndex[FixtureA] = 1
     /\ log[FixtureB] = FrozenAuthorityLog
     /\ log[FixtureC] = FrozenAuthorityLog
     /\ FrozenAuthorityDelayedAppend \in messages
     /\ Commit(FixtureA, 2)
  \/ /\ role[FixtureA] = Follower
     /\ commitIndex[FixtureA] = 2
     /\ FrozenAuthorityDelayedAppend \in messages
     /\ DeliverAppend(FrozenAuthorityDelayedAppend)
  \/ /\ FrozenAppendAuthorityDone
     /\ UNCHANGED vars

FrozenAppendAuthoritySpec ==
  /\ FrozenAppendAuthorityInit
  /\ [][FrozenAppendAuthorityNext]_vars
  /\ WF_vars(FrozenAppendAuthorityNext)

FrozenAppendAuthorityCompletes == <>FrozenAppendAuthorityDone

FrozenAppendAuthorityInvariant ==
  /\ TypeOK
  /\ ElectionSafety
  /\ LogMatching
  /\ LeaderCompleteness
  /\ CommittedPrefixStability
  /\ (FrozenAppendAuthorityDelivered => lastAppendAccepted)

CommitAuthorityTermRegressionEntry == Entry(1, FixtureValueA)

CommitAuthorityTermRegressionAuthority == Entry(3, FixtureValueB)

CommitAuthorityTermRegressionLog ==
  <<CommitAuthorityTermRegressionEntry, CommitAuthorityTermRegressionAuthority>>

CommitAuthorityTermRegressionInit ==
  /\ FixtureConstantsOK
  /\ Init

CommitAuthorityVote(from, to, term) ==
  [type |-> RequestVote, term |-> term, from |-> from, to |-> to]

CommitAuthorityAppend ==
  [type |-> AppendEntries,
   term |-> 3,
   from |-> FixtureA,
   to |-> FixtureC,
   entries |-> CommitAuthorityTermRegressionLog,
   leaderCommit |-> 0,
   senderMembership |-> StableMembership(Nodes),
   senderPendingSelfRemoval |-> FALSE]

CommitAuthorityTermRegressionDone ==
  /\ commitIndex[FixtureA] = 2
  /\ committedLedger = {
       CommittedEntry(1, CommitAuthorityTermRegressionEntry, 3),
       CommittedEntry(2, CommitAuthorityTermRegressionAuthority, 3)}
  /\ \E witness \in commitWitnesses.witnessedCommits :
       /\ witness.index = 1
       /\ witness.entry = CommitAuthorityTermRegressionEntry

CommitAuthorityTermRegressionNext ==
  \/ /\ currentTerm[FixtureA] = 0
     /\ currentTerm[FixtureB] = 0
     /\ Timeout(FixtureA)
  \/ /\ currentTerm[FixtureA] = 1
     /\ role[FixtureA] = Candidate
     /\ votedFor[FixtureC] = NoVote
     /\ SendRequestVote(FixtureA, FixtureC)
  \/ /\ CommitAuthorityVote(FixtureA, FixtureC, 1) \in messages
     /\ DeliverRequestVote(
          CommitAuthorityVote(FixtureA, FixtureC, 1))
  \/ /\ currentTerm[FixtureA] = 1
     /\ role[FixtureA] = Candidate
     /\ votedFor[FixtureC] = FixtureA
     /\ BecomeLeader(FixtureA)
  \/ /\ currentTerm[FixtureA] = 1
     /\ role[FixtureA] = Leader
     /\ log[FixtureA] = <<>>
     /\ ClientAppend(FixtureA, FixtureValueA)
  \/ /\ log[FixtureA] = <<CommitAuthorityTermRegressionEntry>>
     /\ currentTerm[FixtureB] = 0
     /\ Timeout(FixtureB)
  \/ /\ currentTerm[FixtureB] = 1
     /\ role[FixtureB] = Candidate
     /\ Timeout(FixtureB)
  \/ /\ currentTerm[FixtureB] = 2
     /\ role[FixtureB] = Candidate
     /\ votedFor[FixtureC] # FixtureB
     /\ SendRequestVote(FixtureB, FixtureC)
  \/ /\ CommitAuthorityVote(FixtureB, FixtureC, 2) \in messages
     /\ DeliverRequestVote(
          CommitAuthorityVote(FixtureB, FixtureC, 2))
  \/ /\ currentTerm[FixtureB] = 2
     /\ role[FixtureB] = Candidate
     /\ votedFor[FixtureC] = FixtureB
     /\ BecomeLeader(FixtureB)
  \/ /\ role[FixtureB] = Leader
     /\ currentTerm[FixtureA] = 1
     /\ Timeout(FixtureA)
  \/ /\ role[FixtureB] = Leader
     /\ currentTerm[FixtureA] = 2
     /\ role[FixtureA] = Candidate
     /\ Timeout(FixtureA)
  \/ /\ currentTerm[FixtureA] = 3
     /\ role[FixtureA] = Candidate
     /\ votedFor[FixtureC] # FixtureA
     /\ SendRequestVote(FixtureA, FixtureC)
  \/ /\ CommitAuthorityVote(FixtureA, FixtureC, 3) \in messages
     /\ DeliverRequestVote(
          CommitAuthorityVote(FixtureA, FixtureC, 3))
  \/ /\ currentTerm[FixtureA] = 3
     /\ role[FixtureA] = Candidate
     /\ votedFor[FixtureC] = FixtureA
     /\ BecomeLeader(FixtureA)
  \/ /\ currentTerm[FixtureA] = 3
     /\ role[FixtureA] = Leader
     /\ log[FixtureA] = <<CommitAuthorityTermRegressionEntry>>
     /\ ClientAppend(FixtureA, FixtureValueB)
  \/ /\ log[FixtureA] = CommitAuthorityTermRegressionLog
     /\ log[FixtureC] = <<>>
     /\ SendAppend(FixtureA, FixtureC)
  \/ /\ CommitAuthorityAppend \in messages
     /\ DeliverAppend(CommitAuthorityAppend)
  \/ /\ log[FixtureC] = CommitAuthorityTermRegressionLog
     /\ commitIndex[FixtureA] = 0
     /\ Commit(FixtureA, 2)
  \/ /\ CommitAuthorityTermRegressionDone
     /\ UNCHANGED vars

CommitAuthorityTermRegressionSpec ==
  /\ CommitAuthorityTermRegressionInit
  /\ [][CommitAuthorityTermRegressionNext]_vars
  /\ WF_vars(CommitAuthorityTermRegressionNext)

CommitAuthorityTermRegressionCompletes ==
  <>CommitAuthorityTermRegressionDone

CommitAuthorityTermRegressionInvariant ==
  /\ TypeOK
  /\ ElectionSafety
  /\ LogMatching
  /\ LeaderCompleteness
  /\ CommittedEntriesHaveQuorum

RemovedCandidateJointEntry == ConfigurationEntry(1, ApplicationConfig)

RemovedCandidateStableEntry ==
  ConfigurationEntry(1, RegressionStableConfig)

RemovedCandidateLog ==
  <<RemovedCandidateJointEntry, RemovedCandidateStableEntry>>

RemovedCandidateVote ==
  [type |-> RequestVote,
   term |-> 2,
   from |-> FixtureC,
   to |-> FixtureB]

RemovedCandidateAppend(entries, leaderCommit, senderMembership) ==
  [type |-> AppendEntries,
   term |-> 1,
   from |-> FixtureA,
   to |-> FixtureB,
   entries |-> entries,
   leaderCommit |-> leaderCommit,
   senderMembership |-> senderMembership,
   senderPendingSelfRemoval |-> FALSE]

RemovedCandidateVoteGuardInit ==
  /\ FixtureConstantsOK
  /\ Init

RemovedCandidateVoteGuardDone ==
  /\ currentTerm[FixtureB] = 2
  /\ RemovedCandidateVote \notin messages

RemovedCandidateVoteGuardNext ==
  \/ /\ currentTerm[FixtureA] = 0
     /\ Timeout(FixtureA)
  \/ /\ currentTerm[FixtureA] = 1
     /\ role[FixtureA] = Candidate
     /\ votedFor[FixtureB] = NoVote
     /\ SendRequestVote(FixtureA, FixtureB)
  \/ /\ CommitAuthorityVote(FixtureA, FixtureB, 1) \in messages
     /\ DeliverRequestVote(
          CommitAuthorityVote(FixtureA, FixtureB, 1))
  \/ /\ currentTerm[FixtureA] = 1
     /\ role[FixtureA] = Candidate
     /\ votedFor[FixtureB] = FixtureA
     /\ BecomeLeader(FixtureA)
  \/ /\ role[FixtureA] = Leader
     /\ log[FixtureA] = <<>>
     /\ EnterJoint(FixtureA, {FixtureA, FixtureB})
  \/ /\ log[FixtureA] = <<RemovedCandidateJointEntry>>
     /\ log[FixtureB] = <<>>
     /\ SendAppend(FixtureA, FixtureB)
  \/ /\ RemovedCandidateAppend(
          <<RemovedCandidateJointEntry>>, 0, ApplicationConfig) \in messages
     /\ DeliverAppend(RemovedCandidateAppend(
          <<RemovedCandidateJointEntry>>, 0, ApplicationConfig))
  \/ /\ commitIndex[FixtureA] = 0
     /\ log[FixtureB] = <<RemovedCandidateJointEntry>>
     /\ Commit(FixtureA, 1)
  \/ /\ AppliedThrough(FixtureA) = 0
     /\ commitIndex[FixtureA] = 1
     /\ Apply(FixtureA)
  \/ /\ AppliedConfiguration(FixtureA).config = ApplicationConfig
     /\ Len(log[FixtureA]) = 1
     /\ LeaveJoint(FixtureA)
  \/ /\ log[FixtureA] = RemovedCandidateLog
     /\ Len(log[FixtureB]) = 1
     /\ SendAppend(FixtureA, FixtureB)
  \/ /\ RemovedCandidateAppend(
          RemovedCandidateLog, 1, RegressionStableConfig) \in messages
     /\ DeliverAppend(RemovedCandidateAppend(
          RemovedCandidateLog, 1, RegressionStableConfig))
  \/ /\ commitIndex[FixtureA] = 1
     /\ log[FixtureB] = RemovedCandidateLog
     /\ Commit(FixtureA, 2)
  \/ /\ AppliedThrough(FixtureA) = 1
     /\ commitIndex[FixtureA] = 2
     /\ Apply(FixtureA)
  \/ /\ AppliedThrough(FixtureA) = 2
     /\ currentTerm[FixtureC] = 0
     /\ Timeout(FixtureC)
  \/ /\ AppliedThrough(FixtureA) = 2
     /\ currentTerm[FixtureC] = 1
     /\ Timeout(FixtureC)
  \/ /\ currentTerm[FixtureC] = 2
     /\ role[FixtureC] = Candidate
     /\ RemovedCandidateVote \notin messages
     /\ ~RemovedCandidateVoteGuardDone
     /\ SendRequestVote(FixtureC, FixtureB)
  \/ /\ RemovedCandidateVote \in messages
     /\ DeliverRequestVote(RemovedCandidateVote)
  \/ /\ RemovedCandidateVoteGuardDone
     /\ UNCHANGED vars

RemovedCandidateVoteGuardSpec ==
  /\ RemovedCandidateVoteGuardInit
  /\ [][RemovedCandidateVoteGuardNext]_vars
  /\ WF_vars(RemovedCandidateVoteGuardNext)

RemovedCandidateVoteGuardCompletes ==
  <>RemovedCandidateVoteGuardDone

RemovedCandidateVoteGuardInvariant ==
  /\ TypeOK
  /\ ElectionSafety
  /\ LogMatching
  /\ LeaderCompleteness
  /\ CommittedPrefixStability
  /\ StateMachineSafety
  /\ StaleLeaderFencing
  /\ CommittedEntriesHaveQuorum
  /\ ReadBarrierLinearizability
  /\ votedFor[FixtureB] # FixtureC

BaseExtendedState ==
  /\ snapshotIndex = BaseSnapshotIndex
  /\ snapshotPrefix = BaseSnapshotPrefix
  /\ compactionPending = BaseCompactionPending
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = BaseApplied
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {}
  /\ commitWitnesses = EmptyCommitWitnessHistory

ClosedElectionLifecycleInit ==
  /\ FixtureConstantsOK
  /\ currentTerm = BaseTerm
  /\ votedFor = [n \in Nodes |-> FixtureA]
  /\ role = [n \in Nodes |-> IF n = FixtureA THEN Leader ELSE Follower]
  /\ log = BaseLog
  /\ commitIndex = BaseCommit
  /\ BaseExtendedState
  /\ messages = {}
  /\ readRequests = {}
  /\ readBarrierViolationSeen = FALSE
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = StableMembership(Nodes)
  /\ effectiveConfigIndex = 0
  /\ electedLeaders = [term \in 1..MaxTerm |->
       IF term = 1 THEN {FixtureA} ELSE {}]
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE
  /\ lastAppendAccepted = FALSE

ClosedElectionLifecycleNext ==
  \/ /\ currentTerm[FixtureA] = 1
     /\ Timeout(FixtureA)
  \/ /\ currentTerm[FixtureA] = 2
     /\ currentTerm[FixtureB] = 1
     /\ Timeout(FixtureB)
  \/ /\ currentTerm[FixtureA] = 2
     /\ currentTerm[FixtureB] = 2
     /\ currentTerm[FixtureC] = 1
     /\ Timeout(FixtureC)
  \/ /\ \A n \in Nodes : currentTerm[n] = 2
     /\ electedLeaders[1] = {}
     /\ UNCHANGED vars

ClosedElectionLifecycleSpec ==
  /\ ClosedElectionLifecycleInit
  /\ [][ClosedElectionLifecycleNext]_vars
  /\ WF_vars(ClosedElectionLifecycleNext)

ClosedElectionLifecycleInvariant == TypeOK /\ ElectionSafety

ClosedElectionLifecycleComplete ==
  /\ \A n \in Nodes : currentTerm[n] = 2
  /\ electedLeaders[1] = {}

ClosedElectionLifecycleCompletes == <>ClosedElectionLifecycleComplete

StaleMessageLifecycleInit ==
  /\ FixtureConstantsOK
  /\ currentTerm = [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
  /\ votedFor = BaseVote
  /\ role = [n \in Nodes |-> IF n = FixtureA THEN Leader ELSE Follower]
  /\ log = BaseLog
  /\ commitIndex = BaseCommit
  /\ BaseExtendedState
  /\ messages = {}
  /\ readRequests = {}
  /\ readBarrierViolationSeen = FALSE
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = StableMembership(Nodes)
  /\ effectiveConfigIndex = 0
  /\ electedLeaders = [t \in 1..MaxTerm |->
       IF t = 1 THEN {FixtureA} ELSE {}]
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE
  /\ lastAppendAccepted = FALSE

StaleMessageLifecycleNext ==
  \/ /\ currentTerm[FixtureB] = 0
     /\ messages = {}
     /\ SendAppend(FixtureA, FixtureB)
  \/ /\ currentTerm[FixtureB] < 2
     /\ messages # {}
     /\ Timeout(FixtureB)
  \/ /\ currentTerm[FixtureB] = 2
     /\ messages = {}
     /\ UNCHANGED vars

StaleMessageLifecycleSpec ==
  /\ StaleMessageLifecycleInit
  /\ [][StaleMessageLifecycleNext]_vars
  /\ WF_vars(StaleMessageLifecycleNext)

StaleMessageLifecycleInvariant == TypeOK

StaleMessageLifecycleComplete ==
  /\ currentTerm[FixtureB] = 2
  /\ messages = {}

StaleMessageLifecycleCompletes == <>StaleMessageLifecycleComplete

SnapshotLifecycleEntry == Entry(1, FixtureValueA)

SixEntryReplayLog ==
  <<Entry(1, FixtureValueA), Entry(1, FixtureValueB),
    Entry(1, FixtureValueA), Entry(1, FixtureValueB),
    Entry(1, FixtureValueA), Entry(1, FixtureValueB)>>

SixEntryReplayExpected ==
  <<FixtureValueA, FixtureValueB, FixtureValueA,
    FixtureValueB, FixtureValueA, FixtureValueB>>

SixEntryReplayInit == FixtureConstantsOK /\ Init

SixEntryReplayNext == UNCHANGED vars

SixEntryReplaySpec ==
  /\ SixEntryReplayInit
  /\ [][SixEntryReplayNext]_vars

SixEntryReplayInvariant ==
  LET state == StateAfterEntries(SixEntryReplayLog)
  IN /\ state.referenceState = SixEntryReplayExpected
     /\ state.membership = StableMembership(Nodes)

SnapshotLifecycleResult ==
  ApplyEntry(InitialApplicationState, SnapshotLifecycleEntry)

SnapshotLifecycleWitness ==
  [index |-> 1,
   entry |-> SnapshotLifecycleEntry,
   leader |-> FixtureA,
   leaderRole |-> Leader,
   leaderTerm |-> 1,
   membership |-> StableMembership(Nodes),
   authorityMembership |-> StableMembership(Nodes),
   derivedMembership |-> StableMembership(Nodes),
   configIndex |-> 0,
   replicas |-> {FixtureA, FixtureB}]

SnapshotLifecycleInit ==
  /\ FixtureConstantsOK
  /\ currentTerm = BaseTerm
  /\ votedFor = [n \in Nodes |-> FixtureA]
  /\ role = [n \in Nodes |-> IF n = FixtureA THEN Leader ELSE Follower]
  /\ log = [n \in Nodes |->
       IF n \in {FixtureA, FixtureB} THEN <<SnapshotLifecycleEntry>> ELSE <<>>]
  /\ commitIndex = [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
  /\ snapshotIndex = BaseSnapshotIndex
  /\ snapshotPrefix = BaseSnapshotPrefix
  /\ compactionPending = BaseCompactionPending
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = [n \in Nodes |->
       IF n = FixtureA
       THEN <<AppliedEpoch(0, InitialApplicationState,
         <<AppliedObservation(SnapshotLifecycleEntry, SnapshotLifecycleResult)>>)>>
       ELSE <<AppliedEpoch(0, InitialApplicationState, <<>>)>>]
  /\ messages = {}
  /\ readRequests = {}
  /\ readBarrierViolationSeen = FALSE
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = StableMembership(Nodes)
  /\ effectiveConfigIndex = 0
  /\ electedLeaders = [t \in 1..MaxTerm |->
       IF t = 1 THEN {FixtureA} ELSE {}]
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {CommittedEntry(1, SnapshotLifecycleEntry, 1)}
  /\ commitWitnesses = CommitWitnessHistory(
       CommitWitnessKeys({SnapshotLifecycleWitness}), FALSE)
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE
  /\ lastAppendAccepted = FALSE

SnapshotLifecycleNext ==
  \/ /\ snapshotIndex[FixtureA] = 0
     /\ CreateSnapshot(FixtureA)
     /\ compactionPending'[FixtureA]
  \/ /\ snapshotIndex[FixtureA] = 1
     /\ compactionPending[FixtureA]
     /\ CompactSnapshot(FixtureA)
     /\ ~compactionPending'[FixtureA]
  \/ /\ snapshotIndex[FixtureA] = 1
     /\ ~compactionPending[FixtureA]
     /\ snapshotIndex[FixtureB] = 0
     /\ ~snapshotTransfer.active
     /\ TransferSnapshot(FixtureA, FixtureB)
  \/ /\ snapshotTransfer.active
     /\ InstallSnapshot
  \/ /\ snapshotIndex[FixtureB] = 1
     /\ currentTerm[FixtureB] = 1
     /\ role[FixtureB] = Follower
     /\ Timeout(FixtureB)
  \/ /\ snapshotIndex[FixtureB] = 1
     /\ currentTerm[FixtureB] = 2
     /\ role[FixtureB] = Candidate
     /\ Restart(FixtureB)
  \/ /\ snapshotIndex[FixtureB] = 1
     /\ currentTerm[FixtureB] = 2
     /\ role[FixtureB] = Follower
     /\ UNCHANGED vars

SnapshotLifecycleSpec ==
  /\ SnapshotLifecycleInit
  /\ [][SnapshotLifecycleNext]_vars
  /\ WF_vars(SnapshotLifecycleNext)

SnapshotLifecycleInvariant ==
  /\ SnapshotIdentitySoundFor(
       log, snapshotIndex, snapshotPrefix, compactionPending)
  /\ LogMatching
  /\ LeaderCompleteness
  /\ CommittedPrefixStability
  /\ StateMachineSafety
  /\ CommittedEntriesHaveQuorum

SnapshotLifecycleComplete ==
  /\ snapshotIndex[FixtureB] = 1
  /\ ~compactionPending[FixtureA]
  /\ ~compactionPending[FixtureB]
  /\ ApplicationEpoch(FixtureB) = 1
  /\ Len(ApplicationObservations(FixtureB)) = 0
  /\ currentTerm[FixtureB] = 2
  /\ role[FixtureB] = Follower

SnapshotLifecycleCompletes == <>SnapshotLifecycleComplete

ApplicationEpochLifecycleInit ==
  /\ FixtureConstantsOK
  /\ currentTerm = BaseTerm
  /\ votedFor = [n \in Nodes |-> FixtureA]
  /\ role = [n \in Nodes |-> IF n = FixtureA THEN Leader ELSE Follower]
  /\ log = [n \in Nodes |->
       IF n \in {FixtureA, FixtureB} THEN <<SnapshotLifecycleEntry>> ELSE <<>>]
  /\ commitIndex = [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
  /\ snapshotIndex = BaseSnapshotIndex
  /\ snapshotPrefix = BaseSnapshotPrefix
  /\ compactionPending = BaseCompactionPending
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = BaseApplied
  /\ messages = {}
  /\ readRequests = {}
  /\ readBarrierViolationSeen = FALSE
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = StableMembership(Nodes)
  /\ effectiveConfigIndex = 0
  /\ electedLeaders = [t \in 1..MaxTerm |->
       IF t = 1 THEN {FixtureA} ELSE {}]
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {CommittedEntry(1, SnapshotLifecycleEntry, 1)}
  /\ commitWitnesses = CommitWitnessHistory(
       CommitWitnessKeys({SnapshotLifecycleWitness}), FALSE)
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE
  /\ lastAppendAccepted = FALSE

ApplicationEpochLifecycleNext ==
  \/ /\ ApplicationEpoch(FixtureA) = 0
     /\ AppliedThrough(FixtureA) = 0
     /\ Apply(FixtureA)
  \/ /\ ApplicationEpoch(FixtureA) = 0
     /\ AppliedThrough(FixtureA) = 1
     /\ ApplicationStateLoss(FixtureA)
  \/ /\ ApplicationEpoch(FixtureA) = 1
     /\ AppliedThrough(FixtureA) = 0
     /\ Apply(FixtureA)
  \/ /\ ApplicationEpoch(FixtureA) = 1
     /\ AppliedThrough(FixtureA) = 1
     /\ UNCHANGED vars

ApplicationEpochLifecycleSpec ==
  /\ ApplicationEpochLifecycleInit
  /\ [][ApplicationEpochLifecycleNext]_vars
  /\ WF_vars(ApplicationEpochLifecycleNext)

ApplicationEpochLifecycleInvariant ==
  /\ StateMachineSafety
  /\ CommittedEntriesHaveQuorum

ApplicationEpochLifecycleComplete ==
  /\ ApplicationEpoch(FixtureA) = 1
  /\ AppliedThrough(FixtureA) = 1
  /\ Len(applied[FixtureA]) = 2
  /\ \A epochPosition \in 1..Len(applied[FixtureA]) :
       Len(applied[FixtureA][epochPosition].observations) = 1

ApplicationEpochLifecycleCompletes == <>ApplicationEpochLifecycleComplete

SelfRemovalNewVoters == {FixtureB, FixtureC}

SelfRemovalJointMembership ==
  JointMembership(Nodes, SelfRemovalNewVoters)

SelfRemovalStableMembership == StableMembership(SelfRemovalNewVoters)

SelfRemovalJointEntry ==
  ConfigurationEntry(1, SelfRemovalJointMembership)

SelfRemovalStableEntry ==
  ConfigurationEntry(1, SelfRemovalStableMembership)

SelfRemovalJointState ==
  ApplyEntry(InitialApplicationState, SelfRemovalJointEntry)

SelfRemovalJointWitness ==
  [index |-> 1,
   entry |-> SelfRemovalJointEntry,
   leader |-> FixtureA,
   leaderRole |-> Leader,
   leaderTerm |-> 1,
   membership |-> SelfRemovalJointMembership,
   authorityMembership |-> SelfRemovalJointMembership,
   derivedMembership |-> SelfRemovalJointMembership,
   configIndex |-> 1,
   replicas |-> Nodes]

SelfRemovalCommitInit ==
  /\ FixtureConstantsOK
  /\ currentTerm = BaseTerm
  /\ votedFor = [n \in Nodes |-> FixtureA]
  /\ role = [n \in Nodes |-> IF n = FixtureA THEN Leader ELSE Follower]
  /\ log = [n \in Nodes |->
       <<SelfRemovalJointEntry, SelfRemovalStableEntry>>]
  /\ commitIndex = [n \in Nodes |-> 1]
  /\ snapshotIndex = BaseSnapshotIndex
  /\ snapshotPrefix = BaseSnapshotPrefix
  /\ compactionPending = BaseCompactionPending
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = [n \in Nodes |->
       IF n = FixtureA
       THEN <<AppliedEpoch(0, InitialApplicationState,
         <<AppliedObservation(SelfRemovalJointEntry, SelfRemovalJointState)>>)>>
       ELSE <<AppliedEpoch(0, InitialApplicationState, <<>>)>>]
  /\ messages = {}
  /\ readRequests = {}
  /\ readBarrierViolationSeen = FALSE
  /\ membership = SelfRemovalJointMembership
  /\ appliedConfigIndex = 1
  /\ effectiveMembership = SelfRemovalStableMembership
  /\ effectiveConfigIndex = 2
  /\ electedLeaders = [t \in 1..MaxTerm |->
       IF t = 1 THEN {FixtureA} ELSE {}]
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {CommittedEntry(1, SelfRemovalJointEntry, 1)}
  /\ commitWitnesses = CommitWitnessHistory(
       CommitWitnessKeys({SelfRemovalJointWitness}), FALSE)
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE
  /\ lastAppendAccepted = FALSE

SelfRemovalCommitAction ==
  /\ commitIndex[FixtureA] = 1
  /\ Commit(FixtureA, 2)

SelfRemovalCommitNext ==
  \/ SelfRemovalCommitAction
  \/ /\ commitIndex[FixtureA] = 2
     /\ UNCHANGED vars

SelfRemovalCommitSpec ==
  /\ SelfRemovalCommitInit
  /\ [][SelfRemovalCommitNext]_vars
  /\ WF_vars(SelfRemovalCommitAction)

SelfRemovalCommitInvariant ==
  /\ effectiveMembership = SelfRemovalStableMembership
  /\ effectiveConfigIndex = 2
  /\ IF commitIndex[FixtureA] = 1
     THEN /\ role[FixtureA] = Leader
          /\ PendingSelfRemoval(FixtureA)
     ELSE /\ commitIndex[FixtureA] = 2
          /\ role[FixtureA] = Follower
          /\ ~PendingSelfRemoval(FixtureA)

SelfRemovalCommitComplete ==
  /\ commitIndex[FixtureA] = 2
  /\ role[FixtureA] = Follower

SelfRemovalCommitCompletes == <>SelfRemovalCommitComplete

ConfigurationRegressionInit ==
  /\ FixtureConstantsOK
  /\ currentTerm = BaseTerm
  /\ votedFor = BaseVote
  /\ role = BaseRole
  /\ log = [n \in Nodes |->
       IF n = FixtureA
       THEN <<ConfigurationEntry(1, ApplicationConfig),
               ConfigurationEntry(1, RegressionStableConfig)>>
       ELSE IF n = FixtureB
       THEN <<ConfigurationEntry(1, ApplicationConfig)>>
       ELSE <<>>]
  /\ commitIndex = [n \in Nodes |->
       IF n = FixtureA THEN 2 ELSE IF n = FixtureB THEN 1 ELSE 0]
  /\ BaseExtendedState
  /\ messages = {}
  /\ readRequests = {}
  /\ readBarrierViolationSeen = FALSE
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = RegressionStableConfig
  /\ effectiveConfigIndex = 2
  /\ electedLeaders = [t \in 1..MaxTerm |-> {}]
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE
  /\ lastAppendAccepted = FALSE

ConfigurationRegressionNext ==
  \/ /\ AppliedThrough(FixtureA) = 0
     /\ Apply(FixtureA)
  \/ /\ AppliedThrough(FixtureA) = 1
     /\ Apply(FixtureA)
  \/ /\ AppliedThrough(FixtureA) = 2
     /\ AppliedThrough(FixtureB) = 0
     /\ Apply(FixtureB)
  \/ /\ AppliedThrough(FixtureA) = 2
     /\ AppliedThrough(FixtureB) = 1
     /\ UNCHANGED vars

ConfigurationRegressionSpec ==
  /\ ConfigurationRegressionInit
  /\ [][ConfigurationRegressionNext]_vars
  /\ WF_vars(ConfigurationRegressionNext)

ConfigurationRegressionInvariant ==
  /\ EffectiveConfiguration(FixtureA).configIndex = 2
  /\ EffectiveConfiguration(FixtureA).config = RegressionStableConfig
  /\ EffectiveConfiguration(FixtureB).configIndex = 1
  /\ EffectiveConfiguration(FixtureB).config = ApplicationConfig
  /\ EffectiveConfiguration(FixtureC).configIndex = 0
  /\ EffectiveConfiguration(FixtureC).config = StableMembership(Nodes)
  /\ CASE AppliedThrough(FixtureA) >= 2 ->
            /\ AppliedConfiguration(FixtureA).configIndex = 2
            /\ AppliedConfiguration(FixtureA).config = RegressionStableConfig
       [] AppliedThrough(FixtureA) = 1 ->
            /\ AppliedConfiguration(FixtureA).configIndex = 1
            /\ AppliedConfiguration(FixtureA).config = ApplicationConfig
       [] OTHER ->
            /\ AppliedConfiguration(FixtureA).configIndex = 0
            /\ AppliedConfiguration(FixtureA).config = StableMembership(Nodes)
  /\ IF AppliedThrough(FixtureB) = 1
     THEN /\ AppliedConfiguration(FixtureB).configIndex = 1
          /\ AppliedConfiguration(FixtureB).config = ApplicationConfig
     ELSE /\ AppliedConfiguration(FixtureB).configIndex = 0
          /\ AppliedConfiguration(FixtureB).config = StableMembership(Nodes)
  /\ AppliedConfiguration(FixtureC).configIndex = 0
  /\ AppliedConfiguration(FixtureC).config = StableMembership(Nodes)

ConfigurationRegressionComplete ==
  /\ AppliedThrough(FixtureA) = 2
  /\ AppliedThrough(FixtureB) = 1

ConfigurationRegressionCompletes == <>ConfigurationRegressionComplete

JointQuorumNewVoters == {FixtureA, FixtureB}

JointQuorumRegressionInit ==
  /\ FixtureConstantsOK
  /\ currentTerm = BaseTerm
  /\ votedFor = [n \in Nodes |-> FixtureA]
  /\ role = [n \in Nodes |-> IF n = FixtureA THEN Leader ELSE Follower]
  /\ log = BaseLog
  /\ commitIndex = BaseCommit
  /\ BaseExtendedState
  /\ messages = {}
  /\ readRequests = {}
  /\ readBarrierViolationSeen = FALSE
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = StableMembership(Nodes)
  /\ effectiveConfigIndex = 0
  /\ electedLeaders = [t \in 1..MaxTerm |->
       IF t = 1 THEN {FixtureA} ELSE {}]
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE
  /\ lastAppendAccepted = FALSE

JointQuorumRegressionNext ==
  \/ /\ Len(log[FixtureA]) = 0
     /\ EnterJoint(FixtureA, JointQuorumNewVoters)
  \/ /\ Len(log[FixtureA]) = 1
     /\ Len(log[FixtureB]) = 0
     /\ Len(log[FixtureC]) = 0
     /\ messages = {}
     /\ SendAppend(FixtureA, FixtureC)
  \/ \E m \in messages :
       /\ m.type = AppendEntries
       /\ m.to = FixtureC
       /\ DeliverAppend(m)
  \/ /\ Len(log[FixtureA]) = 1
     /\ Len(log[FixtureB]) = 0
     /\ Len(log[FixtureC]) = 1
     /\ messages = {}
     /\ (Commit(FixtureA, 1) \/ SendAppend(FixtureA, FixtureB))
  \/ \E m \in messages :
       /\ m.type = AppendEntries
       /\ m.to = FixtureB
       /\ DeliverAppend(m)
  \/ /\ \A n \in Nodes : Len(log[n]) = 1
     /\ commitIndex[FixtureA] = 0
     /\ messages = {}
     /\ Commit(FixtureA, 1)
  \/ /\ commitIndex[FixtureA] = 1
     /\ AppliedThrough(FixtureA) = 0
     /\ Apply(FixtureA)
  \/ /\ AppliedThrough(FixtureA) = 1
     /\ UNCHANGED vars

JointQuorumRegressionSpec ==
  /\ JointQuorumRegressionInit
  /\ [][JointQuorumRegressionNext]_vars
  /\ WF_vars(JointQuorumRegressionNext)

JointQuorumOldSideCannotCommit ==
  ( /\ Len(log[FixtureA]) = 1
    /\ Len(log[FixtureB]) = 0
    /\ Len(log[FixtureC]) = 1 ) =>
    commitIndex[FixtureA] = 0

JointQuorumRegressionComplete == AppliedThrough(FixtureA) = 1

JointQuorumRegressionCompletes == <>JointQuorumRegressionComplete

EffectiveOverwriteRegressionInit == JointQuorumRegressionInit

PrepareEffectiveOverwriteLeader ==
  /\ Len(log[FixtureA]) = 1
  /\ log[FixtureA][1].kind = ConfigurationEntryKind
  /\ Len(log[FixtureB]) = 0
  /\ currentTerm' = [currentTerm EXCEPT ![FixtureB] = 2]
  /\ votedFor' = [n \in Nodes |-> FixtureB]
  /\ role' = [n \in Nodes |-> IF n = FixtureB THEN Leader ELSE Follower]
  /\ log' = [log EXCEPT ![FixtureB] = <<Entry(2, FixtureValueA)>>]
  /\ electedLeaders' = [electedLeaders EXCEPT ![2] = @ \cup {FixtureB}]
  /\ RecordLogicalPrefixes(log', snapshotIndex, snapshotPrefix)
  /\ UNCHANGED <<commitIndex, messages, readRequests,
                  readBarrierViolationSeen, committedLedger, commitWitnesses>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED authorityVars

EffectiveOverwriteRegressionNext ==
  \/ /\ Len(log[FixtureA]) = 0
     /\ EnterJoint(FixtureA, JointQuorumNewVoters)
  \/ /\ role[FixtureA] = Leader
     /\ Len(log[FixtureA]) = 1
     /\ Len(log[FixtureB]) = 0
     /\ PrepareEffectiveOverwriteLeader
  \/ /\ role[FixtureB] = Leader
     /\ Len(log[FixtureB]) = 1
     /\ log[FixtureB][1].kind = CommandEntryKind
     /\ log[FixtureA][1].kind = ConfigurationEntryKind
     /\ messages = {}
     /\ SendAppend(FixtureB, FixtureA)
  \/ \E m \in messages :
       /\ m.type = AppendEntries
       /\ m.from = FixtureB
       /\ m.to = FixtureA
       /\ DeliverAppend(m)
  \/ /\ Len(log[FixtureA]) = 1
     /\ log[FixtureA][1].kind = CommandEntryKind
     /\ effectiveConfigIndex = 0
     /\ UNCHANGED vars

EffectiveOverwriteRegressionSpec ==
  /\ EffectiveOverwriteRegressionInit
  /\ [][EffectiveOverwriteRegressionNext]_vars
  /\ WF_vars(EffectiveOverwriteRegressionNext)

EffectiveOverwriteRegressionInvariant ==
  CASE Len(log[FixtureA]) = 0 ->
         /\ EffectiveConfiguration(FixtureA).configIndex = 0
         /\ EffectiveConfiguration(FixtureA).config = StableMembership(Nodes)
    [] log[FixtureA][1].kind = ConfigurationEntryKind ->
         /\ EffectiveConfiguration(FixtureA).configIndex = 1
         /\ EffectiveConfiguration(FixtureA).config = ApplicationConfig
    [] OTHER ->
         /\ EffectiveConfiguration(FixtureA).configIndex = 0
         /\ EffectiveConfiguration(FixtureA).config = StableMembership(Nodes)

EffectiveOverwriteRegressionComplete ==
  /\ Len(log[FixtureA]) = 1
  /\ log[FixtureA][1].kind = CommandEntryKind
  /\ EffectiveConfiguration(FixtureA).configIndex = 0

EffectiveOverwriteRegressionCompletes ==
  <>EffectiveOverwriteRegressionComplete

DelayedHeartbeatRegressionInit == JointQuorumRegressionInit

DelayedHeartbeatRegressionNext ==
  \/ /\ Len(log[FixtureA]) = 0
     /\ messages = {}
     /\ SendAppend(FixtureA, FixtureB)
  \/ /\ Len(log[FixtureA]) = 0
     /\ \E m \in messages :
          /\ m.to = FixtureB
          /\ Len(m.entries) = 0
     /\ EnterJoint(FixtureA, JointQuorumNewVoters)
  \/ /\ Len(log[FixtureA]) = 1
     /\ Len(log[FixtureC]) = 0
     /\ \E m \in messages :
          /\ m.to = FixtureB
          /\ Len(m.entries) = 0
     /\ SendAppend(FixtureA, FixtureC)
  \/ \E m \in messages :
       /\ m.to = FixtureC
       /\ Len(m.entries) = 1
       /\ DeliverAppend(m)
  \/ \E m \in messages :
       /\ Len(log[FixtureC]) = 1
       /\ m.to = FixtureB
       /\ Len(m.entries) = 0
       /\ DeliverAppend(m)
  \/ /\ Len(log[FixtureA]) = 1
     /\ Len(log[FixtureB]) = 0
     /\ Len(log[FixtureC]) = 1
     /\ messages = {}
     /\ (Commit(FixtureA, 1) \/ SendAppend(FixtureA, FixtureB))
  \/ \E m \in messages :
       /\ m.to = FixtureB
       /\ Len(m.entries) = 1
       /\ DeliverAppend(m)
  \/ /\ \A n \in Nodes : Len(log[n]) = 1
     /\ commitIndex[FixtureA] = 0
     /\ messages = {}
     /\ Commit(FixtureA, 1)
  \/ /\ commitIndex[FixtureA] = 1
     /\ AppliedThrough(FixtureA) = 0
     /\ Apply(FixtureA)
  \/ /\ AppliedThrough(FixtureA) = 1
     /\ UNCHANGED vars

DelayedHeartbeatRegressionSpec ==
  /\ DelayedHeartbeatRegressionInit
  /\ [][DelayedHeartbeatRegressionNext]_vars
  /\ WF_vars(DelayedHeartbeatRegressionNext)

DelayedHeartbeatRegressionInvariant ==
  /\ \A n \in Nodes :
       IF Len(log[n]) = 0
       THEN /\ EffectiveConfiguration(n).configIndex = 0
            /\ EffectiveConfiguration(n).config = StableMembership(Nodes)
       ELSE /\ EffectiveConfiguration(n).configIndex = 1
            /\ EffectiveConfiguration(n).config = ApplicationConfig
  /\ ( /\ Len(log[FixtureA]) = 1
       /\ Len(log[FixtureB]) = 0
       /\ Len(log[FixtureC]) = 1
       /\ messages = {} ) =>
       commitIndex[FixtureA] = 0

DelayedHeartbeatRegressionComplete == AppliedThrough(FixtureA) = 1

DelayedHeartbeatRegressionCompletes ==
  <>DelayedHeartbeatRegressionComplete

====
