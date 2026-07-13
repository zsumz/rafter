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
BaseApplied == [n \in Nodes |-> <<>>]
BaseSnapshotIndex == [n \in Nodes |-> 0]
BaseSnapshotPrefix == [n \in Nodes |-> <<>>]
BaseCompactedIndex == [n \in Nodes |-> 0]
BaseApplicationEpoch == [n \in Nodes |-> 0]
BaseEpochIndex == [n \in Nodes |-> 0]
BaseApplicationState == [n \in Nodes |-> InitialApplicationState]
BaseAppliedThrough == [n \in Nodes |-> 0]

ApplicationConfig ==
  JointMembership(Nodes, {FixtureA, FixtureB})

CorruptedApplicationConfig ==
  JointMembership(Nodes, {FixtureA, FixtureC})

RegressionStableConfig == StableMembership({FixtureA, FixtureB})

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

InitialCompactedIndex ==
  IF IsMode("LogMatching", SnapshotPrefixRecorderMode)
  THEN [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
  ELSE BaseCompactedIndex

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
  THEN {[index |-> 1, entry |-> Entry(1, FixtureValueA)]}
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
  /\ compactedIndex = InitialCompactedIndex
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = BaseApplied
  /\ applicationEpoch = BaseApplicationEpoch
  /\ epochBaseIndex = BaseEpochIndex
  /\ epochBaseState = BaseApplicationState
  /\ applicationState = BaseApplicationState
  /\ appliedThrough = BaseAppliedThrough
  /\ messages = {}
  /\ readRequests = InitialReadRequests
  /\ readGrants = {}
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = InitialEffectiveMembership
  /\ effectiveConfigIndex = InitialEffectiveConfigIndex
  /\ electedLeaders = InitialElectedLeaders
  /\ logicalPrefixLedger = {}
  /\ committedLedger = InitialCommittedLedger
  /\ commitWitnesses = {}
  /\ higherTermEvidenceSeen = FALSE
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE

ElectionFirstLeader ==
  /\ role[FixtureA] = Candidate
  /\ electedLeaders[1] = {}
  /\ BecomeLeader(FixtureA)

PrepareSequentialCandidate ==
  /\ role[FixtureA] = Leader
  /\ votedFor' = [n \in Nodes |-> FixtureB]
  /\ role' = [n \in Nodes |-> IF n = FixtureB THEN Candidate ELSE Follower]
  /\ UNCHANGED <<currentTerm, log, commitIndex, messages,
                  readRequests, readGrants, membership, appliedConfigIndex,
                  effectiveMembership, effectiveConfigIndex,
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
                  readRequests, readGrants, membership, appliedConfigIndex,
                  effectiveMembership, effectiveConfigIndex,
                  higherTermEvidenceSeen, higherTermStepDownFailed>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars

FaultyHigherTermAndAuthority ==
  /\ ~higherTermEvidenceSeen
  /\ ~higherTermStepDownFailed
  /\ ~staleAuthorityAccepted
  /\ currentTerm' = [currentTerm EXCEPT ![FixtureA] = 2]
  /\ votedFor' = [votedFor EXCEPT ![FixtureA] = NoVote]
  /\ role' = [role EXCEPT ![FixtureA] = Candidate]
  /\ RecordHigherTermOutcome(FixtureA, 2, TRUE)
  /\ RecordAuthorityAcceptance(
       1, 2, FixtureMode # HigherTermRecorderMode)
  /\ UNCHANGED <<log, commitIndex, messages, readRequests, readGrants,
                  membership, appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars

FaultyStaleAuthorityOnly ==
  /\ ~higherTermEvidenceSeen
  /\ ~higherTermStepDownFailed
  /\ ~staleAuthorityAccepted
  /\ RecordHigherTermOutcome(FixtureA, currentTerm[FixtureA], FALSE)
  /\ RecordAuthorityAcceptance(1, currentTerm[FixtureA], TRUE)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars

ApplicationFirstApply ==
  /\ Len(applied[FixtureA]) = 0
  /\ Len(applied[FixtureB]) = 0
  /\ Apply(FixtureA)

FaultyApplicationResult ==
  LET entry == log[FixtureB][1]
      priorState == InitialApplicationState
      corruptedResult ==
        [referenceState |-> <<>>, membership |-> CorruptedApplicationConfig]
  IN
    /\ Len(applied[FixtureA]) = 1
    /\ Len(applied[FixtureB]) = 0
    /\ RecordApplication(FixtureB, 1, entry, priorState, corruptedResult)
    /\ applicationState' =
         [applicationState EXCEPT ![FixtureB] = corruptedResult]
    /\ appliedThrough' = [appliedThrough EXCEPT ![FixtureB] = 1]
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    messages, readRequests, readGrants, membership,
                    appliedConfigIndex, effectiveMembership,
                    effectiveConfigIndex, electedLeaders,
                    applicationEpoch, epochBaseIndex, epochBaseState>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED historyVars
    /\ UNCHANGED authorityVars

ApplicationEpochFirstApply ==
  /\ applicationEpoch[FixtureA] = 0
  /\ appliedThrough[FixtureA] = 0
  /\ Apply(FixtureA)

ApplicationEpochLoss ==
  /\ applicationEpoch[FixtureA] = 0
  /\ appliedThrough[FixtureA] = 1
  /\ ApplicationStateLoss(FixtureA)

FaultyApplicationEpochReplay ==
  LET entry == Entry(1, FixtureValueB)
      priorState == InitialApplicationState
      resultState == ApplyEntry(priorState, entry)
  IN
    /\ applicationEpoch[FixtureA] = 1
    /\ appliedThrough[FixtureA] = 0
    /\ RecordApplication(FixtureA, 1, entry, priorState, resultState)
    /\ applicationState' =
         [applicationState EXCEPT ![FixtureA] = resultState]
    /\ appliedThrough' = [appliedThrough EXCEPT ![FixtureA] = 1]
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    messages, readRequests, readGrants, membership,
                    appliedConfigIndex, effectiveMembership,
                    effectiveConfigIndex, electedLeaders,
                    applicationEpoch, epochBaseIndex, epochBaseState>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED historyVars
    /\ UNCHANGED authorityVars

FaultyLogMatchingRecorder ==
  /\ logicalPrefixLedger = {}
  /\ RecordLogicalPrefixes(
       DivergentLogs, BaseSnapshotIndex, BaseSnapshotPrefix)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders,
                  committedLedger, commitWitnesses>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED authorityVars

ObserveSnapshotSource ==
  /\ logicalPrefixLedger = {}
  /\ RecordLogicalPrefixes(log, snapshotIndex, snapshotPrefix)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders,
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
                  snapshotIndex, snapshotPrefix, compactedIndex,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders>>
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars
  /\ UNCHANGED authorityVars

FaultyLeaderCompletenessRecorder ==
  /\ committedLedger = {}
  /\ RecordCommittedEntries(
       SingleAEntryLogs, BaseSnapshotIndex, BaseSnapshotPrefix,
       FixtureA, 0, 1)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders,
                  logicalPrefixLedger, commitWitnesses>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED authorityVars

FaultyCommittedPrefixRecorder ==
  /\ Cardinality(committedLedger) = 1
  /\ RecordCommittedEntries(
       SingleBEntryLogs, BaseSnapshotIndex, BaseSnapshotPrefix,
       FixtureB, 0, 1)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders,
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
    /\ commitWitnesses = {}
    /\ RecordCommitWitnesses({witness})
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    messages, readRequests, readGrants, membership,
                    appliedConfigIndex, effectiveMembership,
                    effectiveConfigIndex, electedLeaders,
                    logicalPrefixLedger, committedLedger>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED authorityVars

FaultyReadBarrierRecorder ==
  LET grant ==
        [node |-> FixtureA, request |-> FixtureRead, readIndex |-> 0]
  IN
    /\ readGrants = {}
    /\ RecordReadGrant(grant)
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    messages, readRequests, membership,
                    appliedConfigIndex, effectiveMembership,
                    effectiveConfigIndex, electedLeaders>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED historyVars
    /\ UNCHANGED authorityVars

LegacyTargetTerm ==
  IF TargetPredicate = "LeaderCompleteness"
  THEN [n \in Nodes |-> IF n = FixtureB THEN 2 ELSE 1]
  ELSE BaseTerm

LegacyTargetRole ==
  IF TargetPredicate = "LeaderCompleteness"
  THEN [n \in Nodes |-> IF n = FixtureB THEN Leader ELSE Follower]
  ELSE BaseRole

LegacyTargetLog ==
  CASE TargetPredicate = "LogMatching" ->
         [n \in Nodes |->
           IF n = FixtureA
           THEN <<Entry(1, FixtureValueA), Entry(2, FixtureValueA)>>
           ELSE IF n = FixtureB
           THEN <<Entry(1, FixtureValueB), Entry(2, FixtureValueB)>>
           ELSE <<>>]
    [] TargetPredicate = "CommittedPrefixStability" -> DivergentLogs
    [] TargetPredicate \in {
           "LeaderCompleteness", "CommittedEntriesHaveQuorum"} ->
         SingleAEntryLogs
    [] OTHER -> BaseLog

LegacyTargetCommit ==
  IF TargetPredicate \in {
       "LeaderCompleteness", "CommittedEntriesHaveQuorum"}
  THEN [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
  ELSE IF TargetPredicate = "CommittedPrefixStability"
  THEN [n \in Nodes |-> IF n \in {FixtureA, FixtureB} THEN 1 ELSE 0]
  ELSE BaseCommit

LegacyTargetCommittedLedger ==
  CASE TargetPredicate = "LeaderCompleteness" ->
         {[index |-> 1, entry |-> Entry(1, FixtureValueA)]}
    [] TargetPredicate = "CommittedPrefixStability" ->
         {[index |-> 1, entry |-> Entry(1, FixtureValueA)],
          [index |-> 1, entry |-> Entry(1, FixtureValueB)]}
    [] TargetPredicate = "CommittedEntriesHaveQuorum" ->
         {[index |-> 1, entry |-> Entry(1, FixtureValueA)]}
    [] OTHER -> {}

LegacyTargetCommitWitnesses ==
  IF TargetPredicate = "CommittedEntriesHaveQuorum"
  THEN {[index |-> 1,
         entry |-> Entry(1, FixtureValueA),
         leader |-> FixtureA,
         leaderRole |-> Leader,
         leaderTerm |-> 1,
         membership |-> StableMembership(Nodes),
         authorityMembership |-> StableMembership(Nodes),
         derivedMembership |-> StableMembership(Nodes),
         configIndex |-> 0,
         replicas |-> {FixtureA}]}
  ELSE {}

LegacyTargetElectedLeaders ==
  [t \in 1..MaxTerm |->
    IF TargetPredicate = "LeaderCompleteness" /\ t = 2
    THEN {FixtureB}
    ELSE {}]

LegacyTargetReadGrants ==
  IF TargetPredicate = "ReadBarrierLinearizability"
  THEN {[node |-> FixtureA, request |-> FixtureRead, readIndex |-> 0]}
  ELSE {}

LegacyViolation ==
  /\ currentTerm' = LegacyTargetTerm
  /\ votedFor' = BaseVote
  /\ role' = LegacyTargetRole
  /\ log' = LegacyTargetLog
  /\ commitIndex' = LegacyTargetCommit
  /\ snapshotIndex' = BaseSnapshotIndex
  /\ snapshotPrefix' = BaseSnapshotPrefix
  /\ compactedIndex' = BaseCompactedIndex
  /\ snapshotTransfer' = NoSnapshotTransfer
  /\ applied' = BaseApplied
  /\ applicationEpoch' = BaseApplicationEpoch
  /\ epochBaseIndex' = BaseEpochIndex
  /\ epochBaseState' = BaseApplicationState
  /\ applicationState' = BaseApplicationState
  /\ appliedThrough' = BaseAppliedThrough
  /\ messages' = {}
  /\ readRequests' = {}
  /\ readGrants' = LegacyTargetReadGrants
  /\ membership' = StableMembership(Nodes)
  /\ appliedConfigIndex' = 0
  /\ effectiveMembership' = StableMembership(Nodes)
  /\ effectiveConfigIndex' = 0
  /\ electedLeaders' = LegacyTargetElectedLeaders
  /\ logicalPrefixLedger' = {}
  /\ committedLedger' = LegacyTargetCommittedLedger
  /\ commitWitnesses' = LegacyTargetCommitWitnesses
  /\ higherTermEvidenceSeen' = FALSE
  /\ higherTermStepDownFailed' = FALSE
  /\ staleAuthorityAccepted' = FALSE

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
  \/ /\ FixtureMode = DefaultFixtureMode
     /\ TargetPredicate \notin {
          "ElectionSafety", "StaleLeaderFencing", "StateMachineSafety"}
     /\ LegacyViolation

FixtureSpec == FixtureInit /\ [][FixtureNext]_vars

BaseExtendedState ==
  /\ snapshotIndex = BaseSnapshotIndex
  /\ snapshotPrefix = BaseSnapshotPrefix
  /\ compactedIndex = BaseCompactedIndex
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = BaseApplied
  /\ applicationEpoch = BaseApplicationEpoch
  /\ epochBaseIndex = BaseEpochIndex
  /\ epochBaseState = BaseApplicationState
  /\ applicationState = BaseApplicationState
  /\ appliedThrough = BaseAppliedThrough
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {}
  /\ commitWitnesses = {}

SnapshotLifecycleEntry == Entry(1, FixtureValueA)

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
  /\ compactedIndex = BaseCompactedIndex
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = [n \in Nodes |->
       IF n = FixtureA
       THEN <<AppliedEvent(
         0, 0, InitialApplicationState, 1, SnapshotLifecycleEntry,
         InitialApplicationState, SnapshotLifecycleResult)>>
       ELSE <<>>]
  /\ applicationEpoch = BaseApplicationEpoch
  /\ epochBaseIndex = BaseEpochIndex
  /\ epochBaseState = BaseApplicationState
  /\ applicationState = [n \in Nodes |->
       IF n = FixtureA THEN SnapshotLifecycleResult ELSE InitialApplicationState]
  /\ appliedThrough =
       [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
  /\ messages = {}
  /\ readRequests = {}
  /\ readGrants = {}
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = StableMembership(Nodes)
  /\ effectiveConfigIndex = 0
  /\ electedLeaders = [t \in 1..MaxTerm |->
       IF t = 1 THEN {FixtureA} ELSE {}]
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {[index |-> 1, entry |-> SnapshotLifecycleEntry]}
  /\ commitWitnesses = {SnapshotLifecycleWitness}
  /\ higherTermEvidenceSeen = FALSE
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE

SnapshotLifecycleNext ==
  \/ /\ snapshotIndex[FixtureA] = 0
     /\ CreateSnapshot(FixtureA)
  \/ /\ snapshotIndex[FixtureA] = 1
     /\ compactedIndex[FixtureA] = 0
     /\ CompactSnapshot(FixtureA)
  \/ /\ compactedIndex[FixtureA] = 1
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
       log, snapshotIndex, snapshotPrefix, compactedIndex)
  /\ LogMatching
  /\ LeaderCompleteness
  /\ CommittedPrefixStability
  /\ StateMachineSafety
  /\ CommittedEntriesHaveQuorum

SnapshotLifecycleComplete ==
  /\ snapshotIndex[FixtureB] = 1
  /\ compactedIndex[FixtureB] = 1
  /\ applicationEpoch[FixtureB] = 1
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
  /\ compactedIndex = BaseCompactedIndex
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = BaseApplied
  /\ applicationEpoch = BaseApplicationEpoch
  /\ epochBaseIndex = BaseEpochIndex
  /\ epochBaseState = BaseApplicationState
  /\ applicationState = BaseApplicationState
  /\ appliedThrough = BaseAppliedThrough
  /\ messages = {}
  /\ readRequests = {}
  /\ readGrants = {}
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = StableMembership(Nodes)
  /\ effectiveConfigIndex = 0
  /\ electedLeaders = [t \in 1..MaxTerm |->
       IF t = 1 THEN {FixtureA} ELSE {}]
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {[index |-> 1, entry |-> SnapshotLifecycleEntry]}
  /\ commitWitnesses = {SnapshotLifecycleWitness}
  /\ higherTermEvidenceSeen = FALSE
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE

ApplicationEpochLifecycleNext ==
  \/ /\ applicationEpoch[FixtureA] = 0
     /\ appliedThrough[FixtureA] = 0
     /\ Apply(FixtureA)
  \/ /\ applicationEpoch[FixtureA] = 0
     /\ appliedThrough[FixtureA] = 1
     /\ ApplicationStateLoss(FixtureA)
  \/ /\ applicationEpoch[FixtureA] = 1
     /\ appliedThrough[FixtureA] = 0
     /\ Apply(FixtureA)
  \/ /\ applicationEpoch[FixtureA] = 1
     /\ appliedThrough[FixtureA] = 1
     /\ UNCHANGED vars

ApplicationEpochLifecycleSpec ==
  /\ ApplicationEpochLifecycleInit
  /\ [][ApplicationEpochLifecycleNext]_vars
  /\ WF_vars(ApplicationEpochLifecycleNext)

ApplicationEpochLifecycleInvariant ==
  /\ StateMachineSafety
  /\ CommittedEntriesHaveQuorum

ApplicationEpochLifecycleComplete ==
  /\ applicationEpoch[FixtureA] = 1
  /\ appliedThrough[FixtureA] = 1
  /\ Len(applied[FixtureA]) = 2

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
  /\ compactedIndex = BaseCompactedIndex
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = [n \in Nodes |->
       IF n = FixtureA
       THEN <<AppliedEvent(
         0, 0, InitialApplicationState, 1, SelfRemovalJointEntry,
         InitialApplicationState, SelfRemovalJointState)>>
       ELSE <<>>]
  /\ applicationEpoch = BaseApplicationEpoch
  /\ epochBaseIndex = BaseEpochIndex
  /\ epochBaseState = BaseApplicationState
  /\ applicationState = [n \in Nodes |->
       IF n = FixtureA THEN SelfRemovalJointState ELSE InitialApplicationState]
  /\ appliedThrough = [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
  /\ messages = {}
  /\ readRequests = {}
  /\ readGrants = {}
  /\ membership = SelfRemovalJointMembership
  /\ appliedConfigIndex = 1
  /\ effectiveMembership = SelfRemovalStableMembership
  /\ effectiveConfigIndex = 2
  /\ electedLeaders = [t \in 1..MaxTerm |->
       IF t = 1 THEN {FixtureA} ELSE {}]
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {[index |-> 1, entry |-> SelfRemovalJointEntry]}
  /\ commitWitnesses = {SelfRemovalJointWitness}
  /\ higherTermEvidenceSeen = FALSE
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE

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
  /\ readGrants = {}
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = RegressionStableConfig
  /\ effectiveConfigIndex = 2
  /\ electedLeaders = [t \in 1..MaxTerm |-> {}]
  /\ higherTermEvidenceSeen = FALSE
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE

ConfigurationRegressionNext ==
  \/ /\ appliedThrough[FixtureA] = 0
     /\ Apply(FixtureA)
  \/ /\ appliedThrough[FixtureA] = 1
     /\ Apply(FixtureA)
  \/ /\ appliedThrough[FixtureA] = 2
     /\ appliedThrough[FixtureB] = 0
     /\ Apply(FixtureB)
  \/ /\ appliedThrough[FixtureA] = 2
     /\ appliedThrough[FixtureB] = 1
     /\ UNCHANGED vars

ConfigurationRegressionSpec ==
  /\ ConfigurationRegressionInit
  /\ [][ConfigurationRegressionNext]_vars
  /\ WF_vars(ConfigurationRegressionNext)

ConfigurationRegressionInvariant ==
  /\ effectiveConfigIndex = 2
  /\ effectiveMembership = RegressionStableConfig
  /\ CASE appliedThrough[FixtureA] >= 2 ->
            /\ appliedConfigIndex = 2
            /\ membership = RegressionStableConfig
       [] appliedThrough[FixtureA] = 1 ->
            /\ appliedConfigIndex = 1
            /\ membership = ApplicationConfig
       [] OTHER ->
            /\ appliedConfigIndex = 0
            /\ membership = StableMembership(Nodes)

ConfigurationRegressionComplete ==
  /\ appliedThrough[FixtureA] = 2
  /\ appliedThrough[FixtureB] = 1

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
  /\ readGrants = {}
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = StableMembership(Nodes)
  /\ effectiveConfigIndex = 0
  /\ electedLeaders = [t \in 1..MaxTerm |->
       IF t = 1 THEN {FixtureA} ELSE {}]
  /\ higherTermEvidenceSeen = FALSE
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE

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
     /\ appliedThrough[FixtureA] = 0
     /\ Apply(FixtureA)
  \/ /\ appliedThrough[FixtureA] = 1
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

JointQuorumRegressionComplete == appliedThrough[FixtureA] = 1

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
  /\ UNCHANGED <<commitIndex, messages, readRequests, readGrants,
                  membership, appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, committedLedger, commitWitnesses>>
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
         /\ effectiveConfigIndex = 0
         /\ effectiveMembership = StableMembership(Nodes)
    [] log[FixtureA][1].kind = ConfigurationEntryKind ->
         /\ effectiveConfigIndex = 1
         /\ effectiveMembership = ApplicationConfig
    [] OTHER ->
         /\ effectiveConfigIndex = 0
         /\ effectiveMembership = StableMembership(Nodes)

EffectiveOverwriteRegressionComplete ==
  /\ Len(log[FixtureA]) = 1
  /\ log[FixtureA][1].kind = CommandEntryKind
  /\ effectiveConfigIndex = 0

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
     /\ appliedThrough[FixtureA] = 0
     /\ Apply(FixtureA)
  \/ /\ appliedThrough[FixtureA] = 1
     /\ UNCHANGED vars

DelayedHeartbeatRegressionSpec ==
  /\ DelayedHeartbeatRegressionInit
  /\ [][DelayedHeartbeatRegressionNext]_vars
  /\ WF_vars(DelayedHeartbeatRegressionNext)

DelayedHeartbeatRegressionInvariant ==
  /\ IF Len(log[FixtureA]) = 0
     THEN /\ effectiveConfigIndex = 0
          /\ effectiveMembership = StableMembership(Nodes)
     ELSE /\ effectiveConfigIndex = 1
          /\ effectiveMembership = ApplicationConfig
  /\ ( /\ Len(log[FixtureA]) = 1
       /\ Len(log[FixtureB]) = 0
       /\ Len(log[FixtureC]) = 1
       /\ messages = {} ) =>
       commitIndex[FixtureA] = 0

DelayedHeartbeatRegressionComplete == appliedThrough[FixtureA] = 1

DelayedHeartbeatRegressionCompletes ==
  <>DelayedHeartbeatRegressionComplete

====
