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
HigherTermRecorderMode == "HigherTermRecorderOnly"
StaleAuthorityRecorderMode == "StaleAuthorityRecorderOnly"
ApplicationRecorderMode == "ApplicationRecorderOnly"

FixtureModes == {
    DefaultFixtureMode,
    ElectionRecorderMode,
    HigherTermRecorderMode,
    StaleAuthorityRecorderMode,
    ApplicationRecorderMode
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

ApplicationConfig ==
  JointMembership(Nodes, {FixtureA, FixtureB})

CorruptedApplicationConfig ==
  JointMembership(Nodes, {FixtureA, FixtureC})

InitialTerm ==
  IF TargetPredicate = "StaleLeaderFencing" /\
       FixtureMode = StaleAuthorityRecorderMode
  THEN [n \in Nodes |-> IF n = FixtureA THEN 2 ELSE 1]
  ELSE BaseTerm

InitialVote ==
  IF TargetPredicate = "ElectionSafety"
  THEN [n \in Nodes |-> FixtureA]
  ELSE BaseVote

InitialRole ==
  CASE TargetPredicate = "ElectionSafety" ->
         [n \in Nodes |-> IF n = FixtureA THEN Candidate ELSE Follower]
    [] TargetPredicate = "StaleLeaderFencing" /\
         FixtureMode # StaleAuthorityRecorderMode ->
         [n \in Nodes |-> IF n = FixtureA THEN Leader ELSE Follower]
    [] OTHER -> BaseRole

InitialLog ==
  IF TargetPredicate = "StateMachineSafety"
  THEN [n \in Nodes |-> <<ConfigurationEntry(1, ApplicationConfig)>>]
  ELSE BaseLog

InitialCommit ==
  IF TargetPredicate = "StateMachineSafety"
  THEN [n \in Nodes |-> 1]
  ELSE BaseCommit

InitialElectedLeaders ==
  [t \in 1..MaxTerm |->
    IF TargetPredicate = "StaleLeaderFencing" /\
         FixtureMode # StaleAuthorityRecorderMode /\ t = 1
    THEN {FixtureA}
    ELSE {}]

DivergentLogs ==
  [n \in Nodes |->
    IF n = FixtureA THEN <<Entry(1, FixtureValueA)>>
    ELSE IF n = FixtureB THEN <<Entry(1, FixtureValueB)>>
    ELSE <<>>]

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
         [n \in Nodes |->
           IF n = FixtureA THEN <<Entry(1, FixtureValueA)>> ELSE <<>>]
    [] OTHER -> BaseLog

LegacyTargetCommit ==
  IF TargetPredicate \in {
       "LeaderCompleteness", "CommittedEntriesHaveQuorum"}
  THEN [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
  ELSE IF TargetPredicate = "CommittedPrefixStability"
  THEN [n \in Nodes |-> IF n \in {FixtureA, FixtureB} THEN 1 ELSE 0]
  ELSE BaseCommit

LegacyTargetElectedLeaders ==
  [t \in 1..MaxTerm |->
    IF TargetPredicate = "LeaderCompleteness" /\ t = 2
    THEN {FixtureB}
    ELSE {}]

LegacyTargetReadGrants ==
  IF TargetPredicate = "ReadBarrierLinearizability"
  THEN {[node |-> FixtureA, request |-> FixtureRead, readIndex |-> 0]}
  ELSE {}

FixtureInit ==
  /\ TargetPredicate \in PredicateNames
  /\ FixtureConstantsOK
  /\ currentTerm = InitialTerm
  /\ votedFor = InitialVote
  /\ role = InitialRole
  /\ log = InitialLog
  /\ commitIndex = InitialCommit
  /\ applied = BaseApplied
  /\ messages = {}
  /\ readRequests = {}
  /\ readGrants = {}
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ electedLeaders = InitialElectedLeaders
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
  /\ UNCHANGED << currentTerm, log, commitIndex, applied, messages,
                  readRequests, readGrants, membership, appliedConfigIndex,
                  electedLeaders,
                  higherTermEvidenceSeen, higherTermStepDownFailed,
                  staleAuthorityAccepted >>

FaultySequentialLeader ==
  /\ role[FixtureB] = Candidate
  /\ role' = [n \in Nodes |-> IF n = FixtureB THEN Leader ELSE Follower]
  /\ RecordElection(FixtureB)
  /\ RecordAuthorityAcceptance(1, 1, TRUE)
  /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, applied, messages,
                  readRequests, readGrants, membership, appliedConfigIndex,
                  higherTermEvidenceSeen, higherTermStepDownFailed >>

FaultyHigherTermAndAuthority ==
  /\ ~higherTermEvidenceSeen
  /\ ~higherTermStepDownFailed
  /\ ~staleAuthorityAccepted
  /\ currentTerm' = [currentTerm EXCEPT ![FixtureA] = 2]
  /\ votedFor' = [votedFor EXCEPT ![FixtureA] = NoVote]
  /\ role' = [role EXCEPT ![FixtureA] = Candidate]
  /\ RecordHigherTermOutcome(FixtureA, 2, TRUE)
  /\ RecordAuthorityAcceptance(
       1,
       2,
       FixtureMode # HigherTermRecorderMode)
  /\ UNCHANGED << log, commitIndex, applied, messages, readRequests,
                  readGrants, membership, appliedConfigIndex,
                  electedLeaders >>

FaultyStaleAuthorityOnly ==
  /\ ~higherTermEvidenceSeen
  /\ ~higherTermStepDownFailed
  /\ ~staleAuthorityAccepted
  /\ RecordHigherTermOutcome(FixtureA, currentTerm[FixtureA], FALSE)
  /\ RecordAuthorityAcceptance(1, currentTerm[FixtureA], TRUE)
  /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, applied,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex,
                  electedLeaders >>

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
    /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, messages,
                    readRequests, readGrants, membership,
                    appliedConfigIndex, electedLeaders,
                    higherTermEvidenceSeen, higherTermStepDownFailed,
                    staleAuthorityAccepted >>

LegacyViolation ==
  /\ currentTerm' = LegacyTargetTerm
  /\ votedFor' = BaseVote
  /\ role' = LegacyTargetRole
  /\ log' = LegacyTargetLog
  /\ commitIndex' = LegacyTargetCommit
  /\ applied' = BaseApplied
  /\ messages' = {}
  /\ readRequests' = {}
  /\ readGrants' = LegacyTargetReadGrants
  /\ membership' = StableMembership(Nodes)
  /\ appliedConfigIndex' = 0
  /\ electedLeaders' = LegacyTargetElectedLeaders
  /\ higherTermEvidenceSeen' = FALSE
  /\ higherTermStepDownFailed' = FALSE
  /\ staleAuthorityAccepted' = FALSE

ElectionDone ==
  /\ role[FixtureB] = Leader
  /\ UNCHANGED vars

FencingDone ==
  /\ (higherTermStepDownFailed \/ staleAuthorityAccepted)
  /\ UNCHANGED vars

ApplicationDone ==
  /\ Len(applied[FixtureA]) = 1
  /\ Len(applied[FixtureB]) = 1
  /\ UNCHANGED vars

FixtureNext ==
  \/ /\ TargetPredicate = "ElectionSafety"
     /\ (ElectionFirstLeader
          \/ PrepareSequentialCandidate
          \/ FaultySequentialLeader
          \/ ElectionDone)
  \/ /\ TargetPredicate = "StaleLeaderFencing"
     /\ (IF FixtureMode = StaleAuthorityRecorderMode
          THEN FaultyStaleAuthorityOnly
          ELSE FaultyHigherTermAndAuthority)
  \/ /\ TargetPredicate = "StaleLeaderFencing"
     /\ FencingDone
  \/ /\ TargetPredicate = "StateMachineSafety"
     /\ (ApplicationFirstApply
          \/ FaultyApplicationResult
          \/ ApplicationDone)
  \/ /\ TargetPredicate \notin {
           "ElectionSafety", "StaleLeaderFencing", "StateMachineSafety"}
     /\ LegacyViolation

FixtureSpec == FixtureInit /\ [][FixtureNext]_vars

RegressionStableConfig == StableMembership({FixtureA, FixtureB})

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
  /\ applied = BaseApplied
  /\ messages = {}
  /\ readRequests = {}
  /\ readGrants = {}
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ electedLeaders = [t \in 1..MaxTerm |-> {}]
  /\ higherTermEvidenceSeen = FALSE
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE

ConfigurationRegressionNext ==
  \/ /\ Len(applied[FixtureA]) = 0
     /\ Apply(FixtureA)
  \/ /\ Len(applied[FixtureA]) = 1
     /\ Apply(FixtureA)
  \/ /\ Len(applied[FixtureA]) = 2
     /\ Len(applied[FixtureB]) = 0
     /\ Apply(FixtureB)
  \/ /\ Len(applied[FixtureA]) = 2
     /\ Len(applied[FixtureB]) = 1
     /\ UNCHANGED vars

ConfigurationRegressionSpec ==
  ConfigurationRegressionInit /\ [][ConfigurationRegressionNext]_vars

ConfigurationRegressionInvariant ==
  CASE Len(applied[FixtureA]) >= 2 ->
         /\ appliedConfigIndex = 2
         /\ membership = RegressionStableConfig
    [] Len(applied[FixtureA]) = 1 ->
         /\ appliedConfigIndex = 1
         /\ membership = ApplicationConfig
    [] OTHER ->
         /\ appliedConfigIndex = 0
         /\ membership = StableMembership(Nodes)

ConfigurationRegressionComplete ==
  /\ Len(applied[FixtureA]) = 2
  /\ Len(applied[FixtureB]) = 1

====
