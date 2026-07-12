---- MODULE RafterInvariantDetectorNegative ----
EXTENDS Raft

CONSTANTS TargetPredicate, FixtureA, FixtureB, FixtureC,
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

FixtureConstantsOK ==
  /\ Nodes = {FixtureA, FixtureB, FixtureC}
  /\ FixtureValueA \in Values
  /\ FixtureValueB \in Values
  /\ FixtureValueA # FixtureValueB
  /\ FixtureRead \in ReadRequests
  /\ MaxTerm >= 2
  /\ MaxLogLen >= 2

BaseTerm == [n \in Nodes |-> 1]
BaseVote == [n \in Nodes |-> NoVote]
BaseRole == [n \in Nodes |-> Follower]
BaseLog == [n \in Nodes |-> <<>>]
BaseCommit == [n \in Nodes |-> 0]
BaseApplied == [n \in Nodes |-> <<>>]

DivergentLogs ==
  [n \in Nodes |->
    IF n = FixtureA THEN <<Entry(1, FixtureValueA)>>
    ELSE IF n = FixtureB THEN <<Entry(1, FixtureValueB)>>
    ELSE <<>>]

TargetTerm ==
  IF TargetPredicate = "LeaderCompleteness"
  THEN [n \in Nodes |-> IF n = FixtureB THEN 2 ELSE 1]
  ELSE BaseTerm

TargetRole ==
  CASE TargetPredicate = "ElectionSafety" ->
         [n \in Nodes |->
           IF n \in {FixtureA, FixtureB} THEN Leader ELSE Follower]
    [] TargetPredicate = "LeaderCompleteness" ->
         [n \in Nodes |-> IF n = FixtureB THEN Leader ELSE Follower]
    [] TargetPredicate = "StaleLeaderFencing" ->
         [n \in Nodes |-> IF n = FixtureA THEN Leader ELSE Follower]
    [] OTHER -> BaseRole

TargetLog ==
  CASE TargetPredicate = "LogMatching" ->
         [n \in Nodes |->
           IF n = FixtureA
           THEN <<Entry(1, FixtureValueA), Entry(2, FixtureValueA)>>
           ELSE IF n = FixtureB
           THEN <<Entry(1, FixtureValueB), Entry(2, FixtureValueB)>>
           ELSE <<>>]
    [] TargetPredicate \in {
           "CommittedPrefixStability", "StateMachineSafety"} -> DivergentLogs
    [] TargetPredicate \in {
           "LeaderCompleteness", "StaleLeaderFencing",
           "CommittedEntriesHaveQuorum"} ->
         [n \in Nodes |->
           IF n = FixtureA THEN <<Entry(1, FixtureValueA)>> ELSE <<>>]
    [] OTHER -> BaseLog

TargetCommit ==
  IF TargetPredicate \in {
       "LeaderCompleteness", "StaleLeaderFencing",
       "CommittedEntriesHaveQuorum"}
  THEN [n \in Nodes |-> IF n = FixtureA THEN 1 ELSE 0]
  ELSE IF TargetPredicate \in {
       "CommittedPrefixStability", "StateMachineSafety"}
  THEN [n \in Nodes |-> IF n \in {FixtureA, FixtureB} THEN 1 ELSE 0]
  ELSE BaseCommit

TargetApplied ==
  IF TargetPredicate = "StateMachineSafety"
  THEN [n \in Nodes |->
          IF n = FixtureA THEN <<FixtureValueA>>
          ELSE IF n = FixtureB THEN <<FixtureValueB>>
          ELSE <<>>]
  ELSE BaseApplied

TargetReadGrants ==
  IF TargetPredicate = "ReadBarrierLinearizability"
  THEN {[node |-> FixtureA, request |-> FixtureRead, readIndex |-> 0]}
  ELSE {}

FixtureInit ==
  /\ TargetPredicate \in PredicateNames
  /\ FixtureConstantsOK
  /\ currentTerm = BaseTerm
  /\ votedFor = BaseVote
  /\ role = BaseRole
  /\ log = BaseLog
  /\ commitIndex = BaseCommit
  /\ applied = BaseApplied
  /\ messages = {}
  /\ readRequests = {}
  /\ readGrants = {}
  /\ membership = StableMembership(Nodes)

FixtureNext ==
  /\ currentTerm' = TargetTerm
  /\ votedFor' = BaseVote
  /\ role' = TargetRole
  /\ log' = TargetLog
  /\ commitIndex' = TargetCommit
  /\ applied' = TargetApplied
  /\ messages' = {}
  /\ readRequests' = {}
  /\ readGrants' = TargetReadGrants
  /\ membership' = StableMembership(Nodes)

FixtureSpec == FixtureInit /\ [][FixtureNext]_vars

====
