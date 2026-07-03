---- MODULE Raft ----
EXTENDS Naturals, Sequences, FiniteSets, TLC

\* This is a small, bounded design model. It is not implementation code and it
\* intentionally does not import or depend on rafter.

CONSTANTS Nodes, Values, MaxTerm, MaxLogLen, ReadRequests

NodePermutations == Permutations(Nodes)

NoVote == "none"

Follower == "Follower"
Candidate == "Candidate"
Leader == "Leader"

StableConfig == "StableConfig"
JointConfig == "JointConfig"

RequestVote == "RequestVote"
AppendEntries == "AppendEntries"

VARIABLES currentTerm, votedFor, role, log, commitIndex, applied, messages,
          readRequests, readGrants, membership

vars == << currentTerm, votedFor, role, log, commitIndex, applied, messages,
          readRequests, readGrants, membership >>

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b

Entry(term, value) == [term |-> term, value |-> value]

EntrySet == {Entry(t, v) : t \in 1..MaxTerm, v \in Values}

AllLogs ==
  {<<>>}
  \cup {<<e1>> : e1 \in EntrySet}
  \cup {<<e1, e2>> : e1 \in EntrySet, e2 \in EntrySet}
  \cup {<<e1, e2, e3>> : e1 \in EntrySet, e2 \in EntrySet, e3 \in EntrySet}

LogSet == {s \in AllLogs : Len(s) <= MaxLogLen}

AllApplied ==
  {<<>>}
  \cup {<<v1>> : v1 \in Values}
  \cup {<<v1, v2>> : v1 \in Values, v2 \in Values}
  \cup {<<v1, v2, v3>> : v1 \in Values, v2 \in Values, v3 \in Values}

AppliedSet == {s \in AllApplied : Len(s) <= MaxLogLen}

ReadRequestSet ==
  {[node |-> n, request |-> request, committedFloor |-> floor] :
      n \in Nodes,
      request \in ReadRequests,
      floor \in 0..MaxLogLen}

ReadGrantSet ==
  {[node |-> n, request |-> request, readIndex |-> index] :
      n \in Nodes,
      request \in ReadRequests,
      index \in 0..MaxLogLen}

VoterSets ==
  {voters \in SUBSET Nodes :
      /\ voters # {}
      /\ Cardinality(voters) >= Cardinality(Nodes) - 1}

StableMembership(voters) ==
  [phase |-> StableConfig, old |-> voters, new |-> voters]

JointMembership(oldVoters, newVoters) ==
  [phase |-> JointConfig, old |-> oldVoters, new |-> newVoters]

MembershipSet ==
  {StableMembership(voters) : voters \in VoterSets}
  \cup {JointMembership(oldVoters, newVoters) :
      oldVoters \in VoterSets, newVoters \in VoterSets}

RequestVoteMessages ==
  {[type |-> RequestVote, term |-> t, from |-> src, to |-> dst] :
      t \in 1..MaxTerm, src \in Nodes, dst \in Nodes}

AppendEntriesMessages ==
  {[type |-> AppendEntries,
    term |-> t,
    from |-> src,
    to |-> dst,
    entries |-> entries,
    leaderCommit |-> leaderCommit] :
      t \in 1..MaxTerm,
      src \in Nodes,
      dst \in Nodes,
      entries \in LogSet,
      leaderCommit \in 0..MaxLogLen}

MessageSet == RequestVoteMessages \cup AppendEntriesMessages

EntryOK(e) ==
  /\ e.term \in 1..MaxTerm
  /\ e.value \in Values

LogOK(s) == s \in LogSet

AppliedOK(s) == s \in AppliedSet

Prefix(s, i) ==
  IF i = 0 THEN <<>> ELSE SubSeq(s, 1, i)

LastTerm(s) ==
  IF Len(s) = 0 THEN 0 ELSE s[Len(s)].term

UpToDate(candidate, voter) ==
  \/ LastTerm(log[candidate]) > LastTerm(log[voter])
  \/ /\ LastTerm(log[candidate]) = LastTerm(log[voter])
     /\ Len(log[candidate]) >= Len(log[voter])

ActiveVoters(config) ==
  IF config.phase = StableConfig THEN config.old ELSE config.old \cup config.new

StableQuorum(voters, ns) ==
  2 * Cardinality(ns \cap voters) > Cardinality(voters)

MembershipQuorum(config, ns) ==
  IF config.phase = StableConfig
  THEN StableQuorum(config.old, ns)
  ELSE /\ StableQuorum(config.old, ns)
       /\ StableQuorum(config.new, ns)

QuorumNodes(ns) == MembershipQuorum(membership, ns)

MatchingReplicas(n, i) ==
  {r \in Nodes : /\ i \in 1..Len(log[r])
                 /\ i \in 1..Len(log[n])
                 /\ log[r][i] = log[n][i]}

CanAdoptLog(n, entries) ==
  /\ LogOK(entries)
  /\ Len(entries) >= Len(log[n])
  /\ \A i \in 1..commitIndex[n] :
       /\ i \in 1..Len(entries)
       /\ entries[i] = log[n][i]

AdvanceCommit(n, leaderCommit, lastIndex) ==
  Max(commitIndex[n], Min(leaderCommit, lastIndex))

MaxCommittedIndex ==
  CHOOSE floor \in 0..MaxLogLen :
    /\ \E n \in Nodes : commitIndex[n] = floor
    /\ \A n \in Nodes : commitIndex[n] <= floor

CommittedEntriesHeldBy(voters) ==
  \A n \in Nodes :
    \A i \in 1..commitIndex[n] :
      StableQuorum(voters, MatchingReplicas(n, i))

RoleAfterMembershipChange(config) ==
  [n \in Nodes |-> IF n \in ActiveVoters(config) THEN role[n] ELSE Follower]

MessageOK(m) ==
  \/ /\ m.type = RequestVote
     /\ m.term \in 1..MaxTerm
     /\ m.from \in Nodes
     /\ m.to \in Nodes
     /\ m.from # m.to
  \/ /\ m.type = AppendEntries
     /\ m.term \in 1..MaxTerm
     /\ m.from \in Nodes
     /\ m.to \in Nodes
     /\ m.from # m.to
     /\ LogOK(m.entries)
     /\ m.leaderCommit \in 0..MaxLogLen

TypeOK ==
  /\ currentTerm \in [Nodes -> 0..MaxTerm]
  /\ votedFor \in [Nodes -> (Nodes \cup {NoVote})]
  /\ role \in [Nodes -> {Follower, Candidate, Leader}]
  /\ membership \in MembershipSet
  /\ \A n \in Nodes :
       n \notin ActiveVoters(membership) => role[n] = Follower
  /\ log \in [Nodes -> LogSet]
  /\ \A n \in Nodes : LogOK(log[n])
  /\ commitIndex \in [Nodes -> 0..MaxLogLen]
  /\ \A n \in Nodes : commitIndex[n] <= Len(log[n])
  /\ applied \in [Nodes -> AppliedSet]
  /\ \A n \in Nodes :
       /\ AppliedOK(applied[n])
       /\ Len(applied[n]) <= commitIndex[n]
       /\ \A i \in 1..Len(applied[n]) :
            applied[n][i] = log[n][i].value
  /\ messages \in SUBSET MessageSet
  /\ \A m \in messages : MessageOK(m)
  /\ readRequests \in SUBSET ReadRequestSet
  /\ \A n \in Nodes :
       \A request \in ReadRequests :
         Cardinality({r \in readRequests :
             /\ r.node = n
             /\ r.request = request}) <= 1
  /\ readGrants \in SUBSET ReadGrantSet
  /\ \A n \in Nodes :
       \A request \in ReadRequests :
         Cardinality({g \in readGrants :
             /\ g.node = n
             /\ g.request = request}) <= 1

Init ==
  /\ currentTerm = [n \in Nodes |-> 0]
  /\ votedFor = [n \in Nodes |-> NoVote]
  /\ role = [n \in Nodes |-> Follower]
  /\ log = [n \in Nodes |-> <<>>]
  /\ commitIndex = [n \in Nodes |-> 0]
  /\ applied = [n \in Nodes |-> <<>>]
  /\ messages = {}
  /\ readRequests = {}
  /\ readGrants = {}
  /\ membership = StableMembership(Nodes)

Timeout(n) ==
  /\ currentTerm[n] < MaxTerm
  /\ n \in ActiveVoters(membership)
  /\ currentTerm' = [currentTerm EXCEPT ![n] = @ + 1]
  /\ votedFor' = [votedFor EXCEPT ![n] = n]
  /\ role' = [role EXCEPT ![n] = Candidate]
  /\ UNCHANGED << log, commitIndex, applied, messages, readRequests, readGrants,
                  membership >>

SendRequestVote(c, v) ==
  LET msg == [type |-> RequestVote,
              term |-> currentTerm[c],
              from |-> c,
              to |-> v]
  IN
    /\ role[c] = Candidate
    /\ c # v
    /\ currentTerm[c] \in 1..MaxTerm
    /\ msg \notin messages
    /\ messages' = messages \cup {msg}
    /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, applied,
                    readRequests, readGrants, membership >>

DeliverRequestVote(m) ==
  LET grant == /\ m.term >= currentTerm[m.to]
               /\ votedFor[m.to] \in {NoVote, m.from}
               /\ m.from \in ActiveVoters(membership)
               /\ m.to \in ActiveVoters(membership)
               /\ UpToDate(m.from, m.to)
  IN
    /\ m \in messages
    /\ m.type = RequestVote
    /\ messages' = messages \ {m}
    /\ IF grant
       THEN /\ currentTerm' = [currentTerm EXCEPT ![m.to] = m.term]
            /\ votedFor' = [votedFor EXCEPT ![m.to] = m.from]
            /\ role' = [role EXCEPT ![m.to] = Follower]
       ELSE /\ currentTerm' = currentTerm
            /\ votedFor' = votedFor
            /\ role' = role
    /\ UNCHANGED << log, commitIndex, applied, readRequests, readGrants,
                    membership >>

BecomeLeader(n) ==
  /\ role[n] = Candidate
  /\ n \in ActiveVoters(membership)
  /\ QuorumNodes({v \in Nodes :
       /\ votedFor[v] = n
       /\ currentTerm[v] = currentTerm[n]})
  /\ role' = [role EXCEPT ![n] = Leader]
  /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, applied, messages,
                  readRequests, readGrants, membership >>

ClientAppend(n, value) ==
  /\ role[n] = Leader
  /\ currentTerm[n] \in 1..MaxTerm
  /\ Len(log[n]) < MaxLogLen
  /\ log' = [log EXCEPT ![n] = Append(@, Entry(currentTerm[n], value))]
  /\ UNCHANGED << currentTerm, votedFor, role, commitIndex, applied, messages,
                  readRequests, readGrants, membership >>

SendAppend(l, f) ==
  LET msg == [type |-> AppendEntries,
              term |-> currentTerm[l],
              from |-> l,
              to |-> f,
              entries |-> log[l],
              leaderCommit |-> commitIndex[l]]
  IN
    /\ role[l] = Leader
    /\ l # f
    /\ currentTerm[l] \in 1..MaxTerm
    /\ msg \notin messages
    /\ messages' = messages \cup {msg}
    /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, applied,
                    readRequests, readGrants, membership >>

DeliverAppend(m) ==
  LET accept == /\ m.term >= currentTerm[m.to]
                /\ m.from \in ActiveVoters(membership)
                /\ m.to \in ActiveVoters(membership)
                /\ CanAdoptLog(m.to, m.entries)
  IN
    /\ m \in messages
    /\ m.type = AppendEntries
    /\ messages' = messages \ {m}
    /\ IF accept
       THEN /\ currentTerm' = [currentTerm EXCEPT ![m.to] = m.term]
            /\ votedFor' = [votedFor EXCEPT ![m.to] =
                  IF m.term > currentTerm[m.to] THEN NoVote ELSE @]
            /\ role' = [role EXCEPT ![m.to] = Follower]
            /\ log' = [log EXCEPT ![m.to] = m.entries]
            /\ commitIndex' = [commitIndex EXCEPT ![m.to] =
                  AdvanceCommit(m.to, m.leaderCommit, Len(m.entries))]
       ELSE /\ currentTerm' = currentTerm
            /\ votedFor' = votedFor
            /\ role' = role
            /\ log' = log
            /\ commitIndex' = commitIndex
    /\ UNCHANGED << applied, readRequests, readGrants, membership >>

Commit(n, i) ==
  /\ role[n] = Leader
  /\ n \in ActiveVoters(membership)
  /\ i \in (commitIndex[n] + 1)..Len(log[n])
  /\ log[n][i].term = currentTerm[n]
  /\ QuorumNodes(MatchingReplicas(n, i))
  /\ commitIndex' = [commitIndex EXCEPT ![n] = i]
  /\ UNCHANGED << currentTerm, votedFor, role, log, applied, messages,
                  readRequests, readGrants, membership >>

Apply(n) ==
  LET next == Len(applied[n]) + 1
  IN
    /\ Len(applied[n]) < commitIndex[n]
    /\ applied' = [applied EXCEPT ![n] = Append(@, log[n][next].value)]
    /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, messages,
                    readRequests, readGrants, membership >>

Restart(n) ==
  /\ role' = [role EXCEPT ![n] = Follower]
  /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, applied, messages,
                  readRequests, readGrants, membership >>

EnterJoint(n, newVoters) ==
  LET next == JointMembership(membership.old, newVoters)
  IN
    /\ membership.phase = StableConfig
    /\ membership.old = Nodes
    /\ newVoters \in VoterSets
    /\ newVoters # membership.old
    /\ n \in newVoters
    /\ role[n] = Leader
    /\ CommittedEntriesHeldBy(newVoters)
    /\ membership' = next
    /\ role' = RoleAfterMembershipChange(next)
    /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, applied, messages,
                    readRequests, readGrants >>

LeaveJoint(n) ==
  LET next == StableMembership(membership.new)
  IN
    /\ membership.phase = JointConfig
    /\ n \in membership.new
    /\ role[n] = Leader
    /\ CommittedEntriesHeldBy(membership.new)
    /\ membership' = next
    /\ role' = RoleAfterMembershipChange(next)
    /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, applied, messages,
                    readRequests, readGrants >>

RegisterRead(n, request) ==
  LET read == [node |-> n,
               request |-> request,
               committedFloor |-> MaxCommittedIndex]
  IN
    /\ role[n] = Leader
    /\ request \in ReadRequests
    /\ \A r \in readRequests :
         ~(r.node = n /\ r.request = request)
    /\ readRequests' = readRequests \cup {read}
    /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, applied,
                    messages, readGrants, membership >>

GrantRead(n, request) ==
  \E read \in readRequests :
    /\ read.node = n
    /\ read.request = request
    /\ role[n] = Leader
    /\ commitIndex[n] >= read.committedFloor
    /\ \A grant \in readGrants :
         ~(grant.node = n /\ grant.request = request)
    /\ readGrants' = readGrants \cup
         {[node |-> n, request |-> request, readIndex |-> commitIndex[n]]}
    /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, applied,
                    messages, readRequests, membership >>

Next ==
  \/ \E n \in Nodes : Timeout(n)
  \/ \E c, v \in Nodes : SendRequestVote(c, v)
  \/ \E m \in messages : DeliverRequestVote(m)
  \/ \E n \in Nodes : BecomeLeader(n)
  \/ \E n \in Nodes, value \in Values : ClientAppend(n, value)
  \/ \E l, f \in Nodes : SendAppend(l, f)
  \/ \E m \in messages : DeliverAppend(m)
  \/ \E n \in Nodes, i \in 1..MaxLogLen : Commit(n, i)
  \/ \E n \in Nodes : Apply(n)
  \/ \E n \in Nodes : Restart(n)
  \/ \E n \in Nodes, voters \in VoterSets : EnterJoint(n, voters)
  \/ \E n \in Nodes : LeaveJoint(n)
  \/ \E n \in Nodes, request \in ReadRequests : RegisterRead(n, request)
  \/ \E n \in Nodes, request \in ReadRequests : GrantRead(n, request)

Spec == Init /\ [][Next]_vars

ElectionSafety ==
  \A t \in 1..MaxTerm :
    Cardinality({n \in Nodes : /\ role[n] = Leader
                              /\ currentTerm[n] = t}) <= 1

LogMatching ==
  \A a, b \in Nodes :
    \A i \in 1..MaxLogLen :
      (/\ i \in 1..Len(log[a])
       /\ i \in 1..Len(log[b])
       /\ log[a][i].term = log[b][i].term)
      => Prefix(log[a], i) = Prefix(log[b], i)

LeaderCompleteness ==
  \A leader, n \in Nodes :
    \A i \in 1..MaxLogLen :
      (/\ role[leader] = Leader
       /\ i \in 1..commitIndex[n]
       /\ currentTerm[leader] > log[n][i].term)
      => /\ i \in 1..Len(log[leader])
         /\ log[leader][i] = log[n][i]

CommittedPrefixStability ==
  \A a, b \in Nodes :
    \A i \in 1..MaxLogLen :
      (/\ i \in 1..commitIndex[a]
       /\ i \in 1..commitIndex[b])
      => log[a][i] = log[b][i]

StateMachineSafety ==
  \A a, b \in Nodes :
    \A i \in 1..MaxLogLen :
      (/\ i \in 1..Len(applied[a])
       /\ i \in 1..Len(applied[b]))
      => applied[a][i] = applied[b][i]

StaleLeaderFencing ==
  \A n \in Nodes :
    role[n] = Leader =>
      /\ n \in ActiveVoters(membership)
      /\ \A i \in 1..commitIndex[n] :
           /\ log[n][i].term <= currentTerm[n]
           /\ QuorumNodes(MatchingReplicas(n, i))

CommittedEntriesHaveQuorum ==
  \A n \in Nodes :
    \A i \in 1..commitIndex[n] :
      QuorumNodes(MatchingReplicas(n, i))

ReadBarrierLinearizability ==
  \A grant \in readGrants :
    \E read \in readRequests :
      /\ read.node = grant.node
      /\ read.request = grant.request
      /\ grant.readIndex >= read.committedFloor

====
