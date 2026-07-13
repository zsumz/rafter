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
          readRequests, readGrants, membership, appliedConfigIndex,
          electedLeaders,
          higherTermEvidenceSeen, higherTermStepDownFailed,
          staleAuthorityAccepted

vars == << currentTerm, votedFor, role, log, commitIndex, applied, messages,
          readRequests, readGrants, membership, appliedConfigIndex,
          electedLeaders,
          higherTermEvidenceSeen, higherTermStepDownFailed,
          staleAuthorityAccepted >>

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b

CommandEntryKind == "Command"
ConfigurationEntryKind == "Configuration"

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

Entry(term, value) ==
  [term |-> term, kind |-> CommandEntryKind, input |-> value]

ConfigurationEntry(term, config) ==
  [term |-> term, kind |-> ConfigurationEntryKind, input |-> config]

ConfigurationSet ==
  {JointMembership(Nodes, voters) :
      voters \in {candidate \in VoterSets : candidate # Nodes}}
  \cup {StableMembership(voters) :
      voters \in {candidate \in VoterSets : candidate # Nodes}}

EntrySet ==
  {Entry(t, v) : t \in 1..MaxTerm, v \in Values}
  \cup {ConfigurationEntry(t, config) :
      t \in 1..MaxTerm, config \in ConfigurationSet}

AllLogs ==
  {<<>>}
  \cup {<<e1>> : e1 \in EntrySet}
  \cup {<<e1, e2>> : e1 \in EntrySet, e2 \in EntrySet}
  \cup {<<e1, e2, e3>> : e1 \in EntrySet, e2 \in EntrySet, e3 \in EntrySet}

LogSet == {s \in AllLogs : Len(s) <= MaxLogLen}

AllReferenceStates ==
  {<<>>}
  \cup {<<v1>> : v1 \in Values}
  \cup {<<v1, v2>> : v1 \in Values, v2 \in Values}
  \cup {<<v1, v2, v3>> : v1 \in Values, v2 \in Values, v3 \in Values}

ReferenceStateSet ==
  {s \in AllReferenceStates : Len(s) <= MaxLogLen}

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

InitialApplicationState ==
  [referenceState |-> <<>>, membership |-> StableMembership(Nodes)]

ApplicationStateOK(state) ==
  /\ DOMAIN state = {"referenceState", "membership"}
  /\ state.referenceState \in ReferenceStateSet
  /\ state.membership \in MembershipSet

ApplyEntry(state, entry) ==
  IF entry.kind = CommandEntryKind
  THEN [state EXCEPT !.referenceState = Append(@, entry.input)]
  ELSE [state EXCEPT !.membership = entry.input]

AppliedEvent(index, entry, priorState, resultState) ==
  [index |-> index,
   entry |-> entry,
   priorState |-> priorState,
   resultState |-> resultState]

AppliedEventOK(event) ==
  /\ DOMAIN event = {"index", "entry", "priorState", "resultState"}
  /\ event.index \in 1..MaxLogLen
  /\ event.entry \in EntrySet
  /\ ApplicationStateOK(event.priorState)
  /\ ApplicationStateOK(event.resultState)

AppliedResultState(history) ==
  IF Len(history) = 0
  THEN InitialApplicationState
  ELSE history[Len(history)].resultState

AppliedEntryPrefix(history, index) ==
  [i \in 1..index |-> history[i].entry]

AppliedHistorySound(history) ==
  \A i \in 1..Len(history) :
    /\ history[i].priorState =
         IF i = 1 THEN InitialApplicationState ELSE history[i - 1].resultState
    /\ history[i].resultState =
         ApplyEntry(history[i].priorState, history[i].entry)

RecordElection(node) ==
  electedLeaders' = [electedLeaders EXCEPT
    ![currentTerm[node]] = @ \cup {node}]

RecordHigherTermOutcome(node, evidenceTerm, observedHigherTerm) ==
  /\ higherTermEvidenceSeen' =
       IF observedHigherTerm THEN TRUE ELSE higherTermEvidenceSeen
  /\ higherTermStepDownFailed' =
       IF /\ observedHigherTerm
          /\ (currentTerm'[node] # evidenceTerm \/ role'[node] # Follower)
       THEN TRUE
       ELSE higherTermStepDownFailed

RecordAuthorityAcceptance(authorityTerm, knownTerm, accepted) ==
  staleAuthorityAccepted' =
    IF accepted /\ authorityTerm < knownTerm
    THEN TRUE
    ELSE staleAuthorityAccepted

RecordApplication(node, index, entry, priorState, resultState) ==
  applied' = [applied EXCEPT ![node] =
    Append(@, AppliedEvent(index, entry, priorState, resultState))]

NodePairSet ==
  {pair \in [from: Nodes, to: Nodes] : pair.from # pair.to}

TermLogSet ==
  {payload \in [term: 1..MaxTerm, entries: LogSet] :
    \A i \in 1..Len(payload.entries) :
      payload.entries[i].term <= payload.term}

RequestVoteMessages ==
  {[type |-> RequestVote,
    term |-> t,
    from |-> pair.from,
    to |-> pair.to] :
      t \in 1..MaxTerm, pair \in NodePairSet}

AppendEntriesMessages ==
  {message \in
    {[type |-> AppendEntries,
      term |-> payload.term,
      from |-> pair.from,
      to |-> pair.to,
      entries |-> payload.entries,
      leaderCommit |-> leaderCommit] :
        pair \in NodePairSet,
        payload \in TermLogSet,
        leaderCommit \in 0..MaxLogLen} :
    message.leaderCommit <= Len(message.entries)}

MessageSet == RequestVoteMessages \cup AppendEntriesMessages

EntryOK(e) ==
  /\ e.term \in 1..MaxTerm
  /\ e.kind \in {CommandEntryKind, ConfigurationEntryKind}
  /\ IF e.kind = CommandEntryKind
     THEN e.input \in Values
     ELSE e.input \in ConfigurationSet

LogOK(s) == s \in LogSet

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
     /\ m.leaderCommit \in 0..Len(m.entries)
     /\ \A i \in 1..Len(m.entries) : m.entries[i].term <= m.term

AppliedConfigurationStateOK ==
  /\ \A n \in Nodes :
       \A i \in 1..Len(applied[n]) :
         applied[n][i].entry.kind = ConfigurationEntryKind =>
           i <= appliedConfigIndex
  /\ IF appliedConfigIndex = 0
     THEN membership = InitialApplicationState.membership
     ELSE \E n \in Nodes :
            /\ appliedConfigIndex <= Len(applied[n])
            /\ applied[n][appliedConfigIndex].entry.kind =
                 ConfigurationEntryKind
            /\ membership =
                 applied[n][appliedConfigIndex].resultState.membership

TypeOK ==
  /\ currentTerm \in [Nodes -> 0..MaxTerm]
  /\ votedFor \in [Nodes -> (Nodes \cup {NoVote})]
  /\ role \in [Nodes -> {Follower, Candidate, Leader}]
  /\ electedLeaders \in [1..MaxTerm -> SUBSET Nodes]
  /\ \A n \in Nodes :
       role[n] = Leader =>
         /\ currentTerm[n] \in 1..MaxTerm
         /\ n \in electedLeaders[currentTerm[n]]
  /\ higherTermEvidenceSeen \in BOOLEAN
  /\ higherTermStepDownFailed \in BOOLEAN
  /\ staleAuthorityAccepted \in BOOLEAN
  /\ membership \in MembershipSet
  /\ appliedConfigIndex \in 0..MaxLogLen
  /\ \A n \in Nodes :
       n \notin ActiveVoters(membership) => role[n] = Follower
  /\ log \in [Nodes -> LogSet]
  /\ \A n \in Nodes : LogOK(log[n])
  /\ commitIndex \in [Nodes -> 0..MaxLogLen]
  /\ \A n \in Nodes : commitIndex[n] <= Len(log[n])
  /\ DOMAIN applied = Nodes
  /\ \A n \in Nodes :
       /\ Len(applied[n]) <= commitIndex[n]
       /\ DOMAIN applied[n] = 1..Len(applied[n])
       /\ \A i \in 1..Len(applied[n]) :
            /\ AppliedEventOK(applied[n][i])
            /\ applied[n][i].index = i
            /\ applied[n][i].entry = log[n][i]
  /\ AppliedConfigurationStateOK
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
  /\ appliedConfigIndex = 0
  /\ electedLeaders = [t \in 1..MaxTerm |-> {}]
  /\ higherTermEvidenceSeen = FALSE
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE

Timeout(n) ==
  /\ currentTerm[n] < MaxTerm
  /\ n \in ActiveVoters(membership)
  /\ currentTerm' = [currentTerm EXCEPT ![n] = @ + 1]
  /\ votedFor' = [votedFor EXCEPT ![n] = n]
  /\ role' = [role EXCEPT ![n] = Candidate]
  /\ UNCHANGED << log, commitIndex, applied, messages, readRequests, readGrants,
                  membership, appliedConfigIndex, electedLeaders,
                  higherTermEvidenceSeen,
                  higherTermStepDownFailed, staleAuthorityAccepted >>

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
    /\ RecordAuthorityAcceptance(currentTerm[c], currentTerm[c], TRUE)
    /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, applied,
                    readRequests, readGrants, membership, appliedConfigIndex,
                    electedLeaders,
                    higherTermEvidenceSeen, higherTermStepDownFailed >>

DeliverRequestVote(m) ==
  LET higher == m.term > currentTerm[m.to]
      eligibleVote == IF higher THEN NoVote ELSE votedFor[m.to]
      grant == /\ m.term >= currentTerm[m.to]
               /\ eligibleVote \in {NoVote, m.from}
               /\ m.from \in ActiveVoters(membership)
               /\ m.to \in ActiveVoters(membership)
               /\ UpToDate(m.from, m.to)
  IN
    /\ m \in messages
    /\ m.type = RequestVote
    /\ messages' = messages \ {m}
    /\ currentTerm' = [currentTerm EXCEPT ![m.to] =
         IF higher THEN m.term ELSE @]
    /\ votedFor' = [votedFor EXCEPT ![m.to] =
         IF grant THEN m.from ELSE IF higher THEN NoVote ELSE @]
    /\ role' = [role EXCEPT ![m.to] =
         IF higher \/ grant THEN Follower ELSE @]
    /\ RecordHigherTermOutcome(m.to, m.term, higher)
    /\ RecordAuthorityAcceptance(m.term, currentTerm[m.to], grant)
    /\ UNCHANGED << log, commitIndex, applied, readRequests, readGrants,
                    membership, appliedConfigIndex, electedLeaders >>

BecomeLeader(n) ==
  /\ role[n] = Candidate
  /\ n \in ActiveVoters(membership)
  /\ QuorumNodes({v \in Nodes :
       /\ votedFor[v] = n
       /\ currentTerm[v] = currentTerm[n]})
  /\ role' = [role EXCEPT ![n] = Leader]
  /\ RecordElection(n)
  /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
  /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, applied,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex,
                  higherTermEvidenceSeen, higherTermStepDownFailed >>

ClientAppend(n, value) ==
  /\ role[n] = Leader
  /\ currentTerm[n] \in 1..MaxTerm
  /\ Len(log[n]) < MaxLogLen
  /\ log' = [log EXCEPT ![n] = Append(@, Entry(currentTerm[n], value))]
  /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
  /\ UNCHANGED << currentTerm, votedFor, role, commitIndex, applied,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex,
                  electedLeaders,
                  higherTermEvidenceSeen, higherTermStepDownFailed >>

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
    /\ RecordAuthorityAcceptance(currentTerm[l], currentTerm[l], TRUE)
    /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, applied,
                    readRequests, readGrants, membership, appliedConfigIndex,
                    electedLeaders,
                    higherTermEvidenceSeen, higherTermStepDownFailed >>

DeliverAppend(m) ==
  LET higher == m.term > currentTerm[m.to]
      accept == /\ m.term >= currentTerm[m.to]
                /\ m.from \in ActiveVoters(membership)
                /\ m.to \in ActiveVoters(membership)
                /\ CanAdoptLog(m.to, m.entries)
  IN
    /\ m \in messages
    /\ m.type = AppendEntries
    /\ messages' = messages \ {m}
    /\ currentTerm' = [currentTerm EXCEPT ![m.to] =
         IF higher THEN m.term ELSE @]
    /\ votedFor' = [votedFor EXCEPT ![m.to] =
         IF higher THEN NoVote ELSE @]
    /\ role' = [role EXCEPT ![m.to] =
         IF higher \/ accept THEN Follower ELSE @]
    /\ IF accept
       THEN /\ log' = [log EXCEPT ![m.to] = m.entries]
            /\ commitIndex' = [commitIndex EXCEPT ![m.to] =
                  AdvanceCommit(m.to, m.leaderCommit, Len(m.entries))]
       ELSE /\ log' = log
            /\ commitIndex' = commitIndex
    /\ RecordHigherTermOutcome(m.to, m.term, higher)
    /\ RecordAuthorityAcceptance(m.term, currentTerm[m.to], accept)
    /\ UNCHANGED << applied, readRequests, readGrants, membership,
                    appliedConfigIndex,
                    electedLeaders >>

Commit(n, i) ==
  /\ role[n] = Leader
  /\ n \in ActiveVoters(membership)
  /\ i \in (commitIndex[n] + 1)..Len(log[n])
  /\ log[n][i].term = currentTerm[n]
  /\ QuorumNodes(MatchingReplicas(n, i))
  /\ commitIndex' = [commitIndex EXCEPT ![n] = i]
  /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
  /\ UNCHANGED << currentTerm, votedFor, role, log, applied,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex,
                  electedLeaders,
                  higherTermEvidenceSeen, higherTermStepDownFailed >>

Apply(n) ==
  LET next == Len(applied[n]) + 1
      entry == log[n][next]
      priorState == AppliedResultState(applied[n])
      resultState == ApplyEntry(priorState, entry)
      isNewConfiguration ==
        /\ entry.kind = ConfigurationEntryKind
        /\ next > appliedConfigIndex
  IN
    /\ Len(applied[n]) < commitIndex[n]
    /\ RecordApplication(n, next, entry, priorState, resultState)
    /\ membership' =
         IF isNewConfiguration THEN entry.input ELSE membership
    /\ appliedConfigIndex' =
         IF isNewConfiguration THEN next ELSE appliedConfigIndex
    /\ role' =
         IF isNewConfiguration
         THEN RoleAfterMembershipChange(entry.input)
         ELSE role
    /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, messages,
                    readRequests, readGrants, electedLeaders,
                    higherTermEvidenceSeen, higherTermStepDownFailed,
                    staleAuthorityAccepted >>

Restart(n) ==
  /\ role' = [role EXCEPT ![n] = Follower]
  /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, applied,
                  messages, readRequests, readGrants, membership,
                  appliedConfigIndex,
                  electedLeaders,
                  higherTermEvidenceSeen, higherTermStepDownFailed,
                  staleAuthorityAccepted >>

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
    /\ currentTerm[n] \in 1..MaxTerm
    /\ Len(log[n]) = Len(applied[n])
    /\ Len(log[n]) < MaxLogLen
    /\ log' = [log EXCEPT ![n] =
         Append(@, ConfigurationEntry(currentTerm[n], next))]
    /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
    /\ UNCHANGED << currentTerm, votedFor, role, commitIndex, applied,
                    messages, readRequests, readGrants, membership,
                    appliedConfigIndex,
                    electedLeaders,
                    higherTermEvidenceSeen, higherTermStepDownFailed >>

LeaveJoint(n) ==
  LET next == StableMembership(membership.new)
  IN
    /\ membership.phase = JointConfig
    /\ n \in membership.new
    /\ role[n] = Leader
    /\ CommittedEntriesHeldBy(membership.new)
    /\ currentTerm[n] \in 1..MaxTerm
    /\ Len(log[n]) = Len(applied[n])
    /\ Len(log[n]) < MaxLogLen
    /\ log' = [log EXCEPT ![n] =
         Append(@, ConfigurationEntry(currentTerm[n], next))]
    /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
    /\ UNCHANGED << currentTerm, votedFor, role, commitIndex, applied,
                    messages, readRequests, readGrants, membership,
                    appliedConfigIndex,
                    electedLeaders,
                    higherTermEvidenceSeen, higherTermStepDownFailed >>

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
    /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
    /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, applied,
                    messages, readGrants, membership, appliedConfigIndex,
                    electedLeaders,
                    higherTermEvidenceSeen, higherTermStepDownFailed >>

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
    /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
    /\ UNCHANGED << currentTerm, votedFor, role, log, commitIndex, applied,
                    messages, readRequests, membership, appliedConfigIndex,
                    electedLeaders,
                    higherTermEvidenceSeen, higherTermStepDownFailed >>

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
    Cardinality(electedLeaders[t]) <= 1

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
  /\ \A n \in Nodes : AppliedHistorySound(applied[n])
  /\ \A a, b \in Nodes :
       \A i \in 1..MaxLogLen :
         (/\ i \in 1..Len(applied[a])
          /\ i \in 1..Len(applied[b]))
         => /\ AppliedEntryPrefix(applied[a], i) =
                  AppliedEntryPrefix(applied[b], i)
            /\ applied[a][i].priorState = applied[b][i].priorState
            /\ applied[a][i].resultState = applied[b][i].resultState

StaleLeaderFencing ==
  /\ ~higherTermStepDownFailed
  /\ ~staleAuthorityAccepted

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
