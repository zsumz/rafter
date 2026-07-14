---- MODULE Raft ----
EXTENDS Naturals, Sequences, FiniteSets, TLC

\* This is a small, bounded design model. It is not implementation code and it
\* intentionally does not import or depend on rafter.
\* The production specification is safety-only. Fair-schedule liveness evidence
\* is owned by the bounded simulator, whose scheduler states its timing bounds.

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

VARIABLES currentTerm, votedFor, role, log, commitIndex,
          snapshotIndex, snapshotPrefix, compactionPending, snapshotTransfer,
          applied, applicationEpoch, epochBaseIndex, epochBaseState,
          applicationState, appliedThrough,
          messages, readRequests, readBarrierViolationSeen,
          membership, appliedConfigIndex,
          effectiveMembership, effectiveConfigIndex,
          electedLeaders, logicalPrefixLedger, committedLedger,
          commitWitnesses,
          higherTermStepDownFailed,
          staleAuthorityAccepted

vars == << currentTerm, votedFor, role, log, commitIndex,
          snapshotIndex, snapshotPrefix, compactionPending, snapshotTransfer,
          applied, applicationEpoch, epochBaseIndex, epochBaseState,
          applicationState, appliedThrough,
          messages, readRequests, readBarrierViolationSeen,
          membership, appliedConfigIndex,
          effectiveMembership, effectiveConfigIndex,
          electedLeaders, logicalPrefixLedger, committedLedger,
          commitWitnesses,
          higherTermStepDownFailed,
          staleAuthorityAccepted >>

snapshotVars ==
  <<snapshotIndex, snapshotPrefix, compactionPending, snapshotTransfer>>

applicationVars ==
  <<applied, applicationEpoch, epochBaseIndex, epochBaseState,
    applicationState, appliedThrough>>

historyVars == <<logicalPrefixLedger, committedLedger, commitWitnesses>>

authorityVars ==
  <<higherTermStepDownFailed, staleAuthorityAccepted>>

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

StateAfterEntries(entries) ==
  CASE Len(entries) = 0 -> InitialApplicationState
    [] Len(entries) = 1 -> ApplyEntry(InitialApplicationState, entries[1])
    [] Len(entries) = 2 ->
         ApplyEntry(ApplyEntry(InitialApplicationState, entries[1]), entries[2])
    [] OTHER ->
         ApplyEntry(
           ApplyEntry(ApplyEntry(InitialApplicationState, entries[1]), entries[2]),
           entries[3])

AppliedEvent(epoch, baseIndex, baseState, index, entry, priorState, resultState) ==
  [epoch |-> epoch,
   baseIndex |-> baseIndex,
   baseState |-> baseState,
   index |-> index,
   entry |-> entry,
   priorState |-> priorState,
   resultState |-> resultState]

AppliedEventOK(event) ==
  /\ DOMAIN event = {"epoch", "baseIndex", "baseState", "index", "entry",
                      "priorState", "resultState"}
  /\ event.epoch \in 0..1
  /\ event.baseIndex \in 0..MaxLogLen
  /\ ApplicationStateOK(event.baseState)
  /\ event.index \in 1..MaxLogLen
  /\ event.baseIndex < event.index
  /\ event.entry \in EntrySet
  /\ ApplicationStateOK(event.priorState)
  /\ ApplicationStateOK(event.resultState)

AppliedHistorySound(history) ==
  \A position \in 1..Len(history) :
    LET event == history[position]
        firstInEpoch ==
          position = 1 \/ history[position - 1].epoch # event.epoch
    IN
      /\ AppliedEventOK(event)
      /\ IF firstInEpoch
         THEN /\ event.index = event.baseIndex + 1
              /\ event.priorState = event.baseState
              /\ IF position = 1
                 THEN TRUE
                 ELSE event.epoch = history[position - 1].epoch + 1
         ELSE /\ event.epoch = history[position - 1].epoch
              /\ event.baseIndex = history[position - 1].baseIndex
              /\ event.baseState = history[position - 1].baseState
              /\ event.index = history[position - 1].index + 1
              /\ event.priorState = history[position - 1].resultState
      /\ event.resultState = ApplyEntry(event.priorState, event.entry)

LatestIndex(indexes) ==
  CHOOSE index \in indexes : \A other \in indexes : other <= index

Prefix(s, i) ==
  IF i = 0 THEN <<>> ELSE SubSeq(s, 1, i)

LatestConfigurationIn(entries) ==
  LET candidates ==
        {index \in 1..Len(entries) :
          entries[index].kind = ConfigurationEntryKind}
  IN
    IF candidates = {}
    THEN [configIndex |-> 0, config |-> StableMembership(Nodes)]
    ELSE LET latest == LatestIndex(candidates)
         IN [configIndex |-> latest, config |-> entries[latest].input]

EffectiveConfigurationFor(entries) ==
  LET candidates ==
        {index \in 1..Len(entries) :
          /\ index > appliedConfigIndex
          /\ entries[index].kind = ConfigurationEntryKind}
  IN
    IF candidates = {}
    THEN [configIndex |-> appliedConfigIndex, config |-> membership]
    ELSE LET latest == LatestIndex(candidates)
         IN [configIndex |-> latest, config |-> entries[latest].input]

LogicalPrefixFrom(logs, snapshotIndexes, snapshotPrefixes, node, index) ==
  IF index = 0
  THEN <<>>
  ELSE IF index <= snapshotIndexes[node]
       THEN Prefix(snapshotPrefixes[node], index)
       ELSE snapshotPrefixes[node]
              \o SubSeq(logs[node], snapshotIndexes[node] + 1, index)

LogicalEntryFrom(logs, snapshotIndexes, snapshotPrefixes, node, index) ==
  LogicalPrefixFrom(logs, snapshotIndexes, snapshotPrefixes, node, index)[index]

LogicalPrefix(node, index) ==
  LogicalPrefixFrom(log, snapshotIndex, snapshotPrefix, node, index)

LogicalEntry(node, index) ==
  LogicalEntryFrom(log, snapshotIndex, snapshotPrefix, node, index)

SnapshotIdentitySoundFor(logs, snapshotIndexes, snapshotPrefixes, compactionPendings) ==
  \A n \in Nodes :
    /\ snapshotIndexes[n] = Len(snapshotPrefixes[n])
    /\ snapshotIndexes[n] <= Len(logs[n])
    /\ compactionPendings[n] => snapshotIndexes[n] > 0
    /\ Prefix(logs[n], snapshotIndexes[n]) = snapshotPrefixes[n]

LogicalPrefixWitnesses(logs, snapshotIndexes, snapshotPrefixes) ==
  UNION {
    {[index |-> index,
      term |-> LogicalEntryFrom(
        logs, snapshotIndexes, snapshotPrefixes, node, index).term,
      prefix |-> LogicalPrefixFrom(
        logs, snapshotIndexes, snapshotPrefixes, node, index)] :
        index \in 1..Len(logs[node])} :
      node \in Nodes}

RecordLogicalPrefixes(logs, snapshotIndexes, snapshotPrefixes) ==
  logicalPrefixLedger' =
    logicalPrefixLedger \cup
      LogicalPrefixWitnesses(logs, snapshotIndexes, snapshotPrefixes)

LogicalPrefixLedgerSound ==
  \A a, b \in logicalPrefixLedger :
    (a.index = b.index /\ a.term = b.term) => a.prefix = b.prefix

AuthoritativeLogReplacement(message, accepted) ==
  /\ accepted
  /\ role[message.from] = Leader
  /\ currentTerm[message.from] = message.term
  /\ log[message.from] = message.entries

RecordElection(node) ==
  electedLeaders' = [electedLeaders EXCEPT
    ![currentTerm[node]] = @ \cup {node}]

RecordHigherTermOutcome(node, evidenceTerm, observedHigherTerm) ==
  higherTermStepDownFailed' =
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
    Append(@, AppliedEvent(
      applicationEpoch[node], epochBaseIndex[node], epochBaseState[node],
      index, entry, priorState, resultState))]

CommittedEntriesFor(
    logs, snapshotIndexes, snapshotPrefixes, node, oldFloor, newFloor) ==
  {[index |-> index,
    entry |-> LogicalEntryFrom(
      logs, snapshotIndexes, snapshotPrefixes, node, index)] :
      index \in (oldFloor + 1)..newFloor}

RecordCommittedEntries(
    logs, snapshotIndexes, snapshotPrefixes, node, oldFloor, newFloor) ==
  committedLedger' =
    committedLedger \cup
      CommittedEntriesFor(
        logs, snapshotIndexes, snapshotPrefixes, node, oldFloor, newFloor)

ConfigurationMembershipAt(
    logs, snapshotIndexes, snapshotPrefixes, node, configIndex) ==
  IF configIndex = 0
  THEN StableMembership(Nodes)
  ELSE LET entry == LogicalEntryFrom(
             logs, snapshotIndexes, snapshotPrefixes, node, configIndex)
       IN IF entry.kind = ConfigurationEntryKind
          THEN entry.input
          ELSE StableMembership(Nodes)

PriorConfigurationFor(node, configIndex) ==
  LatestConfigurationIn(LogicalPrefix(node, configIndex - 1))

FrozenCommitContext(
    leaderRole, leaderTerm, effectiveView, authorityView) ==
  [leaderRole |-> leaderRole,
   leaderTerm |-> leaderTerm,
   effectiveMembership |-> effectiveView,
   authorityMembership |-> authorityView]

MatchingReplicasFrom(logs, snapshotIndexes, snapshotPrefixes, node, index) ==
  {replica \in Nodes :
    /\ index \in 1..Len(logs[replica])
    /\ index \in 1..Len(logs[node])
    /\ LogicalEntryFrom(
         logs, snapshotIndexes, snapshotPrefixes, replica, index)
         = LogicalEntryFrom(
             logs, snapshotIndexes, snapshotPrefixes, node, index)}

MatchingReplicas(node, index) ==
  MatchingReplicasFrom(log, snapshotIndex, snapshotPrefix, node, index)

CommitCertificatesFor(
    node, oldFloor, newFloor, context, configIndex) ==
  {[index |-> index,
    entry |-> LogicalEntry(node, index),
    leader |-> node,
    leaderRole |-> context.leaderRole,
    leaderTerm |-> context.leaderTerm,
    membership |-> context.effectiveMembership,
    authorityMembership |-> context.authorityMembership,
    derivedMembership |-> ConfigurationMembershipAt(
      log, snapshotIndex, snapshotPrefix, node, configIndex),
    configIndex |-> configIndex,
    replicas |-> MatchingReplicas(node, index)] :
      index \in (oldFloor + 1)..newFloor}

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

NoSnapshotTransfer ==
  [active |-> FALSE, term |-> 0, from |-> NoVote, to |-> NoVote,
   index |-> 0, prefix |-> <<>>]

EntryOK(e) ==
  /\ e.term \in 1..MaxTerm
  /\ e.kind \in {CommandEntryKind, ConfigurationEntryKind}
  /\ IF e.kind = CommandEntryKind
     THEN e.input \in Values
     ELSE e.input \in ConfigurationSet

LogOK(s) == s \in LogSet

LastTerm(s) ==
  IF Len(s) = 0 THEN 0 ELSE s[Len(s)].term

UpToDate(candidate, voter) ==
  \/ LastTerm(log[candidate]) > LastTerm(log[voter])
  \/ /\ LastTerm(log[candidate]) = LastTerm(log[voter])
     /\ Len(log[candidate]) >= Len(log[voter])

ActiveVoters(config) ==
  IF config.phase = StableConfig THEN config.old ELSE config.old \cup config.new

PendingSelfRemoval(node) ==
  IF /\ effectiveConfigIndex \in 1..Len(log[node])
     /\ effectiveConfigIndex > commitIndex[node]
  THEN LET entry == LogicalEntry(node, effectiveConfigIndex)
           prior == PriorConfigurationFor(node, effectiveConfigIndex)
       IN /\ role[node] = Leader
          /\ currentTerm[node] = entry.term
          /\ entry.kind = ConfigurationEntryKind
          /\ entry.input = effectiveMembership
          /\ effectiveMembership.phase = StableConfig
          /\ node \notin ActiveVoters(effectiveMembership)
          /\ prior.configIndex < effectiveConfigIndex
          /\ prior.config.phase = JointConfig
          /\ prior.config.new = effectiveMembership.old
          /\ node \in ActiveVoters(prior.config)
  ELSE FALSE

CommitAuthorityMembership(node) ==
  IF PendingSelfRemoval(node)
  THEN PriorConfigurationFor(node, effectiveConfigIndex).config
  ELSE effectiveMembership

StableQuorum(voters, ns) ==
  2 * Cardinality(ns \cap voters) > Cardinality(voters)

MembershipQuorum(config, ns) ==
  IF config.phase = StableConfig
  THEN StableQuorum(config.old, ns)
  ELSE /\ StableQuorum(config.old, ns)
       /\ StableQuorum(config.new, ns)

QuorumNodes(ns) == MembershipQuorum(effectiveMembership, ns)

CommitWitnessOK(witness) ==
  /\ witness.leaderRole = Leader
  /\ witness.entry.term <= witness.leaderTerm
  /\ witness.membership = witness.derivedMembership
  /\ MembershipQuorum(witness.membership, witness.replicas)
  /\ IF witness.leader \in ActiveVoters(witness.membership)
     THEN witness.authorityMembership = witness.membership
     ELSE /\ witness.index = witness.configIndex
          /\ witness.entry.kind = ConfigurationEntryKind
          /\ witness.entry.input = witness.membership
          /\ witness.authorityMembership.phase = JointConfig
          /\ witness.authorityMembership.new = witness.membership.old
          /\ witness.leader \in ActiveVoters(witness.authorityMembership)

CommitWitnessKeys(witnesses) ==
  {[index |-> witness.index, entry |-> witness.entry] :
      witness \in witnesses}

CommitWitnessHistory(witnessed, invalidCertificateSeen) ==
  [witnessedCommits |-> witnessed,
   invalidCertificateSeen |-> invalidCertificateSeen]

EmptyCommitWitnessHistory == CommitWitnessHistory({}, FALSE)

RecordCommitWitnesses(witnesses) ==
  commitWitnesses' = CommitWitnessHistory(
    commitWitnesses.witnessedCommits \cup CommitWitnessKeys(witnesses),
    commitWitnesses.invalidCertificateSeen \/
      \E witness \in witnesses : ~CommitWitnessOK(witness))

ReadGrantOK(grant) ==
  /\ grant \in ReadGrantSet
  /\ \E read \in readRequests :
       /\ read.node = grant.node
       /\ read.request = grant.request
       /\ grant.readIndex >= read.committedFloor

RecordReadGrant(grant) ==
  readBarrierViolationSeen' =
    IF ReadGrantOK(grant) THEN readBarrierViolationSeen ELSE TRUE

CanAdoptLog(n, entries) ==
  /\ LogOK(entries)
  /\ Len(entries) >= Len(log[n])
  /\ \A i \in 1..commitIndex[n] :
       /\ i \in 1..Len(entries)
       /\ entries[i] = LogicalEntry(n, i)

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

RolesAfterMembershipChange(roles, config) ==
  [n \in Nodes |-> IF n \in ActiveVoters(config) THEN roles[n] ELSE Follower]

RoleAfterMembershipChange(config) ==
  RolesAfterMembershipChange(role, config)

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
       \A position \in 1..Len(applied[n]) :
         applied[n][position].entry.kind = ConfigurationEntryKind =>
           applied[n][position].index <= appliedConfigIndex
  /\ IF appliedConfigIndex = 0
     THEN membership = InitialApplicationState.membership
     ELSE \E n \in Nodes :
            /\ appliedConfigIndex <= Len(log[n])
            /\ LogicalEntry(n, appliedConfigIndex).kind = ConfigurationEntryKind
            /\ membership = LogicalEntry(n, appliedConfigIndex).input

EffectiveConfigurationStateOK ==
  /\ appliedConfigIndex <= effectiveConfigIndex
  /\ IF effectiveConfigIndex = appliedConfigIndex
     THEN effectiveMembership = membership
     ELSE \E n \in Nodes :
            /\ effectiveConfigIndex <= Len(log[n])
            /\ LogicalEntry(n, effectiveConfigIndex).kind = ConfigurationEntryKind
            /\ effectiveMembership = LogicalEntry(n, effectiveConfigIndex).input

ApplicationEpochStateOK(n) ==
  /\ epochBaseIndex[n] <= appliedThrough[n]
  /\ appliedThrough[n] <= commitIndex[n]
  /\ IF appliedThrough[n] = epochBaseIndex[n]
     THEN applicationState[n] = epochBaseState[n]
     ELSE /\ Len(applied[n]) > 0
          /\ applied[n][Len(applied[n])].epoch = applicationEpoch[n]
          /\ applied[n][Len(applied[n])].index = appliedThrough[n]
          /\ applicationState[n] = applied[n][Len(applied[n])].resultState

TypeOK ==
  /\ currentTerm \in [Nodes -> 0..MaxTerm]
  /\ votedFor \in [Nodes -> (Nodes \cup {NoVote})]
  /\ role \in [Nodes -> {Follower, Candidate, Leader}]
  /\ electedLeaders \in [1..MaxTerm -> SUBSET Nodes]
  /\ \A n \in Nodes :
       role[n] = Leader =>
         /\ currentTerm[n] \in 1..MaxTerm
         /\ n \in electedLeaders[currentTerm[n]]
  /\ higherTermStepDownFailed \in BOOLEAN
  /\ staleAuthorityAccepted \in BOOLEAN
  /\ membership \in MembershipSet
  /\ appliedConfigIndex \in 0..MaxLogLen
  /\ effectiveMembership \in MembershipSet
  /\ effectiveConfigIndex \in 0..MaxLogLen
  /\ \A n \in Nodes :
       n \notin ActiveVoters(effectiveMembership) =>
         \/ role[n] = Follower
         \/ PendingSelfRemoval(n)
  /\ log \in [Nodes -> LogSet]
  /\ \A n \in Nodes : LogOK(log[n])
  /\ commitIndex \in [Nodes -> 0..MaxLogLen]
  /\ \A n \in Nodes : commitIndex[n] <= Len(log[n])
  /\ DOMAIN snapshotIndex = Nodes
  /\ DOMAIN snapshotPrefix = Nodes
  /\ compactionPending \in [Nodes -> BOOLEAN]
  /\ \A n \in Nodes :
       /\ snapshotIndex[n] \in 0..MaxLogLen
       /\ snapshotPrefix[n] \in LogSet
       /\ snapshotIndex[n] = Len(snapshotPrefix[n])
       /\ snapshotIndex[n] <= Len(log[n])
       /\ compactionPending[n] => snapshotIndex[n] > 0
  /\ DOMAIN snapshotTransfer =
       {"active", "term", "from", "to", "index", "prefix"}
  /\ IF snapshotTransfer.active
     THEN /\ snapshotTransfer.term \in 1..MaxTerm
          /\ snapshotTransfer.from \in Nodes
          /\ snapshotTransfer.to \in Nodes
          /\ snapshotTransfer.from # snapshotTransfer.to
          /\ snapshotTransfer.index \in 1..MaxLogLen
          /\ snapshotTransfer.prefix \in LogSet
          /\ snapshotTransfer.index = Len(snapshotTransfer.prefix)
     ELSE snapshotTransfer = NoSnapshotTransfer
  /\ DOMAIN applied = Nodes
  /\ DOMAIN applicationEpoch = Nodes
  /\ DOMAIN epochBaseIndex = Nodes
  /\ DOMAIN epochBaseState = Nodes
  /\ DOMAIN applicationState = Nodes
  /\ DOMAIN appliedThrough = Nodes
  /\ \A n \in Nodes :
       /\ applicationEpoch[n] \in 0..1
       /\ epochBaseIndex[n] \in 0..MaxLogLen
       /\ ApplicationStateOK(epochBaseState[n])
       /\ ApplicationStateOK(applicationState[n])
       /\ appliedThrough[n] \in 0..MaxLogLen
       /\ ApplicationEpochStateOK(n)
       /\ \A position \in 1..Len(applied[n]) :
            LET event == applied[n][position]
            IN /\ AppliedEventOK(event)
               /\ event.epoch <= applicationEpoch[n]
  /\ \A witness \in logicalPrefixLedger :
       /\ DOMAIN witness = {"index", "term", "prefix"}
       /\ witness.index \in 1..MaxLogLen
       /\ witness.term \in 1..MaxTerm
       /\ witness.prefix \in LogSet
  /\ \A committed \in committedLedger :
       /\ DOMAIN committed = {"index", "entry"}
       /\ committed.index \in 1..MaxLogLen
       /\ committed.entry \in EntrySet
  /\ DOMAIN commitWitnesses =
       {"witnessedCommits", "invalidCertificateSeen"}
  /\ \A witnessed \in commitWitnesses.witnessedCommits :
       /\ DOMAIN witnessed = {"index", "entry"}
       /\ witnessed.index \in 1..MaxLogLen
       /\ witnessed.entry \in EntrySet
  /\ commitWitnesses.invalidCertificateSeen \in BOOLEAN
  /\ AppliedConfigurationStateOK
  /\ EffectiveConfigurationStateOK
  /\ messages \in SUBSET MessageSet
  /\ \A m \in messages : MessageOK(m)
  /\ readRequests \in SUBSET ReadRequestSet
  /\ \A n \in Nodes :
       \A request \in ReadRequests :
         Cardinality({r \in readRequests :
             /\ r.node = n
             /\ r.request = request}) <= 1
  /\ readBarrierViolationSeen \in BOOLEAN

Init ==
  /\ currentTerm = [n \in Nodes |-> 0]
  /\ votedFor = [n \in Nodes |-> NoVote]
  /\ role = [n \in Nodes |-> Follower]
  /\ log = [n \in Nodes |-> <<>>]
  /\ commitIndex = [n \in Nodes |-> 0]
  /\ snapshotIndex = [n \in Nodes |-> 0]
  /\ snapshotPrefix = [n \in Nodes |-> <<>>]
  /\ compactionPending = [n \in Nodes |-> FALSE]
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = [n \in Nodes |-> <<>>]
  /\ applicationEpoch = [n \in Nodes |-> 0]
  /\ epochBaseIndex = [n \in Nodes |-> 0]
  /\ epochBaseState = [n \in Nodes |-> InitialApplicationState]
  /\ applicationState = [n \in Nodes |-> InitialApplicationState]
  /\ appliedThrough = [n \in Nodes |-> 0]
  /\ messages = {}
  /\ readRequests = {}
  /\ readBarrierViolationSeen = FALSE
  /\ membership = StableMembership(Nodes)
  /\ appliedConfigIndex = 0
  /\ effectiveMembership = StableMembership(Nodes)
  /\ effectiveConfigIndex = 0
  /\ electedLeaders = [t \in 1..MaxTerm |-> {}]
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {}
  /\ commitWitnesses = EmptyCommitWitnessHistory
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE

Timeout(n) ==
  /\ currentTerm[n] < MaxTerm
  /\ n \in ActiveVoters(effectiveMembership)
  /\ currentTerm' = [currentTerm EXCEPT ![n] = @ + 1]
  /\ votedFor' = [votedFor EXCEPT ![n] = n]
  /\ role' = [role EXCEPT ![n] = Candidate]
  /\ UNCHANGED <<log, commitIndex, messages, readRequests, readBarrierViolationSeen,
                  membership, appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars
  /\ UNCHANGED authorityVars

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
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    readRequests, readBarrierViolationSeen, membership, appliedConfigIndex,
                    effectiveMembership, effectiveConfigIndex,
                    electedLeaders, higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED historyVars

DeliverRequestVote(m) ==
  LET higher == m.term > currentTerm[m.to]
      eligibleVote == IF higher THEN NoVote ELSE votedFor[m.to]
      grant == /\ m.term >= currentTerm[m.to]
               /\ eligibleVote \in {NoVote, m.from}
               /\ m.from \in ActiveVoters(effectiveMembership)
               /\ m.to \in ActiveVoters(effectiveMembership)
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
    /\ UNCHANGED <<log, commitIndex, readRequests, readBarrierViolationSeen,
                    membership, appliedConfigIndex, effectiveMembership,
                    effectiveConfigIndex, electedLeaders>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED historyVars

BecomeLeader(n) ==
  LET electedConfiguration == EffectiveConfigurationFor(log[n])
      electedRole == [role EXCEPT ![n] = Leader]
  IN
    /\ role[n] = Candidate
    /\ n \in ActiveVoters(effectiveMembership)
    /\ n \in ActiveVoters(electedConfiguration.config)
    /\ QuorumNodes({v \in Nodes :
         /\ votedFor[v] = n
         /\ currentTerm[v] = currentTerm[n]})
    /\ role' = RolesAfterMembershipChange(
         electedRole, electedConfiguration.config)
    /\ effectiveMembership' = electedConfiguration.config
    /\ effectiveConfigIndex' = electedConfiguration.configIndex
    /\ RecordElection(n)
    /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages,
                    readRequests, readBarrierViolationSeen, membership, appliedConfigIndex,
                    higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED historyVars

ClientAppend(n, value) ==
  /\ role[n] = Leader
  /\ currentTerm[n] \in 1..MaxTerm
  /\ Len(log[n]) < MaxLogLen
  /\ log' = [log EXCEPT ![n] = Append(@, Entry(currentTerm[n], value))]
  /\ RecordLogicalPrefixes(log', snapshotIndex, snapshotPrefix)
  /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
  /\ UNCHANGED <<currentTerm, votedFor, role, commitIndex, messages,
                  readRequests, readBarrierViolationSeen, membership, appliedConfigIndex,
                  effectiveMembership, effectiveConfigIndex, electedLeaders,
                  committedLedger, commitWitnesses,
                  higherTermStepDownFailed>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars

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
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    readRequests, readBarrierViolationSeen, membership, appliedConfigIndex,
                    effectiveMembership, effectiveConfigIndex,
                    electedLeaders, higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED historyVars

DeliverAppend(m) ==
  LET higher == m.term > currentTerm[m.to]
      accept == /\ m.term >= currentTerm[m.to]
                /\ \/ m.from \in ActiveVoters(effectiveMembership)
                    \/ /\ m.term = currentTerm[m.from]
                       /\ PendingSelfRemoval(m.from)
                /\ m.to \in ActiveVoters(effectiveMembership)
                /\ CanAdoptLog(m.to, m.entries)
      acceptedConfiguration == EffectiveConfigurationFor(m.entries)
      authoritative == AuthoritativeLogReplacement(m, accept)
      baseRole == [role EXCEPT ![m.to] =
        IF higher \/ accept THEN Follower ELSE @]
      configuredRole ==
        RolesAfterMembershipChange(baseRole, acceptedConfiguration.config)
      nextLog == IF accept THEN [log EXCEPT ![m.to] = m.entries] ELSE log
      nextCommit ==
        IF accept
        THEN [commitIndex EXCEPT ![m.to] =
               AdvanceCommit(m.to, m.leaderCommit, Len(m.entries))]
        ELSE commitIndex
  IN
    /\ m \in messages
    /\ m.type = AppendEntries
    /\ messages' = messages \ {m}
    /\ currentTerm' = [currentTerm EXCEPT ![m.to] =
         IF higher THEN m.term ELSE @]
    /\ votedFor' = [votedFor EXCEPT ![m.to] =
         IF higher THEN NoVote ELSE @]
    /\ role' =
         IF authoritative
         THEN IF PendingSelfRemoval(m.from)
              THEN [configuredRole EXCEPT ![m.from] = baseRole[m.from]]
              ELSE configuredRole
         ELSE baseRole
    /\ log' = nextLog
    /\ commitIndex' = nextCommit
    /\ effectiveMembership' =
         IF authoritative
         THEN acceptedConfiguration.config
         ELSE effectiveMembership
    /\ effectiveConfigIndex' =
         IF authoritative
         THEN acceptedConfiguration.configIndex
         ELSE effectiveConfigIndex
    /\ RecordLogicalPrefixes(nextLog, snapshotIndex, snapshotPrefix)
    /\ RecordCommittedEntries(
         nextLog, snapshotIndex, snapshotPrefix, m.to,
         commitIndex[m.to], nextCommit[m.to])
    /\ RecordHigherTermOutcome(m.to, m.term, higher)
    /\ RecordAuthorityAcceptance(m.term, currentTerm[m.to], accept)
    /\ UNCHANGED <<readRequests, readBarrierViolationSeen, membership,
                    appliedConfigIndex, electedLeaders, commitWitnesses>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars

RoleAfterCommit(node, selfRemoval) ==
  IF selfRemoval
  THEN [role EXCEPT ![node] = Follower]
  ELSE role

Commit(n, i) ==
  LET preRole == role[n]
      preTerm == currentTerm[n]
      preEffectiveMembership == effectiveMembership
      preAuthorityMembership == CommitAuthorityMembership(n)
      preEffectiveConfigIndex == effectiveConfigIndex
      context == FrozenCommitContext(
        preRole, preTerm, preEffectiveMembership, preAuthorityMembership)
      selfRemoval == /\ PendingSelfRemoval(n)
                     /\ i >= preEffectiveConfigIndex
  IN
    /\ preRole = Leader
    /\ \/ n \in ActiveVoters(preEffectiveMembership)
        \/ selfRemoval
    /\ i \in (commitIndex[n] + 1)..Len(log[n])
    /\ LogicalEntry(n, i).term = preTerm
    /\ MembershipQuorum(
         preEffectiveMembership, MatchingReplicas(n, i))
    /\ commitIndex' = [commitIndex EXCEPT ![n] = i]
    /\ role' = RoleAfterCommit(n, selfRemoval)
    /\ RecordCommittedEntries(
         log, snapshotIndex, snapshotPrefix, n, commitIndex[n], i)
    /\ RecordCommitWitnesses(CommitCertificatesFor(
         n, commitIndex[n], i, context, preEffectiveConfigIndex))
    /\ RecordAuthorityAcceptance(preTerm, preTerm, TRUE)
    /\ UNCHANGED <<currentTerm, votedFor, log, messages,
                    readRequests, readBarrierViolationSeen, membership, appliedConfigIndex,
                    effectiveMembership, effectiveConfigIndex,
                    electedLeaders, logicalPrefixLedger,
                    higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars

Apply(n) ==
  LET next == appliedThrough[n] + 1
      entry == LogicalEntry(n, next)
      priorState == applicationState[n]
      resultState == ApplyEntry(priorState, entry)
      isNewConfiguration ==
        /\ entry.kind = ConfigurationEntryKind
        /\ next > appliedConfigIndex
  IN
    /\ appliedThrough[n] < commitIndex[n]
    /\ RecordApplication(n, next, entry, priorState, resultState)
    /\ applicationState' = [applicationState EXCEPT ![n] = resultState]
    /\ appliedThrough' = [appliedThrough EXCEPT ![n] = next]
    /\ membership' =
         IF isNewConfiguration THEN entry.input ELSE membership
    /\ appliedConfigIndex' =
         IF isNewConfiguration THEN next ELSE appliedConfigIndex
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex, messages,
                    readRequests, readBarrierViolationSeen, effectiveMembership,
                    effectiveConfigIndex, electedLeaders,
                    applicationEpoch, epochBaseIndex, epochBaseState>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED historyVars
    /\ UNCHANGED authorityVars

ApplicationStateLoss(n) ==
  /\ applicationEpoch[n] = 0
  /\ appliedThrough[n] > 0
  /\ applicationEpoch' = [applicationEpoch EXCEPT ![n] = 1]
  /\ epochBaseIndex' = [epochBaseIndex EXCEPT ![n] = 0]
  /\ epochBaseState' = [epochBaseState EXCEPT ![n] = InitialApplicationState]
  /\ applicationState' = [applicationState EXCEPT ![n] = InitialApplicationState]
  /\ appliedThrough' = [appliedThrough EXCEPT ![n] = 0]
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex, applied,
                  messages, readRequests, readBarrierViolationSeen, membership,
                  appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED historyVars
  /\ UNCHANGED authorityVars

Restart(n) ==
  /\ role[n] # Follower
  /\ role' = [role EXCEPT ![n] = Follower]
  /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages,
                  readRequests, readBarrierViolationSeen, membership, appliedConfigIndex,
                  effectiveMembership, effectiveConfigIndex, electedLeaders>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars
  /\ UNCHANGED authorityVars

CreateSnapshot(n) ==
  LET index == appliedThrough[n]
      prefix == LogicalPrefix(n, index)
  IN
    /\ index > snapshotIndex[n]
    /\ snapshotIndex' = [snapshotIndex EXCEPT ![n] = index]
    /\ snapshotPrefix' = [snapshotPrefix EXCEPT ![n] = prefix]
    /\ compactionPending' = [compactionPending EXCEPT ![n] = TRUE]
    /\ RecordLogicalPrefixes(log, snapshotIndex', snapshotPrefix')
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    snapshotTransfer, messages,
                    readRequests, readBarrierViolationSeen, membership, appliedConfigIndex,
                    effectiveMembership, effectiveConfigIndex,
                    electedLeaders, committedLedger, commitWitnesses>>
    /\ UNCHANGED applicationVars
    /\ UNCHANGED authorityVars

TransferSnapshot(from, to) ==
  /\ role[from] = Leader
  /\ from # to
  /\ snapshotIndex[from] > 0
  /\ snapshotIndex[to] < snapshotIndex[from]
  /\ appliedThrough[to] <= snapshotIndex[from]
  /\ applicationEpoch[to] = 0
  /\ ~snapshotTransfer.active
  /\ snapshotTransfer' =
       [active |-> TRUE,
        term |-> currentTerm[from],
        from |-> from,
        to |-> to,
        index |-> snapshotIndex[from],
        prefix |-> snapshotPrefix[from]]
  /\ RecordAuthorityAcceptance(currentTerm[from], currentTerm[from], TRUE)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  snapshotIndex, snapshotPrefix, compactionPending,
                  messages, readRequests, readBarrierViolationSeen, membership,
                  appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders,
                  higherTermStepDownFailed>>
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars

InstallSnapshotLog(node, prefix) ==
  IF /\ Len(prefix) <= Len(log[node])
     /\ Prefix(log[node], Len(prefix)) = prefix
  THEN log[node]
  ELSE prefix

InstallSnapshot ==
  LET transfer == snapshotTransfer
      node == transfer.to
      nextLog == [log EXCEPT ![node] = InstallSnapshotLog(node, transfer.prefix)]
      nextCommit == [commitIndex EXCEPT ![node] = Max(@, transfer.index)]
      nextConfiguration == LatestConfigurationIn(transfer.prefix)
      restoredState == StateAfterEntries(transfer.prefix)
  IN
    /\ transfer.active
    /\ transfer.term >= currentTerm[node]
    /\ transfer.index > snapshotIndex[node]
    /\ transfer.index >= appliedThrough[node]
    /\ applicationEpoch[node] = 0
    /\ currentTerm' = [currentTerm EXCEPT ![node] = transfer.term]
    /\ votedFor' = [votedFor EXCEPT ![node] =
         IF transfer.term > currentTerm[node] THEN NoVote ELSE @]
    /\ role' = [role EXCEPT ![node] = Follower]
    /\ log' = nextLog
    /\ commitIndex' = nextCommit
    /\ snapshotIndex' = [snapshotIndex EXCEPT ![node] = transfer.index]
    /\ snapshotPrefix' = [snapshotPrefix EXCEPT ![node] = transfer.prefix]
    /\ compactionPending' = [compactionPending EXCEPT ![node] = FALSE]
    /\ snapshotTransfer' = NoSnapshotTransfer
    /\ applicationEpoch' = [applicationEpoch EXCEPT ![node] = 1]
    /\ epochBaseIndex' = [epochBaseIndex EXCEPT ![node] = transfer.index]
    /\ epochBaseState' = [epochBaseState EXCEPT ![node] = restoredState]
    /\ applicationState' = [applicationState EXCEPT ![node] = restoredState]
    /\ appliedThrough' = [appliedThrough EXCEPT ![node] = transfer.index]
    /\ membership' =
         IF nextConfiguration.configIndex > appliedConfigIndex
         THEN nextConfiguration.config
         ELSE membership
    /\ appliedConfigIndex' =
         Max(appliedConfigIndex, nextConfiguration.configIndex)
    /\ effectiveMembership' =
         IF nextConfiguration.configIndex > effectiveConfigIndex
         THEN nextConfiguration.config
         ELSE effectiveMembership
    /\ effectiveConfigIndex' =
         Max(effectiveConfigIndex, nextConfiguration.configIndex)
    /\ RecordLogicalPrefixes(nextLog, snapshotIndex', snapshotPrefix')
    /\ RecordCommittedEntries(
         nextLog, snapshotIndex', snapshotPrefix', node,
         commitIndex[node], nextCommit[node])
    /\ RecordHigherTermOutcome(
         node, transfer.term, transfer.term > currentTerm[node])
    /\ RecordAuthorityAcceptance(transfer.term, currentTerm[node], TRUE)
    /\ UNCHANGED <<applied, messages, readRequests, readBarrierViolationSeen,
                    electedLeaders, commitWitnesses>>

CompactSnapshot(n) ==
  /\ compactionPending[n]
  /\ compactionPending' = [compactionPending EXCEPT ![n] = FALSE]
  /\ RecordLogicalPrefixes(log, snapshotIndex, snapshotPrefix)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  snapshotIndex, snapshotPrefix, snapshotTransfer,
                  messages, readRequests, readBarrierViolationSeen, membership,
                  appliedConfigIndex, effectiveMembership,
                  effectiveConfigIndex, electedLeaders,
                  committedLedger, commitWitnesses>>
  /\ UNCHANGED applicationVars
  /\ UNCHANGED authorityVars

EnterJoint(n, newVoters) ==
  LET next == JointMembership(effectiveMembership.old, newVoters)
      nextIndex == Len(log[n]) + 1
      nextLog == [log EXCEPT ![n] =
        Append(@, ConfigurationEntry(currentTerm[n], next))]
  IN
    /\ effectiveMembership.phase = StableConfig
    /\ effectiveMembership.old = Nodes
    /\ membership.phase = StableConfig
    /\ newVoters \in VoterSets
    /\ newVoters # effectiveMembership.old
    /\ role[n] = Leader
    /\ CommittedEntriesHeldBy(newVoters)
    /\ currentTerm[n] \in 1..MaxTerm
    /\ Len(log[n]) = appliedThrough[n]
    /\ Len(log[n]) < MaxLogLen
    /\ log' = nextLog
    /\ effectiveMembership' = next
    /\ effectiveConfigIndex' = nextIndex
    /\ role' = RoleAfterMembershipChange(next)
    /\ RecordLogicalPrefixes(nextLog, snapshotIndex, snapshotPrefix)
    /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
    /\ UNCHANGED <<currentTerm, votedFor, commitIndex, messages,
                    readRequests, readBarrierViolationSeen, membership,
                    appliedConfigIndex, electedLeaders,
                    committedLedger, commitWitnesses,
                    higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars

LeaveJoint(n) ==
  LET next == StableMembership(effectiveMembership.new)
      nextIndex == Len(log[n]) + 1
      nextLog == [log EXCEPT ![n] =
        Append(@, ConfigurationEntry(currentTerm[n], next))]
      nextRole == RoleAfterMembershipChange(next)
  IN
    /\ effectiveMembership.phase = JointConfig
    /\ membership.phase = JointConfig
    /\ role[n] = Leader
    /\ CommittedEntriesHeldBy(effectiveMembership.new)
    /\ currentTerm[n] \in 1..MaxTerm
    /\ Len(log[n]) = appliedThrough[n]
    /\ Len(log[n]) < MaxLogLen
    /\ log' = nextLog
    /\ effectiveMembership' = next
    /\ effectiveConfigIndex' = nextIndex
    /\ role' =
         IF n \in ActiveVoters(next)
         THEN nextRole
         ELSE [nextRole EXCEPT ![n] = role[n]]
    /\ RecordLogicalPrefixes(nextLog, snapshotIndex, snapshotPrefix)
    /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
    /\ UNCHANGED <<currentTerm, votedFor, commitIndex, messages,
                    readRequests, readBarrierViolationSeen, membership,
                    appliedConfigIndex, electedLeaders,
                    committedLedger, commitWitnesses,
                    higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars

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
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    messages, readBarrierViolationSeen, membership, appliedConfigIndex,
                    effectiveMembership, effectiveConfigIndex,
                    electedLeaders, higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED historyVars

GrantRead(n, request) ==
  \E read \in readRequests :
    LET grant ==
          [node |-> n, request |-> request, readIndex |-> commitIndex[n]]
    IN
      /\ read.node = n
      /\ read.request = request
      /\ role[n] = Leader
      /\ commitIndex[n] >= read.committedFloor
      /\ RecordReadGrant(grant)
      /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
      /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                      messages, readRequests, membership,
                      appliedConfigIndex, effectiveMembership,
                      effectiveConfigIndex, electedLeaders,
                      higherTermStepDownFailed>>
      /\ UNCHANGED snapshotVars
      /\ UNCHANGED applicationVars
      /\ UNCHANGED historyVars

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
  \/ \E n \in Nodes : ApplicationStateLoss(n)
  \/ \E n \in Nodes : Restart(n)
  \/ \E n \in Nodes : CreateSnapshot(n)
  \/ \E from, to \in Nodes : TransferSnapshot(from, to)
  \/ InstallSnapshot
  \/ \E n \in Nodes : CompactSnapshot(n)
  \/ \E n \in Nodes, voters \in VoterSets : EnterJoint(n, voters)
  \/ \E n \in Nodes : LeaveJoint(n)
  \/ \E n \in Nodes, request \in ReadRequests : RegisterRead(n, request)
  \/ \E n \in Nodes, request \in ReadRequests : GrantRead(n, request)

Spec == Init /\ [][Next]_vars

ElectionSafety ==
  \A t \in 1..MaxTerm :
    Cardinality(electedLeaders[t]) <= 1

LogMatchingFor(logs, snapshotIndexes, snapshotPrefixes) ==
  \A a, b \in Nodes :
    \A i \in 1..MaxLogLen :
      (/\ i \in 1..Len(logs[a])
       /\ i \in 1..Len(logs[b])
       /\ LogicalEntryFrom(logs, snapshotIndexes, snapshotPrefixes, a, i).term
            = LogicalEntryFrom(logs, snapshotIndexes, snapshotPrefixes, b, i).term)
      => LogicalPrefixFrom(logs, snapshotIndexes, snapshotPrefixes, a, i)
           = LogicalPrefixFrom(logs, snapshotIndexes, snapshotPrefixes, b, i)

LogMatching ==
  /\ SnapshotIdentitySoundFor(
       log, snapshotIndex, snapshotPrefix, compactionPending)
  /\ LogicalPrefixLedgerSound
  /\ LogMatchingFor(log, snapshotIndex, snapshotPrefix)

LeaderCompleteness ==
  \A leader \in Nodes :
    \A committed \in committedLedger :
      (/\ role[leader] = Leader
       /\ currentTerm[leader] > committed.entry.term)
      => /\ committed.index \in 1..Len(log[leader])
         /\ LogicalEntry(leader, committed.index) = committed.entry

CommittedPrefixStability ==
  /\ \A a, b \in committedLedger :
       a.index = b.index => a.entry = b.entry
  /\ \A a, b \in Nodes :
       \A i \in 1..MaxLogLen :
         (/\ i \in 1..commitIndex[a]
          /\ i \in 1..commitIndex[b])
         => LogicalEntry(a, i) = LogicalEntry(b, i)

StateMachineSafety ==
  /\ \A n \in Nodes : AppliedHistorySound(applied[n])
  /\ \A a, b \in Nodes :
       \A pa \in 1..Len(applied[a]) :
         \A pb \in 1..Len(applied[b]) :
           applied[a][pa].index = applied[b][pb].index
           => /\ applied[a][pa].entry = applied[b][pb].entry
              /\ applied[a][pa].priorState = applied[b][pb].priorState
              /\ applied[a][pa].resultState = applied[b][pb].resultState

StaleLeaderFencing ==
  /\ ~higherTermStepDownFailed
  /\ ~staleAuthorityAccepted

CommittedEntriesHaveQuorum ==
  /\ ~commitWitnesses.invalidCertificateSeen
  /\ \A committed \in committedLedger :
       committed \in commitWitnesses.witnessedCommits
  /\ \A n \in Nodes :
       \A index \in 1..commitIndex[n] :
         \E committed \in committedLedger :
           /\ committed.index = index
           /\ committed.entry = LogicalEntry(n, index)

ReadBarrierLinearizability ==
  ~readBarrierViolationSeen

====
