---- MODULE Raft ----
EXTENDS Naturals, Sequences, FiniteSets, TLC

\* This is a small, bounded design model. It is not implementation code and it
\* intentionally does not import or depend on rafter.
\* The production specification is safety-only. Fair-schedule liveness evidence
\* is owned by the bounded simulator, whose scheduler states its timing bounds.

CONSTANTS Nodes, Values, MaxTerm, MaxLogLen, ReadRequests

\* Bounded profiles quotient only independent model-value renamings. The weekly
\* profile intentionally omits this symmetry and explores the full graph.
ModelPermutations ==
  {[modelValue \in Nodes \cup Values \cup ReadRequests |->
      CASE modelValue \in Nodes -> nodePermutation[modelValue]
        [] modelValue \in Values -> valuePermutation[modelValue]
        [] OTHER -> requestPermutation[modelValue]] :
    nodePermutation \in Permutations(Nodes),
    valuePermutation \in Permutations(Values),
    requestPermutation \in Permutations(ReadRequests)}

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
          applied, applicationBases, applicationTransitions,
          messages, readRequests, readBarrierViolationSeen,
          electedLeaders, logicalPrefixLedger, committedLedger,
          commitWitnesses,
          higherTermStepDownFailed,
          staleAuthorityAccepted,
          frozenAppendAuthorityFailed

vars == << currentTerm, votedFor, role, log, commitIndex,
          snapshotIndex, snapshotPrefix, compactionPending, snapshotTransfer,
          applied, applicationBases, applicationTransitions,
          messages, readRequests, readBarrierViolationSeen,
          electedLeaders, logicalPrefixLedger, committedLedger,
          commitWitnesses,
          higherTermStepDownFailed,
          staleAuthorityAccepted,
          frozenAppendAuthorityFailed >>

snapshotVars ==
  <<snapshotIndex, snapshotPrefix, compactionPending, snapshotTransfer>>

applicationVars ==
  <<applied, applicationBases, applicationTransitions>>

historyVars == <<logicalPrefixLedger, committedLedger, commitWitnesses>>

authorityVars ==
  <<higherTermStepDownFailed, staleAuthorityAccepted, frozenAppendAuthorityFailed>>

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b

CommandEntryKind == "Command"
ConfigurationEntryKind == "Configuration"

VoterSets ==
  {voters \in SUBSET Nodes :
      voters # {}}

StableMembership(voters) ==
  [phase |-> StableConfig, old |-> voters, new |-> voters]

JointMembership(oldVoters, newVoters) ==
  [phase |-> JointConfig, old |-> oldVoters, new |-> newVoters]

OneVoterChange(oldVoters, newVoters) ==
  /\ oldVoters \in VoterSets
  /\ newVoters \in VoterSets
  /\ oldVoters # newVoters
  /\ Cardinality(
       (oldVoters \ newVoters) \cup (newVoters \ oldVoters)) = 1

MembershipSet ==
  {StableMembership(voters) : voters \in VoterSets}
  \cup {JointMembership(oldVoters, newVoters) :
      oldVoters \in VoterSets, newVoters \in VoterSets}

JointVoterChanges ==
  {pair \in VoterSets \X VoterSets :
    OneVoterChange(pair[1], pair[2])}

Entry(term, value) ==
  [term |-> term, kind |-> CommandEntryKind, input |-> value]

ConfigurationEntry(term, config) ==
  [term |-> term, kind |-> ConfigurationEntryKind, input |-> config]

ConfigurationSet ==
  {StableMembership(voters) : voters \in VoterSets}
  \cup {JointMembership(pair[1], pair[2]) : pair \in JointVoterChanges}

EntrySet ==
  {Entry(t, v) : t \in 1..MaxTerm, v \in Values}
  \cup {ConfigurationEntry(t, config) :
      t \in 1..MaxTerm, config \in ConfigurationSet}

EntryOK(e) ==
  /\ e \in EntrySet
  /\ e.term \in 1..MaxTerm
  /\ e.kind \in {CommandEntryKind, ConfigurationEntryKind}
  /\ IF e.kind = CommandEntryKind
     THEN e.input \in Values
     ELSE e.input \in ConfigurationSet

LogOK(s) ==
  /\ s \in Seq(EntrySet)
  /\ Len(s) <= MaxLogLen

ReferenceStateOK(s) ==
  /\ s \in Seq(Values)
  /\ Len(s) <= MaxLogLen

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
  /\ ReferenceStateOK(state.referenceState)
  /\ state.membership \in MembershipSet

ApplyEntry(state, entry) ==
  IF entry.kind = CommandEntryKind
  THEN [state EXCEPT !.referenceState = Append(@, entry.input)]
  ELSE [state EXCEPT !.membership = entry.input]

StateAfterFourEntries(entries) ==
  ApplyEntry(
    ApplyEntry(
      ApplyEntry(ApplyEntry(InitialApplicationState, entries[1]), entries[2]),
      entries[3]),
    entries[4])

StateAfterEntries(entries) ==
  CASE Len(entries) = 0 -> InitialApplicationState
    [] Len(entries) = 1 -> ApplyEntry(InitialApplicationState, entries[1])
    [] Len(entries) = 2 ->
         ApplyEntry(ApplyEntry(InitialApplicationState, entries[1]), entries[2])
    [] Len(entries) = 3 ->
         ApplyEntry(
           ApplyEntry(ApplyEntry(InitialApplicationState, entries[1]), entries[2]),
           entries[3])
    [] Len(entries) = 4 -> StateAfterFourEntries(entries)
    [] Len(entries) = 5 ->
         ApplyEntry(StateAfterFourEntries(entries), entries[5])
    [] OTHER ->
         ApplyEntry(
           ApplyEntry(StateAfterFourEntries(entries), entries[5]), entries[6])

AppliedCursor(epoch, through, state) ==
  [epoch |-> epoch, through |-> through, state |-> state]

ApplicationBase(node, index, state) ==
  [node |-> node, index |-> index, state |-> state]

ApplicationTransition(node, index, priorState, entry, resultState) ==
  [node |-> node,
   index |-> index,
   priorState |-> priorState,
   entry |-> entry,
   resultState |-> resultState]

AppliedCursorOK(cursor) ==
  /\ DOMAIN cursor = {"epoch", "through", "state"}
  /\ cursor.epoch \in 0..1
  /\ cursor.through \in 0..MaxLogLen
  /\ ApplicationStateOK(cursor.state)

ApplicationBaseOK(base) ==
  /\ DOMAIN base = {"node", "index", "state"}
  /\ base.node \in Nodes
  /\ base.index \in 0..MaxLogLen
  /\ ApplicationStateOK(base.state)

ApplicationTransitionOK(transition) ==
  /\ DOMAIN transition =
       {"node", "index", "priorState", "entry", "resultState"}
  /\ transition.node \in Nodes
  /\ transition.index \in 1..MaxLogLen
  /\ ApplicationStateOK(transition.priorState)
  /\ EntryOK(transition.entry)
  /\ ApplicationStateOK(transition.resultState)

ApplicationEpoch(n) == applied[n].epoch

ApplicationState(n) == applied[n].state

AppliedThrough(n) == applied[n].through

ApplicationObservationWitnesses(node) ==
  {[index |-> transition.index,
    entry |-> transition.entry,
    resultState |-> transition.resultState] :
      transition \in
        {candidate \in applicationTransitions : candidate.node = node}}

ApplicationStateWitnesses(node) ==
  {[index |-> base.index, state |-> base.state] :
      base \in {candidate \in applicationBases : candidate.node = node}}
  \cup {[index |-> transition.index, state |-> transition.resultState] :
      transition \in
        {candidate \in applicationTransitions : candidate.node = node}}

ApplicationTransitionSound(transition) ==
  transition.resultState =
    ApplyEntry(transition.priorState, transition.entry)

ApplicationTransitionLinked(transition) ==
  [index |-> transition.index - 1, state |-> transition.priorState]
    \in ApplicationStateWitnesses(transition.node)

CurrentApplicationStateWitnessed(node) ==
  [index |-> AppliedThrough(node), state |-> ApplicationState(node)]
    \in ApplicationStateWitnesses(node)

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

AppliedConfiguration(node) ==
  LatestConfigurationIn(LogicalPrefix(node, AppliedThrough(node)))

EffectiveConfiguration(node) ==
  LatestConfigurationIn(LogicalPrefix(node, Len(log[node])))

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

LogicalPrefixLedgerSound ==
  \A a, b \in logicalPrefixLedger :
    (a.index = b.index /\ a.term = b.term) => a.prefix = b.prefix

TermClosed(terms, term) ==
  \A n \in Nodes : terms[n] > term

ConflictingPrefixWitness(witness, observed) ==
  \E other \in observed :
    /\ witness.index = other.index
    /\ witness.term = other.term
    /\ witness.prefix # other.prefix

RetainedLogicalPrefixes(observed, terms) ==
  {witness \in observed :
    \/ ~TermClosed(terms, witness.term)
    \/ ConflictingPrefixWitness(witness, observed)}

RecordLogicalPrefixes(logs, snapshotIndexes, snapshotPrefixes, terms) ==
  LET observed ==
        logicalPrefixLedger \cup
          LogicalPrefixWitnesses(logs, snapshotIndexes, snapshotPrefixes)
  IN logicalPrefixLedger' = RetainedLogicalPrefixes(observed, terms)

RetireLogicalPrefixes(terms) ==
  logicalPrefixLedger' = RetainedLogicalPrefixes(logicalPrefixLedger, terms)

RetainedElections(elections, terms) ==
  [term \in 1..MaxTerm |->
    IF TermClosed(terms, term) THEN {} ELSE elections[term]]

RecordElection(node) ==
  electedLeaders' = RetainedElections(
    [electedLeaders EXCEPT ![currentTerm[node]] = @ \cup {node}],
    currentTerm)

RecordHigherTermOutcome(node, evidenceTerm, observedHigherTerm) ==
  higherTermStepDownFailed' =
    IF /\ observedHigherTerm
       /\ (currentTerm'[node] # evidenceTerm \/ role'[node] # Follower)
    THEN TRUE
    ELSE higherTermStepDownFailed

RecordAuthorityAcceptance(authorityTerm, knownTerm, accepted) ==
  /\ staleAuthorityAccepted' =
       IF accepted /\ authorityTerm < knownTerm
       THEN TRUE
       ELSE staleAuthorityAccepted
  /\ UNCHANGED frozenAppendAuthorityFailed

RecordAppendOutcome(message, knownTerm, accepted, receiverWouldAccept) ==
  /\ staleAuthorityAccepted' =
       IF accepted /\ message.term < knownTerm
       THEN TRUE
       ELSE staleAuthorityAccepted
  /\ frozenAppendAuthorityFailed' =
       IF /\ message.senderPendingSelfRemoval
          /\ role[message.from] # Leader
          /\ receiverWouldAccept
          /\ ~accepted
       THEN TRUE
       ELSE frozenAppendAuthorityFailed

StartApplicationEpoch(node, baseIndex, baseState) ==
  LET base == ApplicationBase(node, baseIndex, baseState)
  IN
    /\ applied' = [applied EXCEPT ![node] =
         AppliedCursor(ApplicationEpoch(node) + 1, baseIndex, baseState)]
    /\ applicationBases' = applicationBases \cup {base}
    /\ UNCHANGED applicationTransitions

RecordApplication(node, entry, resultState) ==
  LET index == AppliedThrough(node) + 1
      transition == ApplicationTransition(
        node, index, ApplicationState(node), entry, resultState)
  IN
    /\ applied' = [applied EXCEPT ![node] =
         AppliedCursor(ApplicationEpoch(node), index, resultState)]
    /\ applicationTransitions' = applicationTransitions \cup {transition}
    /\ UNCHANGED applicationBases

CommittedEntry(index, entry, committedInTerm) ==
  [index |-> index, entry |-> entry, committedInTerm |-> committedInTerm]

CommittedEntriesFor(
    logs, snapshotIndexes, snapshotPrefixes, node, oldFloor, newFloor,
    committedInTerm) ==
  {[index |-> index,
    entry |-> LogicalEntryFrom(
      logs, snapshotIndexes, snapshotPrefixes, node, index),
    committedInTerm |-> committedInTerm] :
      index \in (oldFloor + 1)..newFloor}

SameCommittedIdentity(left, right) ==
  /\ left.index = right.index
  /\ left.entry = right.entry

RetainedCommittedEntries(existing, candidates) ==
  {committed \in existing :
    ~\E candidate \in candidates :
      /\ SameCommittedIdentity(committed, candidate)
      /\ candidate.committedInTerm < committed.committedInTerm}
  \cup {candidate \in candidates :
    ~\E committed \in existing :
      /\ SameCommittedIdentity(committed, candidate)
      /\ committed.committedInTerm <= candidate.committedInTerm}

CommittedLedgerCanonical(entries) ==
  \A left, right \in entries :
    SameCommittedIdentity(left, right) =>
      left.committedInTerm = right.committedInTerm

RecordCommittedEntries(
    logs, snapshotIndexes, snapshotPrefixes, node, oldFloor, newFloor,
    committedInTerm) ==
  LET candidates == CommittedEntriesFor(
        logs, snapshotIndexes, snapshotPrefixes, node, oldFloor, newFloor,
        committedInTerm)
  IN committedLedger' = RetainedCommittedEntries(committedLedger, candidates)

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

RetainedMessages(pending, terms) ==
  {message \in pending : message.term >= terms[message.to]}

NoSnapshotTransfer ==
  [active |-> FALSE, term |-> 0, from |-> NoVote, to |-> NoVote,
   index |-> 0, prefix |-> <<>>]

LastTerm(s) ==
  IF Len(s) = 0 THEN 0 ELSE s[Len(s)].term

UpToDate(candidate, voter) ==
  \/ LastTerm(log[candidate]) > LastTerm(log[voter])
  \/ /\ LastTerm(log[candidate]) = LastTerm(log[voter])
     /\ Len(log[candidate]) >= Len(log[voter])

ActiveVoters(config) ==
  IF config.phase = StableConfig THEN config.old ELSE config.old \cup config.new

PendingSelfRemoval(node) ==
  LET effective == EffectiveConfiguration(node)
  IN IF /\ effective.configIndex \in 1..Len(log[node])
        /\ effective.configIndex > commitIndex[node]
     THEN LET entry == LogicalEntry(node, effective.configIndex)
              prior == PriorConfigurationFor(node, effective.configIndex)
       IN /\ role[node] = Leader
          /\ currentTerm[node] = entry.term
          /\ entry.kind = ConfigurationEntryKind
          /\ entry.input = effective.config
          /\ effective.config.phase = StableConfig
          /\ node \notin ActiveVoters(effective.config)
          /\ prior.configIndex < effective.configIndex
          /\ prior.config.phase = JointConfig
          /\ prior.config.new = effective.config.old
          /\ node \in ActiveVoters(prior.config)
     ELSE FALSE

CommitAuthorityMembership(node) ==
  IF PendingSelfRemoval(node)
  THEN PriorConfigurationFor(
         node, EffectiveConfiguration(node).configIndex).config
  ELSE EffectiveConfiguration(node).config

StableQuorum(voters, ns) ==
  2 * Cardinality(ns \cap voters) > Cardinality(voters)

MembershipQuorum(config, ns) ==
  IF config.phase = StableConfig
  THEN StableQuorum(config.old, ns)
  ELSE /\ StableQuorum(config.old, ns)
       /\ StableQuorum(config.new, ns)

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

CanAdoptLog(n, entries, authorityTerm) ==
  /\ LogOK(entries)
  /\ \/ Len(entries) >= Len(log[n])
     \/ authorityTerm > currentTerm[n]
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
     /\ m.senderMembership \in MembershipSet
     /\ m.senderPendingSelfRemoval \in BOOLEAN
     /\ \/ m.from \in ActiveVoters(m.senderMembership)
        \/ m.senderPendingSelfRemoval
     /\ \A i \in 1..Len(m.entries) : m.entries[i].term <= m.term

AppliedConfigurationStateOK ==
  \A n \in Nodes :
    /\ AppliedConfiguration(n).configIndex <= AppliedThrough(n)
    /\ AppliedConfiguration(n).config \in MembershipSet

EffectiveConfigurationStateOK ==
  \A n \in Nodes :
    /\ AppliedConfiguration(n).configIndex
         <= EffectiveConfiguration(n).configIndex
    /\ EffectiveConfiguration(n).configIndex <= Len(log[n])
    /\ EffectiveConfiguration(n).config \in MembershipSet

TypeOK ==
  /\ MaxLogLen \in 1..6
  /\ currentTerm \in [Nodes -> 0..MaxTerm]
  /\ votedFor \in [Nodes -> (Nodes \cup {NoVote})]
  /\ role \in [Nodes -> {Follower, Candidate, Leader}]
  /\ electedLeaders \in [1..MaxTerm -> SUBSET Nodes]
  /\ electedLeaders = RetainedElections(electedLeaders, currentTerm)
  /\ \A n \in Nodes :
       role[n] = Leader =>
         /\ currentTerm[n] \in 1..MaxTerm
         /\ n \in electedLeaders[currentTerm[n]]
  /\ higherTermStepDownFailed \in BOOLEAN
  /\ staleAuthorityAccepted \in BOOLEAN
  /\ frozenAppendAuthorityFailed \in BOOLEAN
  /\ \A n \in Nodes :
       n \notin ActiveVoters(EffectiveConfiguration(n).config) =>
         \/ role[n] = Follower
         \/ PendingSelfRemoval(n)
  /\ DOMAIN log = Nodes
  /\ \A n \in Nodes : LogOK(log[n])
  /\ commitIndex \in [Nodes -> 0..MaxLogLen]
  /\ \A n \in Nodes : commitIndex[n] <= Len(log[n])
  /\ DOMAIN snapshotIndex = Nodes
  /\ DOMAIN snapshotPrefix = Nodes
  /\ compactionPending \in [Nodes -> BOOLEAN]
  /\ \A n \in Nodes :
       /\ snapshotIndex[n] \in 0..MaxLogLen
       /\ LogOK(snapshotPrefix[n])
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
          /\ LogOK(snapshotTransfer.prefix)
          /\ snapshotTransfer.index = Len(snapshotTransfer.prefix)
     ELSE snapshotTransfer = NoSnapshotTransfer
  /\ DOMAIN applied = Nodes
  /\ \A n \in Nodes :
       /\ AppliedCursorOK(applied[n])
       /\ ApplicationEpoch(n) \in 0..1
       /\ ApplicationStateOK(ApplicationState(n))
       /\ AppliedThrough(n) \in 0..MaxLogLen
       /\ AppliedThrough(n) <= commitIndex[n]
       /\ CurrentApplicationStateWitnessed(n)
  /\ IsFiniteSet(applicationBases)
  /\ \A base \in applicationBases : ApplicationBaseOK(base)
  /\ IsFiniteSet(applicationTransitions)
  /\ \A transition \in applicationTransitions :
       ApplicationTransitionOK(transition)
  /\ \A witness \in logicalPrefixLedger :
       /\ DOMAIN witness = {"index", "term", "prefix"}
       /\ witness.index \in 1..MaxLogLen
       /\ witness.term \in 1..MaxTerm
       /\ LogOK(witness.prefix)
  /\ logicalPrefixLedger =
       RetainedLogicalPrefixes(logicalPrefixLedger, currentTerm)
  /\ \A committed \in committedLedger :
       /\ DOMAIN committed = {"index", "entry", "committedInTerm"}
       /\ committed.index \in 1..MaxLogLen
       /\ EntryOK(committed.entry)
       /\ committed.committedInTerm \in 1..MaxTerm
       /\ committed.entry.term <= committed.committedInTerm
  /\ CommittedLedgerCanonical(committedLedger)
  /\ DOMAIN commitWitnesses =
       {"witnessedCommits", "invalidCertificateSeen"}
  /\ \A witnessed \in commitWitnesses.witnessedCommits :
       /\ DOMAIN witnessed = {"index", "entry"}
       /\ witnessed.index \in 1..MaxLogLen
       /\ EntryOK(witnessed.entry)
  /\ commitWitnesses.invalidCertificateSeen \in BOOLEAN
  /\ AppliedConfigurationStateOK
  /\ EffectiveConfigurationStateOK
  /\ IsFiniteSet(messages)
  /\ \A m \in messages : MessageOK(m)
  /\ messages = RetainedMessages(messages, currentTerm)
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
  /\ applied = [n \in Nodes |-> AppliedCursor(0, 0, InitialApplicationState)]
  /\ applicationBases =
       {ApplicationBase(n, 0, InitialApplicationState) : n \in Nodes}
  /\ applicationTransitions = {}
  /\ messages = {}
  /\ readRequests = {}
  /\ readBarrierViolationSeen = FALSE
  /\ electedLeaders = [t \in 1..MaxTerm |-> {}]
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {}
  /\ commitWitnesses = EmptyCommitWitnessHistory
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE
  /\ frozenAppendAuthorityFailed = FALSE

Timeout(n) ==
  /\ currentTerm[n] < MaxTerm
  /\ n \in ActiveVoters(EffectiveConfiguration(n).config)
  /\ currentTerm' = [currentTerm EXCEPT ![n] = @ + 1]
  /\ votedFor' = [votedFor EXCEPT ![n] = n]
  /\ role' = [role EXCEPT ![n] = Candidate]
  /\ messages' = RetainedMessages(messages, currentTerm')
  /\ electedLeaders' = RetainedElections(electedLeaders, currentTerm')
  /\ RetireLogicalPrefixes(currentTerm')
  /\ UNCHANGED <<log, commitIndex, readRequests, readBarrierViolationSeen>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED <<committedLedger, commitWitnesses>>
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
    /\ messages' = RetainedMessages(messages \cup {msg}, currentTerm)
    /\ RecordAuthorityAcceptance(currentTerm[c], currentTerm[c], TRUE)
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    readRequests, readBarrierViolationSeen,
                    electedLeaders, higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED historyVars

VoteTermAndDurableEligible(m) ==
  LET higher == m.term > currentTerm[m.to]
      eligibleVote == IF higher THEN NoVote ELSE votedFor[m.to]
  IN /\ m.term >= currentTerm[m.to]
     /\ eligibleVote \in {NoVote, m.from}

VoteMembershipEligible(m) ==
  /\ m.from \in ActiveVoters(
       EffectiveConfiguration(m.from).config)
  /\ m.from \in ActiveVoters(
       EffectiveConfiguration(m.to).config)
  /\ m.to \in ActiveVoters(
       EffectiveConfiguration(m.to).config)

VoteIsFresh(m) == UpToDate(m.from, m.to)

DeliverRequestVote(m) ==
  LET higher == m.term > currentTerm[m.to]
      grant == /\ VoteTermAndDurableEligible(m)
               /\ VoteMembershipEligible(m)
               /\ VoteIsFresh(m)
  IN
    /\ m \in messages
    /\ m.type = RequestVote
    /\ currentTerm' = [currentTerm EXCEPT ![m.to] =
         IF higher THEN m.term ELSE @]
    /\ messages' = RetainedMessages(messages \ {m}, currentTerm')
    /\ votedFor' = [votedFor EXCEPT ![m.to] =
         IF grant THEN m.from ELSE IF higher THEN NoVote ELSE @]
    /\ role' = [role EXCEPT ![m.to] =
         IF higher \/ grant THEN Follower ELSE @]
    /\ RecordHigherTermOutcome(m.to, m.term, higher)
    /\ RecordAuthorityAcceptance(m.term, currentTerm[m.to], grant)
    /\ electedLeaders' = RetainedElections(electedLeaders, currentTerm')
    /\ RetireLogicalPrefixes(currentTerm')
    /\ UNCHANGED <<log, commitIndex, readRequests, readBarrierViolationSeen>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED <<committedLedger, commitWitnesses>>

BecomeLeader(n) ==
  LET electedConfiguration == EffectiveConfiguration(n)
  IN
    /\ role[n] = Candidate
    /\ n \in ActiveVoters(electedConfiguration.config)
    /\ MembershipQuorum(
         electedConfiguration.config,
         {v \in Nodes :
           /\ votedFor[v] = n
           /\ currentTerm[v] = currentTerm[n]})
    /\ role' = [role EXCEPT ![n] = Leader]
    /\ RecordElection(n)
    /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages,
                    readRequests, readBarrierViolationSeen,
                    higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED historyVars

ClientAppend(n, value) ==
  /\ role[n] = Leader
  /\ currentTerm[n] \in 1..MaxTerm
  /\ Len(log[n]) < MaxLogLen
  /\ log' = [log EXCEPT ![n] = Append(@, Entry(currentTerm[n], value))]
  /\ RecordLogicalPrefixes(log', snapshotIndex, snapshotPrefix, currentTerm)
  /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
  /\ UNCHANGED <<currentTerm, votedFor, role, commitIndex, messages,
                  readRequests, readBarrierViolationSeen, electedLeaders,
                  committedLedger, commitWitnesses,
                  higherTermStepDownFailed>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars

SendAppend(l, f) ==
  LET senderMembership == EffectiveConfiguration(l).config
      senderPendingSelfRemoval == PendingSelfRemoval(l)
      msg == [type |-> AppendEntries,
              term |-> currentTerm[l],
              from |-> l,
              to |-> f,
              entries |-> log[l],
              leaderCommit |-> commitIndex[l],
              senderMembership |-> senderMembership,
              senderPendingSelfRemoval |-> senderPendingSelfRemoval]
  IN
    /\ role[l] = Leader
    /\ l # f
    /\ currentTerm[l] \in 1..MaxTerm
    /\ msg \notin messages
    /\ messages' = RetainedMessages(messages \cup {msg}, currentTerm)
    /\ RecordAuthorityAcceptance(currentTerm[l], currentTerm[l], TRUE)
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    readRequests, readBarrierViolationSeen,
                    electedLeaders, higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars
    /\ UNCHANGED historyVars

AppendSenderAuthorized(m) ==
  \/ m.from \in ActiveVoters(m.senderMembership)
  \/ m.senderPendingSelfRemoval

AppendReceiverEligible(m) ==
  \/ m.to \in ActiveVoters(EffectiveConfiguration(m.to).config)
  \/ m.to \in ActiveVoters(m.senderMembership)

DeliverAppend(m) ==
  LET higher == m.term > currentTerm[m.to]
      accept == /\ m.term >= currentTerm[m.to]
                /\ AppendSenderAuthorized(m)
                /\ AppendReceiverEligible(m)
                /\ CanAdoptLog(m.to, m.entries, m.term)
      receiverWouldAccept ==
        /\ m.term >= currentTerm[m.to]
        /\ AppendReceiverEligible(m)
        /\ CanAdoptLog(m.to, m.entries, m.term)
      baseRole == [role EXCEPT ![m.to] =
        IF higher \/ accept THEN Follower ELSE @]
      nextLog == IF accept THEN [log EXCEPT ![m.to] = m.entries] ELSE log
      nextCommit ==
        IF accept
        THEN [commitIndex EXCEPT ![m.to] =
               AdvanceCommit(m.to, m.leaderCommit, Len(m.entries))]
        ELSE commitIndex
  IN
    /\ m \in messages
    /\ m.type = AppendEntries
    /\ currentTerm' = [currentTerm EXCEPT ![m.to] =
         IF higher THEN m.term ELSE @]
    /\ messages' = RetainedMessages(messages \ {m}, currentTerm')
    /\ votedFor' = [votedFor EXCEPT ![m.to] =
         IF higher THEN NoVote ELSE @]
    /\ role' = baseRole
    /\ log' = nextLog
    /\ commitIndex' = nextCommit
    /\ RecordLogicalPrefixes(
         nextLog, snapshotIndex, snapshotPrefix, currentTerm')
    /\ UNCHANGED committedLedger
    /\ RecordHigherTermOutcome(m.to, m.term, higher)
    /\ RecordAppendOutcome(
         m, currentTerm[m.to], accept, receiverWouldAccept)
    /\ electedLeaders' = RetainedElections(electedLeaders, currentTerm')
    /\ UNCHANGED <<readRequests, readBarrierViolationSeen, commitWitnesses>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars

RoleAfterCommit(node, selfRemoval) ==
  IF selfRemoval
  THEN [role EXCEPT ![node] = Follower]
  ELSE role

Commit(n, i) ==
  LET preRole == role[n]
      preTerm == currentTerm[n]
      preEffectiveMembership == EffectiveConfiguration(n).config
      preAuthorityMembership == CommitAuthorityMembership(n)
      preEffectiveConfigIndex == EffectiveConfiguration(n).configIndex
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
         log, snapshotIndex, snapshotPrefix, n, commitIndex[n], i, preTerm)
    /\ RecordCommitWitnesses(CommitCertificatesFor(
         n, commitIndex[n], i, context, preEffectiveConfigIndex))
    /\ RecordAuthorityAcceptance(preTerm, preTerm, TRUE)
    /\ UNCHANGED <<currentTerm, votedFor, log, messages,
                    readRequests, readBarrierViolationSeen,
                    electedLeaders, logicalPrefixLedger,
                    higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars

Apply(n) ==
  LET next == AppliedThrough(n) + 1
      entry == LogicalEntry(n, next)
      priorState == ApplicationState(n)
      resultState == ApplyEntry(priorState, entry)
  IN
    /\ AppliedThrough(n) < commitIndex[n]
    /\ RecordApplication(n, entry, resultState)
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex, messages,
                    readRequests, readBarrierViolationSeen, electedLeaders,
                    logicalPrefixLedger, committedLedger, commitWitnesses,
                    higherTermStepDownFailed, staleAuthorityAccepted,
                    frozenAppendAuthorityFailed>>
    /\ UNCHANGED snapshotVars

ApplicationStateLoss(n) ==
  /\ ApplicationEpoch(n) = 0
  /\ AppliedThrough(n) > 0
  /\ StartApplicationEpoch(n, 0, InitialApplicationState)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  messages, readRequests, readBarrierViolationSeen,
                  electedLeaders>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED historyVars
  /\ UNCHANGED authorityVars

Restart(n) ==
  /\ role[n] # Follower
  /\ role' = [role EXCEPT ![n] = Follower]
  /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages,
                  readRequests, readBarrierViolationSeen, electedLeaders>>
  /\ UNCHANGED snapshotVars
  /\ UNCHANGED applicationVars
  /\ UNCHANGED historyVars
  /\ UNCHANGED authorityVars

CreateSnapshot(n) ==
  LET index == AppliedThrough(n)
      prefix == LogicalPrefix(n, index)
  IN
    /\ index > snapshotIndex[n]
    /\ snapshotIndex' = [snapshotIndex EXCEPT ![n] = index]
    /\ snapshotPrefix' = [snapshotPrefix EXCEPT ![n] = prefix]
    /\ compactionPending' = [compactionPending EXCEPT ![n] = TRUE]
    /\ RecordLogicalPrefixes(
         log, snapshotIndex', snapshotPrefix', currentTerm)
    /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                    snapshotTransfer, messages,
                    readRequests, readBarrierViolationSeen,
                    electedLeaders, committedLedger, commitWitnesses>>
    /\ UNCHANGED applicationVars
    /\ UNCHANGED authorityVars

TransferSnapshot(from, to) ==
  /\ role[from] = Leader
  /\ from # to
  /\ snapshotIndex[from] > 0
  /\ snapshotIndex[to] < snapshotIndex[from]
  /\ AppliedThrough(to) <= snapshotIndex[from]
  /\ ApplicationEpoch(to) = 0
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
                  messages, readRequests, readBarrierViolationSeen,
                  electedLeaders,
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
      restoredState == StateAfterEntries(transfer.prefix)
  IN
    /\ transfer.active
    /\ transfer.term >= currentTerm[node]
    /\ transfer.index > snapshotIndex[node]
    /\ transfer.index >= AppliedThrough(node)
    /\ ApplicationEpoch(node) = 0
    /\ currentTerm' = [currentTerm EXCEPT ![node] = transfer.term]
    /\ votedFor' = [votedFor EXCEPT ![node] =
         IF transfer.term > currentTerm[node] THEN NoVote ELSE @]
    /\ role' = [role EXCEPT ![node] = Follower]
    /\ messages' = RetainedMessages(messages, currentTerm')
    /\ log' = nextLog
    /\ commitIndex' = nextCommit
    /\ snapshotIndex' = [snapshotIndex EXCEPT ![node] = transfer.index]
    /\ snapshotPrefix' = [snapshotPrefix EXCEPT ![node] = transfer.prefix]
    /\ compactionPending' = [compactionPending EXCEPT ![node] = FALSE]
    /\ snapshotTransfer' = NoSnapshotTransfer
    /\ StartApplicationEpoch(node, transfer.index, restoredState)
    /\ RecordLogicalPrefixes(
         nextLog, snapshotIndex', snapshotPrefix', currentTerm')
    /\ UNCHANGED committedLedger
    /\ RecordHigherTermOutcome(
         node, transfer.term, transfer.term > currentTerm[node])
    /\ RecordAuthorityAcceptance(transfer.term, currentTerm[node], TRUE)
    /\ electedLeaders' = RetainedElections(electedLeaders, currentTerm')
    /\ UNCHANGED <<readRequests, readBarrierViolationSeen,
                    commitWitnesses>>

\* `log` is ghost logical history spanning the snapshot prefix. Compaction
\* completes the modeled snapshot lifecycle but intentionally retains that
\* history for safety witnesses. Physical prefix retention, storage offsets,
\* and crash/reopen compaction are simulator and storage-test evidence.
CompactSnapshot(n) ==
  /\ compactionPending[n]
  /\ compactionPending' = [compactionPending EXCEPT ![n] = FALSE]
  /\ UNCHANGED logicalPrefixLedger
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  snapshotIndex, snapshotPrefix, snapshotTransfer,
                  messages, readRequests, readBarrierViolationSeen,
                  electedLeaders,
                  committedLedger, commitWitnesses>>
  /\ UNCHANGED applicationVars
  /\ UNCHANGED authorityVars

EnterJoint(n, newVoters) ==
  LET effective == EffectiveConfiguration(n)
      appliedConfiguration == AppliedConfiguration(n)
      next == JointMembership(effective.config.old, newVoters)
      nextLog == [log EXCEPT ![n] =
        Append(@, ConfigurationEntry(currentTerm[n], next))]
  IN
    /\ effective.config.phase = StableConfig
    /\ appliedConfiguration.config.phase = StableConfig
    /\ appliedConfiguration = effective
    /\ OneVoterChange(effective.config.old, newVoters)
    /\ role[n] = Leader
    /\ CommittedEntriesHeldBy(newVoters)
    /\ currentTerm[n] \in 1..MaxTerm
    /\ Len(log[n]) = AppliedThrough(n)
    /\ Len(log[n]) < MaxLogLen
    /\ log' = nextLog
    /\ RecordLogicalPrefixes(
         nextLog, snapshotIndex, snapshotPrefix, currentTerm)
    /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
    /\ UNCHANGED <<currentTerm, votedFor, role, commitIndex, messages,
                    readRequests, readBarrierViolationSeen, electedLeaders,
                    committedLedger, commitWitnesses,
                    higherTermStepDownFailed>>
    /\ UNCHANGED snapshotVars
    /\ UNCHANGED applicationVars

LeaveJoint(n) ==
  LET effective == EffectiveConfiguration(n)
      appliedConfiguration == AppliedConfiguration(n)
      next == StableMembership(effective.config.new)
      nextLog == [log EXCEPT ![n] =
        Append(@, ConfigurationEntry(currentTerm[n], next))]
  IN
    /\ effective.config.phase = JointConfig
    /\ appliedConfiguration.config.phase = JointConfig
    /\ role[n] = Leader
    /\ CommittedEntriesHeldBy(effective.config.new)
    /\ currentTerm[n] \in 1..MaxTerm
    /\ Len(log[n]) = AppliedThrough(n)
    /\ Len(log[n]) < MaxLogLen
    /\ log' = nextLog
    /\ RecordLogicalPrefixes(
         nextLog, snapshotIndex, snapshotPrefix, currentTerm)
    /\ RecordAuthorityAcceptance(currentTerm[n], currentTerm[n], TRUE)
    /\ UNCHANGED <<currentTerm, votedFor, role, commitIndex, messages,
                    readRequests, readBarrierViolationSeen, electedLeaders,
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
                    messages, readBarrierViolationSeen,
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
                      messages, readRequests, electedLeaders,
                      higherTermStepDownFailed>>
      /\ UNCHANGED snapshotVars
      /\ UNCHANGED applicationVars
      /\ UNCHANGED historyVars

ProtocolNext ==
  \/ \E n \in Nodes : Timeout(n)
  \/ \E c, v \in Nodes : SendRequestVote(c, v)
  \/ \E m \in {message \in messages : message.type = RequestVote} :
       DeliverRequestVote(m)
  \/ \E n \in Nodes : BecomeLeader(n)
  \/ \E n \in Nodes, value \in Values : ClientAppend(n, value)
  \/ \E l, f \in Nodes : SendAppend(l, f)
  \/ \E m \in {message \in messages : message.type = AppendEntries} :
       DeliverAppend(m)
  \/ \E n \in Nodes, i \in 1..MaxLogLen : Commit(n, i)
  \/ \E n \in Nodes : Apply(n)
  \/ \E n \in Nodes : ApplicationStateLoss(n)
  \/ \E n \in Nodes : Restart(n)
  \/ \E n \in Nodes : CreateSnapshot(n)
  \/ \E from, to \in Nodes : TransferSnapshot(from, to)
  \/ InstallSnapshot
  \/ \E n \in Nodes, voters \in VoterSets : EnterJoint(n, voters)
  \/ \E n \in Nodes : LeaveJoint(n)
  \/ \E n \in Nodes, request \in ReadRequests : RegisterRead(n, request)
  \/ \E n \in Nodes, request \in ReadRequests : GrantRead(n, request)

\* Compaction changes only verifier bookkeeping, so complete it before
\* exploring independent protocol transitions and their equivalent schedules.
Next ==
  IF \E n \in Nodes : compactionPending[n]
  THEN \E n \in Nodes : CompactSnapshot(n)
  ELSE ProtocolNext

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
       /\ currentTerm[leader] > committed.committedInTerm)
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
  /\ \A n \in Nodes :
       /\ AppliedCursorOK(applied[n])
       /\ CurrentApplicationStateWitnessed(n)
       /\ AppliedConfiguration(n).config = ApplicationState(n).membership
  /\ \A base \in applicationBases : ApplicationBaseOK(base)
  /\ \A transition \in applicationTransitions :
       /\ ApplicationTransitionOK(transition)
       /\ ApplicationTransitionSound(transition)
       /\ ApplicationTransitionLinked(transition)
  /\ \A a, b \in Nodes :
       \A left \in ApplicationObservationWitnesses(a) :
         \A right \in ApplicationObservationWitnesses(b) :
           left.index = right.index
           => /\ left.entry = right.entry
              /\ left.resultState = right.resultState
  /\ \A a, b \in Nodes :
       \A left \in ApplicationStateWitnesses(a) :
         \A right \in ApplicationStateWitnesses(b) :
           left.index = right.index => left.state = right.state

StaleLeaderFencing ==
  /\ ~higherTermStepDownFailed
  /\ ~staleAuthorityAccepted

CommittedEntriesHaveQuorum ==
  /\ ~commitWitnesses.invalidCertificateSeen
  /\ \A committed \in committedLedger :
       [index |-> committed.index, entry |-> committed.entry]
         \in commitWitnesses.witnessedCommits
  /\ \A n \in Nodes :
       \A index \in 1..commitIndex[n] :
         \E committed \in committedLedger :
           /\ committed.index = index
           /\ committed.entry = LogicalEntry(n, index)

ReadBarrierLinearizability ==
  ~readBarrierViolationSeen

====
