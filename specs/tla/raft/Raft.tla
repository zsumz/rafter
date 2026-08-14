---- MODULE Raft ----
EXTENDS Naturals, Sequences, FiniteSets, TLC

\* This is a small, bounded design model. It is not implementation code and it
\* intentionally does not import or depend on rafter.
\* The production specification is safety-only. Fair-schedule liveness evidence
\* is owned by the bounded simulator, whose scheduler states its timing bounds.
\*
\* Correspondence notes. These record where the model deliberately differs from
\* the implementation, so a reader does not mistake an abstraction for coverage.
\*
\* No no-op entry kind. EntrySet is Command \cup Configuration; the leader's
\* on-election no-op that rafter appends (LogEntryKind::Noop) is unmodelled.
\* This is a deliberate abstraction, not a missing safety case. The rule the
\* no-op exists to make reachable is Raft's current-term commit restriction,
\* and Commit enforces that rule directly: it requires
\* LogicalEntry(n, i).term = currentTerm[n], so no leader can ever count
\* replicas of a prior-term entry. The no-op is the implementation's device for
\* obtaining a current-term entry to commit, which is a progress concern, and
\* progress is owned by the simulator. The one refinement consequence worth
\* stating in the direction the code uses it: implementation log indexes carry
\* one extra entry per leader term that model log indexes do not, so index
\* equality between a rafter log and a model log is never the refinement
\* mapping.
\*
\* No stored snapshot, and no physical compaction. rafter's snapshot is a real
\* stored object: the implementation writes it, truncates the log behind it,
\* and serves it to a lagging follower from storage. The model keeps only
\* `snapshotIndex[n]`, retains `log[n]` in full as ghost logical history, and
\* derives the snapshot prefix as `Prefix(log[n], snapshotIndex[n])` wherever a
\* live view of it is needed. So a model state carries no evidence about
\* retained prefixes, storage offsets, or crash/reopen compaction; those are
\* simulator and storage-test evidence, and this spec should not be read as
\* covering them. The one place the abstraction does not reach is a snapshot in
\* flight, where the sender's log may move before the receiver installs: that
\* payload is a frozen copy captured at send time and stays materialized in
\* `snapshotTransfer.prefix`, which is a stored field rather than a view.
\*
\* Creation and compaction are also one atomic action here, where the
\* implementation has two distinct moments. The model has nothing to observe
\* between them -- see the note on `CreateSnapshot` -- so the separation would
\* be state without content. Ordering between snapshotting and concurrent
\* protocol work is likewise simulator evidence.
\*
\* Mutation-sensitive monitor. frozenAppendAuthorityFailed stays FALSE under
\* this spec as written, and that is its specified resting state rather than
\* evidence that it is dead. Its latch needs message.senderPendingSelfRemoval
\* together with receiverWouldAccept /\ ~accepted; accept differs from
\* receiverWouldAccept only by AppendSenderAuthorized(m), which holds whenever
\* m.senderPendingSelfRemoval holds. So the latch is unsatisfiable for exactly
\* as long as DeliverAppend judges sender authority by the membership frozen
\* into the message at send time, and failing when that stops being true is the
\* whole job. RafterInvariantDetectorNegative does assert it, through
\* FrozenAppendAuthorityInvariant, and the rafter-invariants mutation suite
\* swaps the guard for a live-membership test and requires TLC to exit 12
\* naming that invariant. It is the only predicate in that fixture that catches
\* the swap: drop the ~frozenAppendAuthorityFailed conjunct and TypeOK,
\* ElectionSafety, LogMatching, LeaderCompleteness and CommittedPrefixStability
\* all pass on the mutated spec. A tier config is the one place it does not
\* belong, since there it could only ever hold.

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
          snapshotIndex, snapshotTransfer,
          applied, applicationBases, applicationTransitions,
          messages, readRequests, readBarrierViolationSeen,
          electedLeaders, logicalPrefixLedger, committedLedger,
          commitWitnesses,
          higherTermStepDownFailed,
          staleAuthorityAccepted,
          frozenAppendAuthorityFailed

vars == << currentTerm, votedFor, role, log, commitIndex,
          snapshotIndex, snapshotTransfer,
          applied, applicationBases, applicationTransitions,
          messages, readRequests, readBarrierViolationSeen,
          electedLeaders, logicalPrefixLedger, committedLedger,
          commitWitnesses,
          higherTermStepDownFailed,
          staleAuthorityAccepted,
          frozenAppendAuthorityFailed >>

snapshotVars == <<snapshotIndex, snapshotTransfer>>

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

\* The snapshot prefix, derived rather than stored.
\*
\* This used to be a `snapshotPrefix` variable carrying a copy of the log up to
\* the snapshot floor, maintained in parallel with `log` and `snapshotIndex` and
\* asserted equal to `Prefix(log[n], snapshotIndex[n])` by
\* `SnapshotIdentitySoundFor`.
\*
\* What deriving it does not do is shrink the state space. The variable was a
\* function of two others, so no two reachable states ever differed in it
\* alone, and the distinct-state counts are identical before and after on every
\* model measured -- which is the point: a state component that cannot
\* distinguish two states is not carrying verification. What it does do is
\* retire an obligation. Three of the four conjuncts `SnapshotIdentitySoundFor`
\* used to check said the stored copy really was the log's prefix at the floor;
\* they are now definitional, and the one that remains is the bound that makes
\* the definition well formed. It also removes the way that obligation could be
\* broken, which is a write of the wrong prefix.
\*
\* The equality it was asserted to satisfy is exactly this definition, so
\* deriving it is sound wherever that equality held -- which is every reachable
\* state, since `LogMatching` checked it. The one place that needs more than
\* "the invariant held" is a recorder called with a primed log and an unprimed
\* snapshot floor, `RecordLogicalPrefixes(log', snapshotIndex, ...)`, where the
\* derived prefix reads the successor log while the stored one held the
\* predecessor's. Those agree because no action rewrites a log at or below its
\* own snapshot floor: `ClientAppend`, `EnterJoint` and `LeaveJoint` only
\* append; `DeliverAppend` replaces the log wholesale but only under
\* `CanAdoptLog`, which pins every index up to `commitIndex[n]`, and
\* `snapshotIndex[n] <= commitIndex[n]` holds because `CreateSnapshot` snapshots
\* at `AppliedThrough(n) <= commitIndex[n]` and `InstallSnapshot` raises
\* `commitIndex` to the transfer index in the same step. TypeOK states that
\* bound so the argument is machine-checked rather than asserted here.
SnapshotPrefixFrom(logs, snapshotIndexes, node) ==
  Prefix(logs[node], snapshotIndexes[node])

SnapshotPrefix(node) == SnapshotPrefixFrom(log, snapshotIndex, node)

LatestConfigurationIn(entries) ==
  LET candidates ==
        {index \in 1..Len(entries) :
          entries[index].kind = ConfigurationEntryKind}
  IN
    IF candidates = {}
    THEN [configIndex |-> 0, config |-> StableMembership(Nodes)]
    ELSE LET latest == LatestIndex(candidates)
         IN [configIndex |-> latest, config |-> entries[latest].input]

LogicalPrefixFrom(logs, snapshotIndexes, node, index) ==
  IF index = 0
  THEN <<>>
  ELSE IF index <= snapshotIndexes[node]
       THEN Prefix(SnapshotPrefixFrom(logs, snapshotIndexes, node), index)
       ELSE SnapshotPrefixFrom(logs, snapshotIndexes, node)
              \o SubSeq(logs[node], snapshotIndexes[node] + 1, index)

LogicalEntryFrom(logs, snapshotIndexes, node, index) ==
  LogicalPrefixFrom(logs, snapshotIndexes, node, index)[index]

LogicalPrefix(node, index) ==
  LogicalPrefixFrom(log, snapshotIndex, node, index)

LogicalEntry(node, index) ==
  LogicalEntryFrom(log, snapshotIndex, node, index)

AppliedConfiguration(node) ==
  LatestConfigurationIn(LogicalPrefix(node, AppliedThrough(node)))

EffectiveConfiguration(node) ==
  LatestConfigurationIn(LogicalPrefix(node, Len(log[node])))

\* What is left of the snapshot identity once the prefix is derived. The other
\* three conjuncts -- that the prefix has the snapshot floor's length, and that
\* it equals the log's prefix at that floor -- were the definition of
\* `SnapshotPrefixFrom` written as an assertion, and are now true by
\* construction rather than by check. The bound below is the residue that still
\* carries content: it is what makes the derived prefix well defined, and it is
\* checked on every reachable state.
SnapshotIdentitySoundFor(logs, snapshotIndexes) ==
  \A n \in Nodes : snapshotIndexes[n] <= Len(logs[n])

LogicalPrefixWitnesses(logs, snapshotIndexes) ==
  UNION {
    {[index |-> index,
      term |-> LogicalEntryFrom(logs, snapshotIndexes, node, index).term,
      prefix |-> LogicalPrefixFrom(logs, snapshotIndexes, node, index)] :
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

RecordLogicalPrefixes(logs, snapshotIndexes, terms) ==
  LET observed ==
        logicalPrefixLedger \cup
          LogicalPrefixWitnesses(logs, snapshotIndexes)
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
    logs, snapshotIndexes, node, oldFloor, newFloor,
    committedInTerm) ==
  {[index |-> index,
    entry |-> LogicalEntryFrom(logs, snapshotIndexes, node, index),
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
    logs, snapshotIndexes, node, oldFloor, newFloor,
    committedInTerm) ==
  LET candidates == CommittedEntriesFor(
        logs, snapshotIndexes, node, oldFloor, newFloor,
        committedInTerm)
  IN committedLedger' = RetainedCommittedEntries(committedLedger, candidates)

ConfigurationMembershipAt(
    logs, snapshotIndexes, node, configIndex) ==
  IF configIndex = 0
  THEN StableMembership(Nodes)
  ELSE LET entry == LogicalEntryFrom(
             logs, snapshotIndexes, node, configIndex)
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

MatchingReplicasFrom(logs, snapshotIndexes, node, index) ==
  {replica \in Nodes :
    /\ index \in 1..Len(logs[replica])
    /\ index \in 1..Len(logs[node])
    /\ LogicalEntryFrom(logs, snapshotIndexes, replica, index)
         = LogicalEntryFrom(logs, snapshotIndexes, node, index)}

MatchingReplicas(node, index) ==
  MatchingReplicasFrom(log, snapshotIndex, node, index)

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
      log, snapshotIndex, node, configIndex),
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
  /\ \A n \in Nodes :
       /\ snapshotIndex[n] \in 0..MaxLogLen
       /\ snapshotIndex[n] <= Len(log[n])
       \* Load-bearing for the derived snapshot prefix: it is what makes
       \* `Prefix(log[n], snapshotIndex[n])` well defined, and what lets
       \* `CanAdoptLog`'s committed-prefix guard protect the snapshot prefix.
       /\ snapshotIndex[n] <= commitIndex[n]
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
  /\ RecordLogicalPrefixes(log', snapshotIndex, currentTerm)
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
    /\ RecordLogicalPrefixes(nextLog, snapshotIndex, currentTerm')
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
         log, snapshotIndex, n, commitIndex[n], i, preTerm)
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

\* Snapshot creation and compaction, as one atomic step.
\*
\* `log` is ghost logical history spanning the snapshot prefix. Compaction
\* completes the modeled snapshot lifecycle but intentionally retains that
\* history for safety witnesses. Physical prefix retention, storage offsets,
\* and crash/reopen compaction are simulator and storage-test evidence.
\*
\* Why one action and not two. This used to be `CreateSnapshot` setting a
\* `compactionPending[n]` flag, a compaction-first branch in `Next` that
\* disabled every protocol action while any flag was set, and a
\* `CompactSnapshot(n)` that cleared it and changed nothing else. The
\* intermediate state was unobservable: no protocol action was enabled in it,
\* the only enabled action agreed with its own successor on every variable but
\* the flag, and no predicate outside the flag's own type constraint read the
\* flag. So each creation cost one extra distinct state that no property could
\* distinguish from the state that followed it. Folding the pair removes that
\* state and the variable with it, and the resulting spec is
\* stuttering-equivalent to the old one for every property over the remaining
\* variables: drop the flag from each old behavior and the intermediate state
\* becomes a stuttering step of the new one, which `[][Next]_vars` already
\* admits. Nothing else had to change, because `CompactSnapshot` wrote nothing
\* else.
CreateSnapshot(n) ==
  LET index == AppliedThrough(n)
  IN
    /\ index > snapshotIndex[n]
    /\ snapshotIndex' = [snapshotIndex EXCEPT ![n] = index]
    /\ RecordLogicalPrefixes(log, snapshotIndex', currentTerm)
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
        \* Frozen at send time: the sender's log may move afterwards, so this
        \* is a copy and not a view. It stays a stored field of the transfer.
        prefix |-> SnapshotPrefix(from)]
  /\ RecordAuthorityAcceptance(currentTerm[from], currentTerm[from], TRUE)
  /\ UNCHANGED <<currentTerm, votedFor, role, log, commitIndex,
                  snapshotIndex,
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
    /\ snapshotTransfer' = NoSnapshotTransfer
    /\ StartApplicationEpoch(node, transfer.index, restoredState)
    /\ RecordLogicalPrefixes(nextLog, snapshotIndex', currentTerm')
    /\ UNCHANGED committedLedger
    /\ RecordHigherTermOutcome(
         node, transfer.term, transfer.term > currentTerm[node])
    /\ RecordAuthorityAcceptance(transfer.term, currentTerm[node], TRUE)
    /\ electedLeaders' = RetainedElections(electedLeaders, currentTerm')
    /\ UNCHANGED <<readRequests, readBarrierViolationSeen,
                    commitWitnesses>>

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
    /\ RecordLogicalPrefixes(nextLog, snapshotIndex, currentTerm)
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
    /\ RecordLogicalPrefixes(nextLog, snapshotIndex, currentTerm)
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

\* Focused proof obligations, and the whole transition relation built out of
\* them.
\*
\* `Next` is the whole protocol and `Spec` is the whole model: every wired tier
\* checks all nine invariants against all of it. The focused relations below are
\* strict disjunct-subsets of `Next`, assembled from the same action operators
\* under the same guards. None of them adds a transition, weakens a guard, or
\* writes a variable the corresponding full-model action does not write, so
\* every behavior of a focused spec is a behavior of `Spec`. That direction is
\* the cheap one and carries no evidence by itself. The direction that does:
\* a focused relation whose queue drains proves its invariants over a state
\* space `Spec` has not finished exploring, and the obligation it discharges is
\* named by what it leaves out.
\*
\* The subset property is load-bearing -- the entire obligation architecture
\* rests on it -- so it is established by construction rather than by eye. The
\* five relations used to be five hand-kept lists that restated the same nine
\* core disjuncts verbatim, and a disjunct added to one list and forgotten in
\* another would have broken the claim with nothing able to detect it. Now the
\* containments are facts about the text: `AgreementNext` is named once,
\* `CoreNext` extends it, the three family relations extend those, and `Next`
\* extends the largest of them. `CoreNext` cannot acquire a disjunct `Next`
\* lacks, because `Next` is built from `CoreNext`. `Next`'s own comment carries
\* the chain for the two families it does not name directly.
\*
\* Composition is inert for TLC. Definitions expand at evaluation, so a model
\* checked against `SnapshotNext` explores exactly the disjunction it did when
\* the disjuncts were written out, and every pinned state count is unchanged.
\*
\* What a focused relation therefore does not prove: anything that needs an
\* action it drops, and anything about the interaction between the families it
\* drops and the families it keeps. Splitting the proof splits the coverage with
\* it. These are obligations to be stated and discharged separately, not a
\* cheaper route to the same claim.
\*
\* Totality on `vars` is inherited, not re-established. Every disjunct of
\* `Next` already constrains all twenty variables -- each action
\* names the ones it primes and carries UNCHANGED for the rest -- so any subset
\* of those disjuncts constrains them too. TLC rejects an unconstrained
\* variable, so a mis-composed relation fails loudly at the first state rather
\* than quietly exploring a larger space.

\* The agreement core: election, replication, commit, apply. Every focused
\* relation contains it, so it is named once and extended rather than copied.
\* `ClientAppend` is deliberately not here -- it is the one core action a
\* membership obligation must drop, for the log-length reason given below --
\* which is why the shared core is eight disjuncts and `CoreNext` is nine.
AgreementNext ==
  \/ \E n \in Nodes : Timeout(n)
  \/ \E c, v \in Nodes : SendRequestVote(c, v)
  \/ \E m \in {message \in messages : message.type = RequestVote} :
       DeliverRequestVote(m)
  \/ \E n \in Nodes : BecomeLeader(n)
  \/ \E l, f \in Nodes : SendAppend(l, f)
  \/ \E m \in {message \in messages : message.type = AppendEntries} :
       DeliverAppend(m)
  \/ \E n \in Nodes, i \in 1..MaxLogLen : Commit(n, i)
  \/ \E n \in Nodes : Apply(n)

\* Core agreement: the agreement core plus ordinary command appends. The actions
\* an election-safety, log-matching, leader-completeness, committed-prefix, or
\* state-machine-safety obligation needs, and nothing else. Deliberately
\* excludes reads, the snapshot lifecycle, membership change, `Restart`, and
\* `ApplicationStateLoss`; each of those is the subject of one of the three
\* relations below, and an obligation that does not name them should not pay
\* for their branching.
CoreNext ==
  \/ AgreementNext
  \/ \E n \in Nodes, value \in Values : ClientAppend(n, value)

\* Membership change: core agreement without ordinary command appends, plus the
\* two configuration-change actions. `ClientAppend` is dropped rather than kept
\* because `MaxLogLen` is the binding constraint on any joint-quorum obligation
\* and a command entry spends a slot that `EnterJoint` and `LeaveJoint` need:
\* completing a change costs two entries, so at `MaxLogLen = 2` a command and a
\* completed change cannot coexist at all. That is why this relation extends
\* `AgreementNext` and not `CoreNext`: it is the only focused relation that
\* subtracts a core action, and the composition says so. Commit and apply stay,
\* because `EnterJoint` guards on `Len(log[n]) = AppliedThrough(n)` and
\* `LeaveJoint` likewise, so a configuration entry must be committed and applied
\* before the next one can be proposed. Excludes reads and the snapshot
\* lifecycle.
MembershipNext ==
  \/ AgreementNext
  \/ \E n \in Nodes, voters \in VoterSets : EnterJoint(n, voters)
  \/ \E n \in Nodes : LeaveJoint(n)

\* Snapshot lifecycle: core agreement plus application-state loss, restart, and
\* create/transfer/install. Restart and `ApplicationStateLoss` belong here and
\* not in `CoreNext` because the properties they threaten are the ones snapshot
\* installation restores: an epoch bump plus a rebuilt application state is the
\* only path by which `applied` moves backwards. Excludes reads and membership
\* change.
\*
\* It used to need a compaction-first wrapper, because `CompactSnapshot` was
\* reachable only through `Next`'s branch and never through the bare
\* disjunction; folding creation and compaction into one action removed the
\* branch and the wrapper with it, which is what lets this be a plain extension
\* of `CoreNext`.
SnapshotNext ==
  \/ CoreNext
  \/ \E n \in Nodes : ApplicationStateLoss(n)
  \/ \E n \in Nodes : Restart(n)
  \/ \E n \in Nodes : CreateSnapshot(n)
  \/ \E from, to \in Nodes : TransferSnapshot(from, to)
  \/ InstallSnapshot

\* Read barriers: core agreement plus read registration and grant. The barrier
\* is stated against `commitIndex`, so commit and apply have to stay; the read
\* actions themselves touch only `readRequests` and `readBarrierViolationSeen`.
\* Excludes the snapshot lifecycle and membership change.
ReadNext ==
  \/ CoreNext
  \/ \E n \in Nodes, request \in ReadRequests : RegisterRead(n, request)
  \/ \E n \in Nodes, request \in ReadRequests : GrantRead(n, request)

\* Every protocol action, and the whole transition relation. There is no
\* wrapper: snapshot creation and compaction are one atomic action, so `Next`
\* has no compaction-first branch to reproduce and no second name to carry.
\*
\* `SnapshotNext` is the largest focused relation, so it carries the core here
\* and the remaining four actions are named alongside it: exactly the eighteen
\* disjuncts, each written once.
\*
\* Each disjunct appears once on purpose. `SnapshotNext \/ ReadNext` also
\* denotes the right relation -- disjunction is idempotent, and the two share
\* every `CoreNext` disjunct -- but TLC enumerates successors per disjunct
\* rather than per distinct action, so the overlap would generate every core
\* successor twice. Distinct-state counts are unaffected; generated-state counts
\* are not, and those are pinned as calibration in the obligation configs. It
\* was measured: written as `SnapshotNext \/ ReadNext`,
\* RaftIntegrationUnsymmetrized.cfg reports 403,405 generated for the same
\* 49,985 distinct, against 254,211 in this form.
\*
\* The containment chain is still by construction, just routed through
\* `SnapshotNext`: `AgreementNext \subseteq CoreNext \subseteq SnapshotNext
\* \subseteq Next`, `ReadNext \subseteq Next` because `ReadNext` is `CoreNext`
\* plus the two read actions named below, and `MembershipNext \subseteq Next`
\* because it is `AgreementNext` plus the two membership actions named below.
Next ==
  \/ SnapshotNext
  \/ \E n \in Nodes, request \in ReadRequests : RegisterRead(n, request)
  \/ \E n \in Nodes, request \in ReadRequests : GrantRead(n, request)
  \/ \E n \in Nodes, voters \in VoterSets : EnterJoint(n, voters)
  \/ \E n \in Nodes : LeaveJoint(n)

Spec == Init /\ [][Next]_vars

CoreSpec == Init /\ [][CoreNext]_vars
MembershipSpec == Init /\ [][MembershipNext]_vars
SnapshotSpec == Init /\ [][SnapshotNext]_vars
ReadSpec == Init /\ [][ReadNext]_vars

\* Focused initial state for the joint-quorum obligation.
\*
\* `Init` starts every node at term zero, so a four-voter model spends its first
\* several levels electing somebody before any membership change is proposed,
\* and it does that once per candidate. The joint-quorum theorem does not begin
\* there. It begins at a legally elected leader over a stable four-voter
\* configuration and asks what the two-half quorum conjunction does from that
\* point on, so that is where its initial state should be.
\*
\* The state below is not a guess at a plausible-looking configuration. It is
\* the exact post-state of one specific `CoreNext` prefix from `Init`:
\*
\*   Timeout(L);
\*   SendRequestVote(L, v)      for each v \in Nodes \ {L};
\*   DeliverRequestVote(that m) for each of those messages;
\*   BecomeLeader(L)
\*
\* Each variable below is derived from the action that last writes it along
\* that prefix, because a monitor left at its `Init` value when the real path
\* would have advanced it makes the invariant that reads it vacuous, and a
\* monitor advanced past what the path justifies makes it fire on a state the
\* protocol never produces. Both failures are silent. The derivations:
\*
\*   currentTerm  `Timeout(L)` raises L to 1; each `DeliverRequestVote` sees
\*                m.term = 1 > 0, so `higher` holds and raises its receiver to
\*                1. Every node ends at 1.
\*   votedFor     `Timeout(L)` self-votes; each delivery grants, because the
\*                voter is unvoted at a higher term, both ends are active
\*                voters of `StableMembership(Nodes)`, and empty logs make
\*                `UpToDate` hold. Every node ends at L.
\*   role         `Timeout` makes L a Candidate and `BecomeLeader` makes it a
\*                Leader; each granting voter is set to Follower.
\*   electedLeaders  `RecordElection(L)` adds L at term 1. `RetainedElections`
\*                keeps it: retirement needs every node above term 1, which no
\*                node reaches at `MaxTerm = 1`.
\*   higherTermStepDownFailed  `RecordHigherTermOutcome` latches only when a
\*                node observing a higher term fails to land at that term as a
\*                Follower. Every delivery lands exactly there, so FALSE.
\*   staleAuthorityAccepted  `RecordAuthorityAcceptance` latches only on an
\*                accepted authority term strictly below the receiver's known
\*                term. Term 1 against known term 0, then 1 against 1. FALSE.
\*   frozenAppendAuthorityFailed  written only by `RecordAppendOutcome`, which
\*                only `DeliverAppend` calls. Untouched by this prefix. FALSE.
\*   logicalPrefixLedger  `RecordLogicalPrefixes` runs only on actions that
\*                change a log. No log changes, and `RetireLogicalPrefixes`
\*                cannot add. Empty.
\*   committedLedger, commitWitnesses  written only by `Commit`. Empty.
\*   applied, applicationBases, applicationTransitions  UNCHANGED by every
\*                action in the prefix, so they hold their `Init` values.
\*   messages     every vote request sent is delivered, and delivery removes
\*                it. Empty.
\*   log, commitIndex, snapshot vars, readRequests, readBarrierViolationSeen
\*                UNCHANGED throughout; `Init` values.
\*
\* So `JointQuorumInit` is a reachable state of `Spec`, and any invariant
\* violation reported at depth 0 against it is a construction error here, not a
\* protocol result.
\*
\* The distinguished leader breaks permutation symmetry over `Nodes`. A config
\* using this initial predicate must not declare `SYMMETRY ModelPermutations`.
JointQuorumLeader == CHOOSE n \in Nodes : TRUE

JointQuorumInit ==
  /\ currentTerm = [n \in Nodes |-> 1]
  /\ votedFor = [n \in Nodes |-> JointQuorumLeader]
  /\ role = [n \in Nodes |->
       IF n = JointQuorumLeader THEN Leader ELSE Follower]
  /\ log = [n \in Nodes |-> <<>>]
  /\ commitIndex = [n \in Nodes |-> 0]
  /\ snapshotIndex = [n \in Nodes |-> 0]
  /\ snapshotTransfer = NoSnapshotTransfer
  /\ applied = [n \in Nodes |-> AppliedCursor(0, 0, InitialApplicationState)]
  /\ applicationBases =
       {ApplicationBase(n, 0, InitialApplicationState) : n \in Nodes}
  /\ applicationTransitions = {}
  /\ messages = {}
  /\ readRequests = {}
  /\ readBarrierViolationSeen = FALSE
  /\ electedLeaders =
       [t \in 1..MaxTerm |-> IF t = 1 THEN {JointQuorumLeader} ELSE {}]
  /\ logicalPrefixLedger = {}
  /\ committedLedger = {}
  /\ commitWitnesses = EmptyCommitWitnessHistory
  /\ higherTermStepDownFailed = FALSE
  /\ staleAuthorityAccepted = FALSE
  /\ frozenAppendAuthorityFailed = FALSE

\* `MembershipNext` is the precise fit and no separate `JointQuorumNext` is
\* warranted. The obligation is about what the two-half quorum conjunction
\* admits, and the actions that reach and leave a joint configuration are
\* exactly `EnterJoint` and `LeaveJoint`, with `SendAppend`/`DeliverAppend`
\* replicating the entries and `Commit`/`Apply` making each change effective.
\* Its election disjuncts are inert here rather than wrong: at `MaxTerm = 1`
\* every node already sits at term 1, `Timeout` guards on
\* `currentTerm[n] < MaxTerm`, and no other action creates a Candidate, so the
\* election family is disabled in every reachable state. Leaving it in keeps
\* one relation shared with `RaftJointQuorumFocusedNext.cfg`, which starts from
\* `Init` and does need it.
JointQuorumFocusedSpec == JointQuorumInit /\ [][MembershipNext]_vars

\* Sound symmetry for `JointQuorumFocusedSpec`, and the reason a config using it
\* is not simply cheaper than one using `ModelPermutations`.
\*
\* `ModelPermutations` permutes every node, which is unsound against an initial
\* predicate that names one. This set permutes only the nodes the predicate does
\* not name, so it is a subgroup of the permutations that fix
\* `JointQuorumLeader`. That subgroup is a symmetry group of the whole model:
\* `JointQuorumInit` maps to itself under any permutation fixing the leader
\* (`currentTerm` is constant, `votedFor` is constantly the leader, `role`
\* separates the leader from a set of Followers permuted among themselves,
\* `electedLeaders[1]` is the leader's singleton, and every other component is a
\* constant function over `Nodes` or empty); `MembershipNext` quantifies over
\* `Nodes` and `VoterSets` without distinguishing any node; and all nine
\* invariants are likewise node-symmetric.
\*
\* The reason this is worth having: naming a leader removes the |Nodes|
\* symmetric copies of the election, but `ModelPermutations` was already
\* quotienting those away, so a focused-Init config that simply drops symmetry
\* trades a quotient it had for a prefix it did not need. This set recovers the
\* part of the quotient that remains sound, which for four voters is 3! rather
\* than 4!. How much that is worth in practice has not been measured; the
\* trajectories that motivated defining it are in docs/model-checking.md.
JointQuorumPermutations ==
  {[modelValue \in Nodes \cup Values \cup ReadRequests |->
      CASE modelValue = JointQuorumLeader -> JointQuorumLeader
        [] modelValue \in Nodes -> nodePermutation[modelValue]
        [] modelValue \in Values -> valuePermutation[modelValue]
        [] OTHER -> requestPermutation[modelValue]] :
    nodePermutation \in Permutations(Nodes \ {JointQuorumLeader}),
    valuePermutation \in Permutations(Values),
    requestPermutation \in Permutations(ReadRequests)}

ElectionSafety ==
  \A t \in 1..MaxTerm :
    Cardinality(electedLeaders[t]) <= 1

LogMatchingFor(logs, snapshotIndexes) ==
  \A a, b \in Nodes :
    \A i \in 1..MaxLogLen :
      (/\ i \in 1..Len(logs[a])
       /\ i \in 1..Len(logs[b])
       /\ LogicalEntryFrom(logs, snapshotIndexes, a, i).term
            = LogicalEntryFrom(logs, snapshotIndexes, b, i).term)
      => LogicalPrefixFrom(logs, snapshotIndexes, a, i)
           = LogicalPrefixFrom(logs, snapshotIndexes, b, i)

LogMatching ==
  /\ SnapshotIdentitySoundFor(log, snapshotIndex)
  /\ LogicalPrefixLedgerSound
  /\ LogMatchingFor(log, snapshotIndex)

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
