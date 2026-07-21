//! TLC configurations used by mutation scenarios.

pub(in super::super) const REMOVED_CANDIDATE_VOTE_GUARD_CONFIG: &str = r#"SPECIFICATION RemovedCandidateVoteGuardSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "ElectionSafety"

INVARIANT TypeOK
INVARIANT RemovedCandidateVoteGuardInvariant

PROPERTY RemovedCandidateVoteGuardCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const SHORTER_CONFLICT_REPAIR_CONFIG: &str = r#"SPECIFICATION ShorterConflictRepairSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LogMatching"

INVARIANT ShorterConflictRepairInvariant

PROPERTY ShorterConflictRepairCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const FROZEN_APPEND_AUTHORITY_CONFIG: &str = r#"SPECIFICATION FrozenAppendAuthoritySpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LogMatching"

INVARIANT FrozenAppendAuthorityInvariant

PROPERTY FrozenAppendAuthorityCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const JOINT_QUORUM_REGRESSION_CONFIG: &str = r#"SPECIFICATION JointQuorumRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT JointQuorumOldSideCannotCommit
INVARIANT CommittedEntriesHaveQuorum
INVARIANT StateMachineSafety

PROPERTY JointQuorumRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const EFFECTIVE_OVERWRITE_REGRESSION_CONFIG: &str = r#"SPECIFICATION EffectiveOverwriteRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT EffectiveOverwriteRegressionInvariant
INVARIANT CommittedEntriesHaveQuorum

PROPERTY EffectiveOverwriteRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const DELAYED_HEARTBEAT_REGRESSION_CONFIG: &str = r#"SPECIFICATION DelayedHeartbeatRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT DelayedHeartbeatRegressionInvariant
INVARIANT CommittedEntriesHaveQuorum
INVARIANT StateMachineSafety

PROPERTY DelayedHeartbeatRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const SNAPSHOT_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION SnapshotLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LogMatching"

INVARIANT TypeOK
INVARIANT SnapshotLifecycleInvariant

PROPERTY SnapshotLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const SIX_ENTRY_REPLAY_CONFIG: &str = r#"SPECIFICATION SixEntryReplaySpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 6
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "StateMachineSafety"

INVARIANT TypeOK
INVARIANT SixEntryReplayInvariant

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const APPLICATION_EPOCH_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION ApplicationEpochLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "StateMachineSafety"

INVARIANT TypeOK
INVARIANT ApplicationEpochLifecycleInvariant

PROPERTY ApplicationEpochLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const STALE_MESSAGE_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION StaleMessageLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "StaleLeaderFencing"

INVARIANT StaleMessageLifecycleInvariant

PROPERTY StaleMessageLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const CLOSED_ELECTION_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION ClosedElectionLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "ElectionSafety"

INVARIANT ClosedElectionLifecycleInvariant

PROPERTY ClosedElectionLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const CLOSED_LOGICAL_PREFIX_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION ClosedLogicalPrefixLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LogMatching"

INVARIANT ClosedLogicalPrefixLifecycleInvariant

PROPERTY ClosedLogicalPrefixLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const CLOSED_TERM_PREFIX_CONFLICT_CONFIG: &str = r#"SPECIFICATION ClosedTermPrefixConflictSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LogMatching"

INVARIANT ClosedTermPrefixConflictInvariant

PROPERTY ClosedTermPrefixConflictCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const COMMIT_AUTHORITY_TERM_REGRESSION_CONFIG: &str = r#"SPECIFICATION CommitAuthorityTermRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 3
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LeaderCompleteness"

INVARIANT CommitAuthorityTermRegressionInvariant

PROPERTY CommitAuthorityTermRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const COMMITTED_LEDGER_CANONICALIZATION_CONFIG: &str = r#"SPECIFICATION CommittedLedgerCanonicalizationSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 3
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LeaderCompleteness"

INVARIANT TypeOK

PROPERTY CommittedLedgerCanonicalizationCompletes

CHECK_DEADLOCK FALSE
"#;

pub(in super::super) const SELF_REMOVAL_COMMIT_CONFIG: &str = r#"SPECIFICATION SelfRemovalCommitSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT SelfRemovalCommitInvariant
INVARIANT CommittedEntriesHaveQuorum

PROPERTY SelfRemovalCommitCompletes

CHECK_DEADLOCK FALSE
"#;
