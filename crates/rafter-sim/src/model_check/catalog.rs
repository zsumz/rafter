pub(super) const ST_01_STATE_WELL_FORMEDNESS: &str = "ST-01 state well-formedness";
pub(super) const EL_01_TERM_MONOTONICITY: &str = "EL-01 term monotonicity";
pub(super) const EL_02_ONE_DURABLE_VOTE_PER_TERM: &str = "EL-02 one durable vote per term";
pub(super) const EL_05_ELECTION_SAFETY_OVER_HISTORY: &str = "EL-05 election safety over history";
pub(super) const EL_06_LEADER_HAS_VALID_ELECTION_QUORUM: &str =
    "EL-06 leader has valid election quorum";
pub(super) const LG_01_LEADER_APPEND_ONLY: &str = "LG-01 leader append-only";
pub(super) const LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE: &str =
    "LG-02 truthful AppendEntries acceptance";
pub(super) const LG_03_LOG_MATCHING: &str = "LG-03 log matching";
pub(super) const LG_04_COMMITTED_PREFIX_STABILITY: &str = "LG-04 committed-prefix stability";
pub(super) const LG_05_LEADER_COMPLETENESS: &str = "LG-05 leader completeness";
pub(super) const CM_01_COMMIT_INDEX_MONOTONICITY_AND_BOUNDS: &str =
    "CM-01 commit-index monotonicity and bounds";
pub(super) const CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM: &str =
    "CM-02 commit requires effective quorum";
pub(super) const CM_03_LEADERS_ONLY_COMMIT_CURRENT_TERM_ENTRIES: &str =
    "CM-03 leaders only commit current-term entries";
pub(super) const AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION: &str =
    "AP-01 ordered exactly-once committed application";
pub(super) const AP_02_STATE_MACHINE_SAFETY: &str = "AP-02 state-machine safety";
pub(super) const MB_01_MEMBERSHIP_WELL_FORMEDNESS: &str = "MB-01 membership well-formedness";
pub(super) const MB_03_SERIALIZED_CONFIGURATION_CHANGES: &str =
    "MB-03 serialized configuration changes";
pub(super) const MB_04_MONOTONE_CONFIGURATION_TRANSITION_AND_IDENTITY: &str =
    "MB-04 monotone configuration transition and identity";
pub(super) const RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR: &str =
    "RD-03 read barrier covers committed floor";
pub(super) const RD_04_APPLY_BEFORE_SERVING_A_READ: &str = "RD-04 apply before serving a read";
pub(super) const RD_06_CLIENT_HISTORY_LINEARIZABILITY: &str =
    "RD-06 client history linearizability";
pub(super) const PS_03_EXACT_DURABLE_RESTART: &str = "PS-03 exact durable restart";
pub(super) const PS_04_APPLIED_FLOOR_RECOVERY: &str = "PS-04 applied-floor recovery";
pub(super) const SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE: &str =
    "SS-01 atomic monotone snapshot state";
pub(super) const SS_03_SNAPSHOT_LOG_INDEX_GEOMETRY: &str = "SS-03 snapshot/log index geometry";
pub(super) const SS_04_SNAPSHOT_TRANSFER_INTEGRITY: &str = "SS-04 snapshot-transfer integrity";
pub(super) const SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE: &str = "SS-05 snapshot semantic equivalence";
pub(super) const LV_01_POST_HEAL_LEADER_CONVERGENCE: &str = "LV-01 post-heal leader convergence";
pub(super) const LV_02_PROPOSAL_PROGRESS: &str = "LV-02 proposal progress";
pub(super) const LV_03_FEATURE_OPERATION_PROGRESS: &str = "LV-03 feature-operation progress";
