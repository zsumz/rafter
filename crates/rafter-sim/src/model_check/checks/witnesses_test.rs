//! Every rare detector branch has a witness scenario that reaches it.
//!
//! The compact scenarios must together exercise each deliberately rare branch
//! they were written for, and a branch left unexercised is reported as a
//! failure, so no detector stays permanently untested behind an unreached case.

use super::*;

#[test]
fn semantic_witness_scenarios_reach_all_rare_branches() {
    let summary = check_raft_semantic_witness_safety().expect("semantic witness leg passes");
    for observation in [
        Observation::NonvoterVoteDecisions,
        Observation::JointElectionCertificates,
        Observation::PostAppendJointCommitCertificates,
        Observation::SameBoundarySnapshotInstallPairs,
        Observation::LeaderPreVoteRequestDeliveries,
        Observation::RestartNonemptyExpectedReplayComparisons,
    ] {
        assert!(summary.observations.contains(observation));
    }
}
