//! Fixed-membership adoption may never discard the old driver's durable facts.
#[path = "../src/checkpoint.rs"]
mod checkpoint;
#[path = "../src/disk.rs"]
mod disk;
#[path = "../src/policy.rs"]
mod policy;
const GROUP: u64 = 1;
const MEMBERS: [u64; 3] = [1, 2, 3];
use rafter::{LogIndex, MembershipConfig, MembershipSet, NodeId, Term};
use rafter_service::PeerControlPlaneCheckpoint;

#[test]
fn empty_checkpoint_requires_pristine_raft_storage() {
    let empty = PeerControlPlaneCheckpoint::empty(GROUP);
    assert!(policy::restore(&empty, LogIndex(0), LogIndex(0), Term(0)).is_ok());
    for (commit, last, term) in [(1, 1, 1), (0, 1, 1), (0, 0, 1)] {
        assert!(policy::restore(&empty, LogIndex(commit), LogIndex(last), Term(term)).is_err());
    }
}

#[test]
fn saved_policy_keeps_retirement_and_contradiction_fences() {
    let original = policy::observed(LogIndex(4));
    let accepts = |checkpoint: &PeerControlPlaneCheckpoint<u64>| {
        policy::restore(checkpoint, LogIndex(6), LogIndex(8), Term(7)).is_ok()
    };
    assert!(accepts(&original));
    for high_water in [None, Some(NodeId(2)), Some(NodeId(4))] {
        let mut damaged = original.clone();
        damaged.committed_id_high_water = high_water;
        assert!(!accepts(&damaged));
    }
    let mut removed = original.clone();
    removed
        .current_committed
        .as_mut()
        .unwrap()
        .membership
        .remove(&NodeId(3));
    assert!(!accepts(&removed));
    let mut newer = original.clone();
    newer.current_committed.as_mut().unwrap().through = LogIndex(7);
    assert!(!accepts(&newer));
    let mut no_current = original.clone();
    no_current.current_committed = None;
    assert!(!accepts(&no_current));
    let mut contradicted = original.clone();
    contradicted.contradicted_at = Some(LogIndex(5));
    assert!(!accepts(&contradicted));
    let mut foreign = original;
    foreign.group += 1;
    assert!(!accepts(&foreign));
}

#[test]
fn membership_refuses_joint_configs_removed_voters_and_learners() {
    let voters = || MEMBERS.map(NodeId).to_vec();
    let stable = MembershipSet::new(voters(), Vec::new()).unwrap();
    assert!(policy::membership(&MembershipConfig::stable(stable.clone())).is_ok());
    for config in [
        MembershipConfig::stable(MembershipSet::new(voters(), vec![NodeId(4)]).unwrap()),
        MembershipConfig::stable(MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![]).unwrap()),
        MembershipConfig::joint(stable.clone(), stable),
    ] {
        assert!(policy::membership(&config).is_err());
    }
}

#[test]
fn checkpoint_roundtrip_rejects_duplicate_members_and_foreign_identity() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("counter-policy-{}-{nonce}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("checkpoint");
    let expected = policy::observed(LogIndex(4));
    checkpoint::save(&path, 1, expected.clone()).unwrap();
    assert_eq!(checkpoint::load(&path, 1).unwrap(), expected);
    assert!(checkpoint::load(&path, 2).is_err());
    disk::save(
        &path,
        &serde_json::json!({
            "node": 1, "group": GROUP, "high_water": 3,
            "current": [4, [1, 2, 3, 3]], "contradicted": null,
        }),
    )
    .unwrap();
    assert!(checkpoint::load(&path, 1).is_err());
    std::fs::remove_dir_all(directory).unwrap();
}
