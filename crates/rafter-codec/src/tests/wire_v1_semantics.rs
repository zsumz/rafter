//! Raw-byte acceptance boundaries that freeze v1 semantics independently of core constructors.

use super::support::{
    raw_v1_joint_membership_append, raw_v1_snapshot_chunk_frame, raw_v1_stable_membership_append,
};
use crate::decode_message;

#[test]
fn v1_snapshot_identity_and_boundary_maxima_remain_accepted() {
    let maximum_id = vec![b'a'; 128];
    let every_punctuation = b"AZaz09._:-";
    let maximums = raw_v1_snapshot_chunk_frame(
        &maximum_id,
        every_punctuation,
        u16::MAX,
        u64::MAX - 1,
        u64::MAX,
        u64::MAX,
    );
    decode_message(&maximums).expect("documented v1 snapshot maxima remain accepted");

    let maximum_kind = vec![b'z'; 128];
    let punctuation_group =
        raw_v1_snapshot_chunk_frame(every_punctuation, &maximum_kind, 1, 1, 1, 1);
    decode_message(&punctuation_group)
        .expect("every documented punctuation remains accepted in both identity fields");
}

#[test]
fn v1_stable_membership_accepts_canonical_node_and_count_boundaries() {
    let node_boundaries = raw_v1_stable_membership_append(&[0, u64::MAX], &[1, u64::MAX - 1]);
    decode_message(&node_boundaries)
        .expect("canonical v1 memberships accept the full node-id range");

    let maximum_voter_count = (0..u16::MAX).map(u64::from).collect::<Vec<_>>();
    let count_boundary = raw_v1_stable_membership_append(&maximum_voter_count, &[]);
    decode_message(&count_boundary).expect("the maximum v1 membership count remains accepted");
}

#[test]
fn v1_joint_membership_accepts_canonical_boundary_values() {
    let frame =
        raw_v1_joint_membership_append(&[0, u64::MAX], &[1], &[0, u64::MAX - 1], &[u64::MAX]);
    decode_message(&frame).expect("both halves of a canonical v1 joint membership decode");
}
