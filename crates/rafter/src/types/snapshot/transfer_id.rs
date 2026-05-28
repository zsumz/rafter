use super::{
    MembershipConfig, MembershipSet, RaftSnapshotMetadata, SnapshotCommittedConfiguration,
    SnapshotTransferId,
};

/// Derives the deterministic transfer identity of a snapshot from its
/// metadata, total payload length, and application payload checksum — the
/// values every chunk of a transfer carries. Both ends of a transfer derive
/// the same identity independently, so a follower can reject chunks whose
/// claimed identity does not match their own header without any shared state.
///
/// This is a stable routing identity for a non-Byzantine protocol path. The
/// FNV-1a accumulator deliberately avoids a crypto dependency in the Raft
/// kernel; it is not collision-resistant and must not be used as an
/// adversarial integrity proof.
pub(crate) fn snapshot_transfer_id_from_parts(
    metadata: &RaftSnapshotMetadata,
    total_payload_len: u64,
    application_payload_crc32: u32,
) -> SnapshotTransferId {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

    let mut hash = FNV_OFFSET_BASIS;
    feed_str(&mut hash, metadata.group_id.as_str());
    feed_u64(&mut hash, metadata.writer_id.0);
    feed_u64(&mut hash, metadata.last_included_index.0);
    feed_u64(&mut hash, metadata.last_included_term.0);
    feed_u64(&mut hash, metadata.hard_state_term.0);
    feed_str(&mut hash, metadata.application.kind.as_str());
    feed_u64(&mut hash, u64::from(metadata.application.version.get()));
    feed_optional_committed_configuration(&mut hash, metadata.committed_configuration.as_ref());
    feed_u64(&mut hash, total_payload_len);
    feed_u64(&mut hash, u64::from(application_payload_crc32));
    SnapshotTransferId(if hash == 0 { 1 } else { hash })
}

fn feed_optional_committed_configuration(
    hash: &mut u64,
    committed: Option<&SnapshotCommittedConfiguration>,
) {
    if let Some(committed) = committed {
        feed_u64(hash, 1);
        if let Some(configuration) = committed.configuration {
            feed_u64(hash, 1);
            feed_u64(hash, configuration.index.0);
            feed_u64(hash, configuration.config_id.0);
        } else {
            feed_u64(hash, 0);
        }
        feed_membership_config(hash, &committed.membership);
    } else {
        feed_u64(hash, 0);
    }
}

fn feed_membership_config(hash: &mut u64, membership: &MembershipConfig) {
    match membership {
        MembershipConfig::Stable(stable) => {
            feed_u64(hash, 0);
            feed_membership_set(hash, stable);
        }
        MembershipConfig::Joint(joint) => {
            feed_u64(hash, 1);
            feed_membership_set(hash, joint.old());
            feed_membership_set(hash, joint.new_membership());
        }
    }
}

fn feed_membership_set(hash: &mut u64, membership: &MembershipSet) {
    feed_u64(hash, membership.voters().len() as u64);
    for voter in membership.voters() {
        feed_u64(hash, voter.0);
    }
    feed_u64(hash, membership.learners().len() as u64);
    for learner in membership.learners() {
        feed_u64(hash, learner.0);
    }
}

fn feed_str(hash: &mut u64, value: &str) {
    feed_u64(hash, value.len() as u64);
    for byte in value.as_bytes() {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

fn feed_u64(hash: &mut u64, value: u64) {
    for byte in value.to_be_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

const FNV_PRIME: u64 = 0x0100_0000_01b3;
