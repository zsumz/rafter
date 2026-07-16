//! Stable and joint membership wire grammar and canonical reconstruction.

use rafter::{JointMembership, MembershipConfig, MembershipSet, NodeId};

use crate::{
    v1::tags::MembershipTag,
    wire::{Reader, Sink, Writer},
    DecodePeerMessageError, EncodePeerMessageError,
};

pub(crate) const NODE_ID_BYTES: usize = 8;

pub(super) fn encode_config<S: Sink>(
    writer: &mut Writer<S>,
    membership: &MembershipConfig,
) -> Result<(), EncodePeerMessageError> {
    match membership {
        MembershipConfig::Stable(stable) => {
            writer.u8(MembershipTag::Stable.into());
            encode_set(writer, stable)
        }
        MembershipConfig::Joint(joint) => {
            writer.u8(MembershipTag::Joint.into());
            encode_set(writer, joint.old())?;
            encode_set(writer, joint.new_membership())
        }
    }
}

pub(super) fn decode_config(
    reader: &mut Reader<'_>,
) -> Result<MembershipConfig, DecodePeerMessageError> {
    match MembershipTag::try_from(reader.u8()?)? {
        MembershipTag::Stable => decode_set(reader).map(MembershipConfig::stable),
        MembershipTag::Joint => {
            let old = decode_set(reader)?;
            let new = decode_set(reader)?;
            Ok(MembershipConfig::joint(old, new))
        }
    }
}

pub(super) fn encode_set<S: Sink>(
    writer: &mut Writer<S>,
    membership: &MembershipSet,
) -> Result<(), EncodePeerMessageError> {
    writer.length_u16("membership_voter_count", membership.voters().len())?;
    for voter in membership.voters() {
        writer.u64(voter.0);
    }
    writer.length_u16("membership_learner_count", membership.learners().len())?;
    for learner in membership.learners() {
        writer.u64(learner.0);
    }
    Ok(())
}

pub(super) fn decode_set(reader: &mut Reader<'_>) -> Result<MembershipSet, DecodePeerMessageError> {
    let voters = decode_node_set(reader)?;
    let learners = decode_node_set(reader)?;

    require_canonical_order(&voters, "membership_voters")?;
    require_canonical_order(&learners, "membership_learners")?;
    MembershipSet::new(voters, learners).map_err(DecodePeerMessageError::InvalidMembership)
}

pub(super) fn joint(old: MembershipSet, new: MembershipSet) -> JointMembership {
    JointMembership::new(old, new)
}

fn decode_node_set(reader: &mut Reader<'_>) -> Result<Vec<NodeId>, DecodePeerMessageError> {
    let count = reader.u16()? as usize;
    let capacity = membership_node_capacity(count, reader.remaining());
    let mut nodes = Vec::with_capacity(capacity);
    for _ in 0..count {
        nodes.push(NodeId(reader.u64()?));
    }
    Ok(nodes)
}

pub(crate) fn membership_node_capacity(count: usize, remaining: usize) -> usize {
    count.min(remaining / NODE_ID_BYTES)
}

fn require_canonical_order(
    nodes: &[NodeId],
    field: &'static str,
) -> Result<(), DecodePeerMessageError> {
    if nodes.windows(2).any(|pair| pair[0] > pair[1]) {
        Err(DecodePeerMessageError::NonCanonicalMembershipOrder { field })
    } else {
        Ok(())
    }
}
