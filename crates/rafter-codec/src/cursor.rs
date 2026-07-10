use std::{ops::Range, sync::Arc};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    CommittedConfiguration, ConfigurationId, LogIndex, MembershipConfig, MembershipSet, NodeId,
    RaftSnapshotMetadata, SharedPayload, SnapshotCommittedConfiguration, SnapshotGroupId,
    SnapshotTransferId, Term,
};

use crate::{DecodePeerMessageError, EncodePeerMessageError};

#[derive(Debug)]
pub(super) struct Writer<'a> {
    bytes: &'a mut Vec<u8>,
}

impl<'a> Writer<'a> {
    pub(super) fn with_capacity(bytes: &'a mut Vec<u8>, capacity: usize) -> Self {
        bytes.clear();
        bytes.reserve(capacity);
        Self { bytes }
    }

    pub(super) fn bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub(super) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(super) fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    pub(super) fn u16(
        &mut self,
        field: &'static str,
        value: usize,
    ) -> Result<(), EncodePeerMessageError> {
        let value = u16::try_from(value)
            .map_err(|_| EncodePeerMessageError::FieldTooLarge { field, len: value })?;
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    pub(super) fn u32(
        &mut self,
        field: &'static str,
        value: usize,
    ) -> Result<(), EncodePeerMessageError> {
        let value = u32::try_from(value)
            .map_err(|_| EncodePeerMessageError::FieldTooLarge { field, len: value })?;
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    pub(super) fn raw_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(super) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(super) fn node_id(&mut self, value: NodeId) {
        self.u64(value.0);
    }

    pub(super) fn term(&mut self, value: Term) {
        self.u64(value.0);
    }

    pub(super) fn log_index(&mut self, value: LogIndex) {
        self.u64(value.0);
    }

    pub(super) fn snapshot_transfer_id(&mut self, value: SnapshotTransferId) {
        self.u64(value.0);
    }

    pub(super) fn optional_snapshot_transfer_id(&mut self, value: Option<SnapshotTransferId>) {
        self.bool(value.is_some());
        if let Some(value) = value {
            self.snapshot_transfer_id(value);
        }
    }

    pub(super) fn string(
        &mut self,
        field: &'static str,
        value: &str,
    ) -> Result<(), EncodePeerMessageError> {
        self.u16(field, value.len())?;
        self.bytes(value.as_bytes());
        Ok(())
    }

    pub(super) fn blob(
        &mut self,
        field: &'static str,
        value: &[u8],
    ) -> Result<(), EncodePeerMessageError> {
        self.u32(field, value.len())?;
        self.bytes(value);
        Ok(())
    }

    pub(super) fn snapshot_metadata(
        &mut self,
        metadata: &RaftSnapshotMetadata,
    ) -> Result<(), EncodePeerMessageError> {
        self.string("snapshot_group_id", metadata.group_id.as_str())?;
        self.node_id(metadata.writer_id);
        self.log_index(metadata.last_included_index);
        self.term(metadata.last_included_term);
        self.term(metadata.hard_state_term);
        self.string(
            "application_snapshot_kind",
            metadata.application.kind.as_str(),
        )?;
        self.u16(
            "application_snapshot_version",
            metadata.application.version.get() as usize,
        )?;
        self.optional_snapshot_committed_configuration(metadata.committed_configuration.as_ref())
    }

    fn optional_snapshot_committed_configuration(
        &mut self,
        committed: Option<&SnapshotCommittedConfiguration>,
    ) -> Result<(), EncodePeerMessageError> {
        self.bool(committed.is_some());
        if let Some(committed) = committed {
            self.bool(committed.configuration.is_some());
            if let Some(configuration) = committed.configuration {
                self.log_index(configuration.index);
                self.u64(configuration.config_id.0);
            }
            self.membership_config(&committed.membership)?;
        }
        Ok(())
    }

    fn membership_config(
        &mut self,
        membership: &MembershipConfig,
    ) -> Result<(), EncodePeerMessageError> {
        match membership {
            MembershipConfig::Stable(stable) => {
                self.u8(0);
                self.membership_set(stable)
            }
            MembershipConfig::Joint(joint) => {
                self.u8(1);
                self.membership_set(joint.old())?;
                self.membership_set(joint.new_membership())
            }
        }
    }

    pub(super) fn membership_set(
        &mut self,
        membership: &MembershipSet,
    ) -> Result<(), EncodePeerMessageError> {
        self.u16("membership_voter_count", membership.voters().len())?;
        for voter in membership.voters() {
            self.node_id(*voter);
        }
        self.u16("membership_learner_count", membership.learners().len())?;
        for learner in membership.learners() {
            self.node_id(*learner);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct Reader<'a> {
    payload: &'a [u8],
    position: usize,
    shared_payload: Option<Arc<[u8]>>,
}

impl<'a> Reader<'a> {
    pub(super) fn new(payload: &'a [u8]) -> Self {
        Self {
            payload,
            position: 0,
            shared_payload: None,
        }
    }

    pub(super) fn finish(&self) -> Result<(), DecodePeerMessageError> {
        let remaining = self.remaining();
        if remaining == 0 {
            Ok(())
        } else {
            Err(DecodePeerMessageError::TrailingBytes(remaining))
        }
    }

    pub(super) fn position(&self) -> usize {
        self.position
    }

    /// Bytes not yet consumed. A decoded count can never legitimately imply
    /// more elements than this — every element costs at least one byte — so
    /// it caps speculative pre-allocation against a hostile length prefix.
    pub(super) fn remaining(&self) -> usize {
        self.payload.len() - self.position
    }

    pub(super) fn magic(&mut self) -> Result<[u8; 4], DecodePeerMessageError> {
        let bytes = self.take(4)?;
        Ok([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    pub(super) fn u8(&mut self) -> Result<u8, DecodePeerMessageError> {
        Ok(self.take(1)?[0])
    }

    pub(super) fn bool(&mut self) -> Result<bool, DecodePeerMessageError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(DecodePeerMessageError::InvalidBoolean(other)),
        }
    }

    pub(super) fn u16(&mut self) -> Result<u16, DecodePeerMessageError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub(super) fn u32(&mut self) -> Result<u32, DecodePeerMessageError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(super) fn u64(&mut self) -> Result<u64, DecodePeerMessageError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub(super) fn node_id(&mut self) -> Result<NodeId, DecodePeerMessageError> {
        Ok(NodeId(self.u64()?))
    }

    pub(super) fn term(&mut self) -> Result<Term, DecodePeerMessageError> {
        Ok(Term(self.u64()?))
    }

    pub(super) fn log_index(&mut self) -> Result<LogIndex, DecodePeerMessageError> {
        Ok(LogIndex(self.u64()?))
    }

    pub(super) fn snapshot_transfer_id(
        &mut self,
    ) -> Result<SnapshotTransferId, DecodePeerMessageError> {
        Ok(SnapshotTransferId(self.u64()?))
    }

    pub(super) fn optional_snapshot_transfer_id(
        &mut self,
    ) -> Result<Option<SnapshotTransferId>, DecodePeerMessageError> {
        if self.bool()? {
            Ok(Some(self.snapshot_transfer_id()?))
        } else {
            Ok(None)
        }
    }

    pub(super) fn string(&mut self, field: &'static str) -> Result<String, DecodePeerMessageError> {
        let len = self.u16()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| DecodePeerMessageError::InvalidUtf8 { field })
    }

    pub(super) fn blob(&mut self) -> Result<Vec<u8>, DecodePeerMessageError> {
        Ok(self.blob_bytes()?.to_vec())
    }

    pub(super) fn blob_bytes(&mut self) -> Result<&'a [u8], DecodePeerMessageError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    pub(super) fn shared_blob_payload(&mut self) -> Result<SharedPayload, DecodePeerMessageError> {
        let len = self.u32()? as usize;
        let range = self.take_range(len)?;
        let bytes = self.shared_frame();
        SharedPayload::from_shared_range(bytes, range.clone()).ok_or({
            DecodePeerMessageError::UnexpectedEof {
                needed: range.end,
                remaining: self.payload.len(),
            }
        })
    }

    pub(super) fn snapshot_metadata(
        &mut self,
    ) -> Result<RaftSnapshotMetadata, DecodePeerMessageError> {
        let group_id = SnapshotGroupId::new(self.string("snapshot_group_id")?)
            .map_err(DecodePeerMessageError::InvalidSnapshotGroupId)?;
        let writer_id = self.node_id()?;
        let last_included_index = self.log_index()?;
        let last_included_term = self.term()?;
        let hard_state_term = self.term()?;
        let application_kind =
            ApplicationSnapshotKind::new(self.string("application_snapshot_kind")?)
                .map_err(DecodePeerMessageError::InvalidApplicationSnapshotKind)?;
        let application_version = ApplicationSnapshotVersion::new(self.u16()?)
            .map_err(DecodePeerMessageError::InvalidApplicationSnapshotVersion)?;
        let committed_configuration = self.optional_snapshot_committed_configuration()?;
        let mut metadata = RaftSnapshotMetadata::new(
            group_id,
            writer_id,
            last_included_index,
            last_included_term,
            hard_state_term,
            ApplicationSnapshotMetadata::new(application_kind, application_version),
        )
        .map_err(DecodePeerMessageError::InvalidSnapshotMetadata)?;
        if let Some(committed_configuration) = committed_configuration {
            metadata = metadata.with_committed_configuration(committed_configuration);
        }
        Ok(metadata)
    }

    fn optional_snapshot_committed_configuration(
        &mut self,
    ) -> Result<Option<SnapshotCommittedConfiguration>, DecodePeerMessageError> {
        if !self.bool()? {
            return Ok(None);
        }
        let configuration = if self.bool()? {
            Some(CommittedConfiguration {
                index: self.log_index()?,
                config_id: ConfigurationId(self.u64()?),
            })
        } else {
            None
        };
        let membership = self.membership_config()?;
        Ok(Some(SnapshotCommittedConfiguration::new(
            configuration,
            membership,
        )))
    }

    fn membership_config(&mut self) -> Result<MembershipConfig, DecodePeerMessageError> {
        match self.u8()? {
            0 => self.membership_set().map(MembershipConfig::stable),
            1 => {
                let old = self.membership_set()?;
                let new = self.membership_set()?;
                Ok(MembershipConfig::joint(old, new))
            }
            other => Err(DecodePeerMessageError::UnknownMembershipKind(other)),
        }
    }

    pub(super) fn membership_set(&mut self) -> Result<MembershipSet, DecodePeerMessageError> {
        let voters = self.node_set()?;
        let learners = self.node_set()?;
        MembershipSet::new(voters, learners).map_err(DecodePeerMessageError::InvalidMembership)
    }

    fn node_set(&mut self) -> Result<Vec<NodeId>, DecodePeerMessageError> {
        let count = self.u16()? as usize;
        (0..count)
            .map(|_| self.node_id())
            .collect::<Result<Vec<_>, _>>()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodePeerMessageError> {
        let range = self.take_range(len)?;
        Ok(&self.payload[range])
    }

    fn take_range(&mut self, len: usize) -> Result<Range<usize>, DecodePeerMessageError> {
        let remaining = self.payload.len() - self.position;
        if remaining < len {
            return Err(DecodePeerMessageError::UnexpectedEof {
                needed: len,
                remaining,
            });
        }

        let start = self.position;
        self.position += len;
        Ok(start..self.position)
    }

    fn shared_frame(&mut self) -> Arc<[u8]> {
        self.shared_payload
            .get_or_insert_with(|| Arc::from(self.payload))
            .clone()
    }
}
