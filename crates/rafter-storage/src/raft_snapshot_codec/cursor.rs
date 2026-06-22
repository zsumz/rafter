use crate::{DecodeRaftSnapshotError, EncodeRaftSnapshotError};

pub(super) struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    pub(super) fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub(super) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn finish(self) -> Vec<u8> {
        self.bytes
    }

    pub(super) fn bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub(super) fn string(
        &mut self,
        field: &'static str,
        value: &str,
    ) -> Result<(), EncodeRaftSnapshotError> {
        let bytes = value.as_bytes();
        let len =
            u16::try_from(bytes.len()).map_err(|_| EncodeRaftSnapshotError::StringTooLong {
                field,
                len: bytes.len(),
            })?;
        self.u16(len);
        self.bytes(bytes);
        Ok(())
    }

    pub(super) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(super) fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(super) fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(super) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
}

pub(super) struct Reader<'a> {
    envelope: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub(super) fn new(envelope: &'a [u8]) -> Self {
        Self {
            envelope,
            position: 0,
        }
    }

    pub(super) fn position(&self) -> usize {
        self.position
    }

    pub(super) fn finish(&self) -> Result<(), DecodeRaftSnapshotError> {
        let remaining = self.envelope.len() - self.position;
        if remaining == 0 {
            Ok(())
        } else {
            Err(DecodeRaftSnapshotError::TrailingBytes(remaining))
        }
    }

    pub(super) fn magic(&mut self) -> Result<[u8; 4], DecodeRaftSnapshotError> {
        let bytes = self.take(4)?;
        Ok([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    pub(super) fn string(
        &mut self,
        field: &'static str,
    ) -> Result<String, DecodeRaftSnapshotError> {
        let len = self.u16()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| DecodeRaftSnapshotError::InvalidUtf8 { field })
    }

    pub(super) fn u8(&mut self) -> Result<u8, DecodeRaftSnapshotError> {
        Ok(self.take(1)?[0])
    }

    pub(super) fn u16(&mut self) -> Result<u16, DecodeRaftSnapshotError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub(super) fn u32(&mut self) -> Result<u32, DecodeRaftSnapshotError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(super) fn u64(&mut self) -> Result<u64, DecodeRaftSnapshotError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub(super) fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeRaftSnapshotError> {
        let remaining = self.envelope.len() - self.position;
        if remaining < len {
            return Err(DecodeRaftSnapshotError::UnexpectedEof {
                needed: len,
                remaining,
            });
        }

        let start = self.position;
        self.position += len;
        Ok(&self.envelope[start..self.position])
    }
}
