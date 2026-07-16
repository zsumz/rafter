//! Big-endian byte readers, writers, and length-prefix validation only.

use std::{ops::Range, sync::Arc};

use crate::{DecodePeerMessageError, EncodePeerMessageError};

pub(super) trait Sink {
    fn position(&self) -> usize;
    fn write(&mut self, bytes: &[u8]);
}

#[derive(Debug, Default)]
pub(super) struct CountingSink {
    position: usize,
}

impl Sink for CountingSink {
    fn position(&self) -> usize {
        self.position
    }

    fn write(&mut self, bytes: &[u8]) {
        self.position = self.position.saturating_add(bytes.len());
    }
}

#[derive(Debug)]
pub(super) struct VecSink<'a> {
    bytes: &'a mut Vec<u8>,
}

impl<'a> VecSink<'a> {
    pub(super) fn new(bytes: &'a mut Vec<u8>) -> Self {
        Self { bytes }
    }
}

impl Sink for VecSink<'_> {
    fn position(&self) -> usize {
        self.bytes.len()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }
}

#[derive(Debug)]
pub(super) struct Writer<S> {
    sink: S,
}

impl<S: Sink> Writer<S> {
    pub(super) fn new(sink: S) -> Self {
        Self { sink }
    }

    pub(super) fn position(&self) -> usize {
        self.sink.position()
    }

    pub(super) fn bytes(&mut self, bytes: &[u8]) {
        self.sink.write(bytes);
    }

    pub(super) fn u8(&mut self, value: u8) {
        self.bytes(&[value]);
    }

    pub(super) fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    pub(super) fn length_u16(
        &mut self,
        field: &'static str,
        len: usize,
    ) -> Result<(), EncodePeerMessageError> {
        let value = u16::try_from(len).map_err(|_| EncodePeerMessageError::FieldTooLarge {
            field,
            len,
            max: u16::MAX as usize,
        })?;
        self.u16(value);
        Ok(())
    }

    pub(super) fn length_u32(
        &mut self,
        field: &'static str,
        len: usize,
    ) -> Result<(), EncodePeerMessageError> {
        let value = u32::try_from(len).map_err(|_| EncodePeerMessageError::FieldTooLarge {
            field,
            len,
            max: u32::MAX as usize,
        })?;
        self.u32(value);
        Ok(())
    }

    pub(super) fn u16(&mut self, value: u16) {
        self.bytes(&value.to_be_bytes());
    }

    pub(super) fn u32(&mut self, value: u32) {
        self.bytes(&value.to_be_bytes());
    }

    pub(super) fn u64(&mut self, value: u64) {
        self.bytes(&value.to_be_bytes());
    }

    pub(super) fn string(
        &mut self,
        field: &'static str,
        value: &str,
    ) -> Result<(), EncodePeerMessageError> {
        self.length_u16(field, value.len())?;
        self.bytes(value.as_bytes());
        Ok(())
    }

    pub(super) fn blob(
        &mut self,
        field: &'static str,
        value: &[u8],
    ) -> Result<(), EncodePeerMessageError> {
        self.length_u32(field, value.len())?;
        self.bytes(value);
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
        match self.remaining() {
            0 => Ok(()),
            remaining => Err(DecodePeerMessageError::TrailingBytes(remaining)),
        }
    }

    pub(super) fn position(&self) -> usize {
        self.position
    }

    pub(super) fn remaining(&self) -> usize {
        self.payload.len() - self.position
    }

    pub(super) fn array_4(&mut self) -> Result<[u8; 4], DecodePeerMessageError> {
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

    pub(super) fn string(&mut self, field: &'static str) -> Result<String, DecodePeerMessageError> {
        let len = self.u16()? as usize;
        let bytes = self.take(len)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| DecodePeerMessageError::InvalidUtf8 { field })?;
        Ok(value.to_owned())
    }

    pub(super) fn blob(&mut self) -> Result<Vec<u8>, DecodePeerMessageError> {
        Ok(self.blob_bytes()?.to_vec())
    }

    pub(super) fn blob_bytes(&mut self) -> Result<&'a [u8], DecodePeerMessageError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    pub(super) fn shared_blob_range(
        &mut self,
    ) -> Result<(Arc<[u8]>, Range<usize>), DecodePeerMessageError> {
        let len = self.u32()? as usize;
        let range = self.take_range(len)?;
        let bytes = self.shared_frame();
        Ok((bytes, range))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodePeerMessageError> {
        let range = self.take_range(len)?;
        Ok(&self.payload[range])
    }

    fn take_range(&mut self, len: usize) -> Result<Range<usize>, DecodePeerMessageError> {
        let remaining = self.remaining();
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
