//! Bounds-checked reader for the durable session-state grammar.

use super::DecodeTransportSessionStateError;

pub(super) struct Reader<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub(super) const fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    pub(super) const fn position(&self) -> usize {
        self.position
    }

    pub(super) fn u8(&mut self) -> Result<u8, DecodeTransportSessionStateError> {
        Ok(self.bytes(1)?[0])
    }

    pub(super) fn u16(&mut self) -> Result<u16, DecodeTransportSessionStateError> {
        Ok(u16::from_be_bytes(self.array::<2>()?))
    }

    pub(super) fn u32(&mut self) -> Result<u32, DecodeTransportSessionStateError> {
        Ok(u32::from_be_bytes(self.array::<4>()?))
    }

    pub(super) fn u64(&mut self) -> Result<u64, DecodeTransportSessionStateError> {
        Ok(u64::from_be_bytes(self.array::<8>()?))
    }

    pub(super) fn array<const N: usize>(
        &mut self,
    ) -> Result<[u8; N], DecodeTransportSessionStateError> {
        let bytes = self.bytes(N)?;
        let mut output = [0; N];
        output.copy_from_slice(bytes);
        Ok(output)
    }

    pub(super) fn bytes(
        &mut self,
        len: usize,
    ) -> Result<&'a [u8], DecodeTransportSessionStateError> {
        let remaining = self.input.len().saturating_sub(self.position);
        if len > remaining {
            return Err(DecodeTransportSessionStateError::UnexpectedEnd {
                needed: len,
                remaining,
            });
        }
        let start = self.position;
        self.position += len;
        Ok(&self.input[start..self.position])
    }

    pub(super) fn finish(self) -> Result<(), DecodeTransportSessionStateError> {
        let remaining = self.input.len().saturating_sub(self.position);
        if remaining == 0 {
            Ok(())
        } else {
            Err(DecodeTransportSessionStateError::TrailingBytes { remaining })
        }
    }
}
