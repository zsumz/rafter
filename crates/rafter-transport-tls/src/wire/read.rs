//! Minimal big-endian reader and writer for frozen wire formats.

/// Input ended before one requested field was complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct UnexpectedEnd;

/// Exact-slice big-endian reader.
#[derive(Debug)]
pub(super) struct Reader<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub(super) const fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    pub(super) fn u8(&mut self) -> Result<u8, UnexpectedEnd> {
        let value = *self.input.get(self.position).ok_or(UnexpectedEnd)?;
        self.position += 1;
        Ok(value)
    }

    pub(super) fn u16(&mut self) -> Result<u16, UnexpectedEnd> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    pub(super) fn u32(&mut self) -> Result<u32, UnexpectedEnd> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    pub(super) fn u64(&mut self) -> Result<u64, UnexpectedEnd> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    pub(super) fn bytes(&mut self, len: usize) -> Result<&'a [u8], UnexpectedEnd> {
        let end = self.position.checked_add(len).ok_or(UnexpectedEnd)?;
        let value = self.input.get(self.position..end).ok_or(UnexpectedEnd)?;
        self.position = end;
        Ok(value)
    }

    pub(super) const fn remaining(&self) -> usize {
        self.input.len() - self.position
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], UnexpectedEnd> {
        let bytes = self.bytes(N)?;
        let mut output = [0_u8; N];
        output.copy_from_slice(bytes);
        Ok(output)
    }
}

pub(super) fn put_u8(output: &mut Vec<u8>, value: u8) {
    output.push(value);
}

pub(super) fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

pub(super) fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

pub(super) fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}
