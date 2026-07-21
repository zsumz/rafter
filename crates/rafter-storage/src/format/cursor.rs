//! Bounds-checked big-endian byte readers and writers.

/// Structural failures shared by storage-format decoders.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CursorError {
    /// The input ended before the requested field was complete.
    UnexpectedEof { needed: usize, remaining: usize },
    /// A complete value was followed by bytes its grammar did not consume.
    TrailingBytes(usize),
}

/// Cursor over one already-bounded storage-format body.
#[derive(Debug)]
pub(crate) struct Reader<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub(crate) const fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    pub(crate) const fn position(&self) -> usize {
        self.position
    }

    pub(crate) fn remaining(&self) -> usize {
        self.input.len() - self.position
    }

    pub(crate) fn finish(&self) -> Result<(), CursorError> {
        let remaining = self.remaining();
        if remaining == 0 {
            Ok(())
        } else {
            Err(CursorError::TrailingBytes(remaining))
        }
    }

    pub(crate) fn magic(&mut self) -> Result<[u8; 4], CursorError> {
        let bytes = self.take(4)?;
        Ok([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    pub(crate) fn u8(&mut self) -> Result<u8, CursorError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, CursorError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, CursorError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, CursorError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub(crate) fn take(&mut self, len: usize) -> Result<&'a [u8], CursorError> {
        let remaining = self.remaining();
        if remaining < len {
            return Err(CursorError::UnexpectedEof {
                needed: len,
                remaining,
            });
        }

        let start = self.position;
        self.position += len;
        Ok(&self.input[start..self.position])
    }
}

/// Big-endian byte accumulator used by storage-format encoders.
#[derive(Debug, Default)]
pub(crate) struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    pub(crate) const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.bytes
    }

    pub(crate) fn bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(crate) fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
}
