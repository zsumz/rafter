//! Field-sized reads preserve codec errors without collecting an encoded frame.

use std::io::Read;

use rafter::LogIndex;

use super::LedgerCodecError;
use crate::LedgerAdapterError;

pub(super) trait Input {
    type Error: From<LedgerCodecError>;
    fn take<const N: usize>(&mut self) -> Result<[u8; N], Self::Error>;
    fn remaining(&self) -> u64;
}

impl Input for &[u8] {
    type Error = LedgerCodecError;

    fn take<const N: usize>(&mut self) -> Result<[u8; N], Self::Error> {
        let (field, rest) = self
            .split_at_checked(N)
            .ok_or(LedgerCodecError::TruncatedFrame {
                required: N,
                available: self.len(),
            })?;
        *self = rest;
        Ok(field.try_into().expect("exact field width"))
    }

    fn remaining(&self) -> u64 {
        self.len() as u64
    }
}

pub(super) struct Stream<R> {
    pub(super) reader: R,
    pub(super) remaining: u64,
    pub(super) applied_index: LogIndex,
}

impl<R: Read> Input for Stream<R> {
    type Error = LedgerAdapterError;

    fn take<const N: usize>(&mut self) -> Result<[u8; N], Self::Error> {
        if self.remaining < N as u64 {
            return Err(LedgerCodecError::TruncatedFrame {
                required: N,
                available: usize::try_from(self.remaining).unwrap_or(usize::MAX),
            }
            .into());
        }
        let mut field = [0; N];
        self.reader.read_exact(&mut field).map_err(|_| {
            LedgerAdapterError::SnapshotPayloadUnavailable {
                applied_index: self.applied_index,
            }
        })?;
        self.remaining -= N as u64;
        Ok(field)
    }

    fn remaining(&self) -> u64 {
        self.remaining
    }
}
