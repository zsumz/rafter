//! Field-sized reads preserve codec errors without collecting an encoded frame.

use std::io::{self, Read};

use super::LedgerCodecError;

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

pub(super) struct Stream<R, F> {
    pub(super) reader: R,
    pub(super) remaining: u64,
    pub(super) map_error: F,
}

impl<R: Read, F: Fn(io::Error) -> E, E: From<LedgerCodecError>> Input for Stream<R, F> {
    type Error = E;

    fn take<const N: usize>(&mut self) -> Result<[u8; N], Self::Error> {
        if self.remaining < N as u64 {
            return Err(LedgerCodecError::TruncatedFrame {
                required: N,
                available: usize::try_from(self.remaining).unwrap_or(usize::MAX),
            }
            .into());
        }
        let mut field = [0; N];
        self.reader
            .read_exact(&mut field)
            .map_err(&self.map_error)?;
        self.remaining -= N as u64;
        Ok(field)
    }

    fn remaining(&self) -> u64 {
        self.remaining
    }
}
