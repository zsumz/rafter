//! Bounded publication writes; checksums see sealed bytes, append storage does not.

use std::{
    fs::File,
    io::{self, BufWriter, Write},
};

use crate::store::{
    fault::WriteFault, format::UNSEALED_FRAME_MARK, frame::encode_header, frame_output::FramePlan,
};
use crate::{adapter::codec::SnapshotEncodingError, LedgerConfig};

const BUFFER_BYTES: usize = 16 * 1024;

pub(super) fn write_staged<W: Write>(
    sink: W,
    config: LedgerConfig,
    frame: &FramePlan<'_>,
) -> Result<W, SnapshotEncodingError> {
    write_buffered(sink, Some(config), frame)
}

pub(super) fn write_unsealed<W: Write>(
    sink: W,
    frame: &FramePlan<'_>,
) -> Result<W, SnapshotEncodingError> {
    write_buffered(Unsealed { sink, first: true }, None, frame).map(|sink| sink.sink)
}

/// Drain only the encoder buffer. The caller then syncs the unbuffered file.
fn write_buffered<W: Write>(
    sink: W,
    header: Option<LedgerConfig>,
    frame: &FramePlan<'_>,
) -> Result<W, SnapshotEncodingError> {
    let mut buffered = BufWriter::with_capacity(BUFFER_BYTES, sink);
    let encoded = match header {
        Some(config) => buffered
            .write_all(&encode_header(config))
            .map_err(SnapshotEncodingError::Sink),
        None => Ok(()),
    }
    .and_then(|()| frame.write_to(&mut buffered));
    if let Err(error) = encoded {
        let _ = buffered.into_parts(); // No retry or buffer drain from Drop.
        return Err(error);
    }
    buffered.into_inner().map_err(|error| {
        let (error, buffered) = error.into_parts();
        let _ = buffered.into_parts();
        SnapshotEncodingError::Sink(error)
    })
}

/// Mask below the encoder and buffer so both checksums cover the sealed value.
/// Returning one byte first preserves write ordering and handles partial writes
/// without copying or changing any byte the encoder owns.
struct Unsealed<W> {
    sink: W,
    first: bool,
}
impl<W: Write> Write for Unsealed<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.first && !bytes.is_empty() {
            let accepted = self.sink.write(&[UNSEALED_FRAME_MARK])?;
            if accepted != 0 {
                self.first = false;
            }
            Ok(accepted)
        } else {
            self.sink.write(bytes)
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.sink.flush()
    }
}

/// The fault budget spans every write: the frame for append, header plus frame for staging.
pub(super) struct FileSink<'a> {
    file: &'a mut File,
    remaining: u64,
    pub(super) injected: bool,
    pub(super) operation: &'static str,
}

impl<'a> FileSink<'a> {
    pub(super) fn new(
        file: &'a mut File,
        fault: Option<WriteFault>,
        operation: &'static str,
    ) -> Self {
        Self {
            file,
            remaining: match fault {
                Some(WriteFault::AfterBytes(stop)) => stop,
                _ => u64::MAX,
            },
            injected: false,
            operation,
        }
    }
}

impl Write for FileSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            self.operation = "sync an interrupted ledger write";
            self.file.sync_data()?;
            self.injected = true;
            return Err(io::Error::other("injected ledger write boundary"));
        }
        let count = bytes
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let accepted = self.file.write(&bytes[..count])?;
        self.remaining -= accepted as u64;
        Ok(accepted)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
#[path = "output_tests.rs"]
mod tests;
