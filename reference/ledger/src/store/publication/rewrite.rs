//! Bounded staging preserves the existing rename publication and fault boundaries.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
};

use crate::store::{
    fault::{WriteFault, WriteFaultSite},
    format::HEADER_LEN,
    frame::encode_header,
    frame_output::FramePlan,
    journal_file::{staged_path, sync_directory},
    LedgerStore, LedgerStoreError,
};
use crate::{adapter::codec::SnapshotEncodingError, LedgerConfig};

const BUFFER_BYTES: usize = 16 * 1024;

impl LedgerStore {
    pub(super) fn rewrite(
        &mut self,
        frame: &FramePlan<'_>,
        plan: u64,
    ) -> Result<u64, LedgerStoreError> {
        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeFirstByte) {
            return Err(self.publication_failed(error));
        }
        let path = staged_path(&self.directory);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .map_err(|source| LedgerStoreError::Io {
                operation: "open the staged ledger journal",
                path: path.clone(),
                source,
            })?;
        let limit = match self.faults.fault_for(plan) {
            Some(WriteFault::AfterBytes(stop)) => stop,
            _ => u64::MAX,
        };
        let mut sink = StagingSink {
            file: &mut file,
            remaining: limit,
            injected: false,
            operation: "write the staged ledger journal",
        };
        if let Err(error) = write_staged(&mut sink, self.config, frame) {
            let error = if sink.injected {
                self.take_fault(plan, WriteFaultSite::AfterBytes)
                    .expect("only an armed byte boundary can interrupt staging")
            } else {
                match error {
                    SnapshotEncodingError::Codec(error) => LedgerStoreError::Image(error),
                    SnapshotEncodingError::Sink(source) => LedgerStoreError::Io {
                        operation: sink.operation,
                        path: path.clone(),
                        source,
                    },
                }
            };
            return Err(self.publication_failed(error));
        }

        if let Some(error) = self.take_fault(plan, WriteFaultSite::AtFileSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "sync the staged ledger journal",
                path: path.clone(),
                source,
            })
        })?;
        drop(file);

        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeRename) {
            return Err(self.publication_failed(error));
        }
        fs::rename(&path, &self.journal_path).map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "publish the staged ledger journal",
                path: self.journal_path.clone(),
                source,
            })
        })?;
        if let Some(error) = self.take_fault(plan, WriteFaultSite::AfterRename) {
            return Err(self.publication_failed(error));
        }
        sync_directory(&self.directory).map_err(|error| self.publication_failed(error))?;
        Ok(HEADER_LEN as u64 + frame.len())
    }
}

/// Drain only the encoder buffer. The caller then syncs the unbuffered file.
fn write_staged<W: Write>(
    sink: W,
    config: LedgerConfig,
    frame: &FramePlan<'_>,
) -> Result<W, SnapshotEncodingError> {
    let mut buffered = BufWriter::with_capacity(BUFFER_BYTES, sink);
    let encoded = buffered
        .write_all(&encode_header(config))
        .map_err(SnapshotEncodingError::Sink)
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

/// The fault budget covers the complete staged journal, across all write calls.
struct StagingSink<'a> {
    file: &'a mut File,
    remaining: u64,
    injected: bool,
    operation: &'static str,
}

impl Write for StagingSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            self.operation = "sync an interrupted ledger write";
            self.file.sync_data()?;
            self.injected = true;
            return Err(io::Error::other("injected ledger staging boundary"));
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
#[path = "rewrite_tests.rs"]
mod tests;
