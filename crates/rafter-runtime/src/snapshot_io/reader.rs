//! Sequential reads with exact chunk validation and explicit final verification.

use super::{
    SnapshotCancellation, SnapshotReadError, SnapshotReadOptions, VerifiedSnapshotPayload,
};
use rafter::{LogIndex, RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource};
use rafter_crc32::RunningCrc32;
use std::io::{self, Read};

/// A bounded `Read` adapter over one immutable snapshot's chunk source.
///
/// The reader retains at most one configured chunk, independently of payload
/// length and caller read-buffer size. Sources must honor the requested length
/// without allocating more than their own budget. Every request carries the
/// complete descriptor and transfer identity. The source must keep those bytes
/// stable for this attempt. Call `finish` after decoding into temporary state;
/// bytes returned by `Read` are not yet safe to publish as application state.
pub struct SnapshotPayloadReader<'a> {
    snapshot: &'a RaftSnapshot,
    source: &'a dyn SnapshotChunkSource,
    options: SnapshotReadOptions,
    cancellation: SnapshotCancellation,
    buffer: Vec<u8>,
    cursor: usize,
    consumed: u64,
    checksum: RunningCrc32,
    failure: Option<SnapshotReadError>,
}

impl<'a> SnapshotPayloadReader<'a> {
    /// Checks total length and the application's durable floor before any read.
    ///
    /// Raft metadata/membership policy remains with the runtime. The application
    /// must hold a quiescent installation boundary so its floor cannot change
    /// between this check and publication.
    ///
    /// # Errors
    /// Refuses zero chunk sizes, excessive payloads, regressing floors, or an
    /// already-cancelled attempt without calling the source.
    pub fn new(
        snapshot: &'a RaftSnapshot,
        source: &'a dyn SnapshotChunkSource,
        applied_floor: LogIndex,
        options: SnapshotReadOptions,
        cancellation: SnapshotCancellation,
    ) -> Result<Self, SnapshotReadError> {
        if options.chunk_bytes == 0 {
            return Err(SnapshotReadError::ZeroChunkSize);
        }
        if snapshot.application_payload_len > options.max_payload_bytes {
            return Err(SnapshotReadError::PayloadLimit {
                declared: snapshot.application_payload_len,
                limit: options.max_payload_bytes,
            });
        }
        if snapshot.metadata.last_included_index < applied_floor {
            return Err(SnapshotReadError::BehindAppliedFloor {
                snapshot: snapshot.metadata.last_included_index,
                applied: applied_floor,
            });
        }
        if cancellation.is_cancelled() {
            return Err(SnapshotReadError::Cancelled);
        }
        Ok(Self {
            snapshot,
            source,
            options,
            cancellation,
            buffer: Vec::new(),
            cursor: 0,
            consumed: 0,
            checksum: RunningCrc32::new(),
            failure: None,
        })
    }

    /// Checks complete consumption, final CRC32, and cancellation.
    ///
    /// This consumes the reader. It does not drain unread bytes, publish state,
    /// flush a destination, or fsync. Dropping an unfinished reader abandons its
    /// attempt without producing a receipt.
    ///
    /// # Errors
    /// Returns the first read failure, cancellation, incomplete consumption, or
    /// a checksum mismatch. Never publish temporary application state on error.
    pub fn finish(self) -> Result<VerifiedSnapshotPayload, SnapshotReadError> {
        self.verify()?;
        Ok(VerifiedSnapshotPayload {
            snapshot: self.snapshot.clone(),
        })
    }

    fn verify(&self) -> Result<(), SnapshotReadError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.cancellation.is_cancelled() {
            return Err(SnapshotReadError::Cancelled);
        }
        if self.consumed != self.snapshot.application_payload_len {
            return Err(SnapshotReadError::Incomplete {
                consumed: self.consumed,
                expected: self.snapshot.application_payload_len,
            });
        }
        let actual = self.checksum.value();
        let expected = self.snapshot.application_payload_crc32;
        if actual != expected {
            return Err(SnapshotReadError::ChecksumMismatch { expected, actual });
        }
        Ok(())
    }

    fn read_payload(&mut self, bytes: &mut [u8]) -> Result<usize, SnapshotReadError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.cancellation.is_cancelled() {
            return Err(SnapshotReadError::Cancelled);
        }
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.consumed == self.snapshot.application_payload_len {
            self.verify()?;
            return Ok(0);
        }
        if self.cursor == self.buffer.len() {
            // Release the previous chunk before asking for another allocation.
            self.buffer = Vec::new();
            let len = u32::try_from(
                (self.snapshot.application_payload_len - self.consumed)
                    .min(u64::from(self.options.chunk_bytes)),
            )
            .unwrap_or(self.options.chunk_bytes);
            // If the source unwinds and the embedding catches it, keep this
            // attempt fenced rather than reusing partially advanced state.
            self.failure = Some(SnapshotReadError::SourceInterrupted {
                offset: self.consumed,
            });
            let chunk = self.source.snapshot_chunk(SnapshotChunkRequest {
                transfer_id: self.snapshot.transfer_id(),
                metadata: &self.snapshot.metadata,
                total_payload_len: self.snapshot.application_payload_len,
                application_payload_crc32: self.snapshot.application_payload_crc32,
                offset: self.consumed,
                len,
            });
            if self.cancellation.is_cancelled() {
                return Err(SnapshotReadError::Cancelled);
            }
            let chunk = chunk.ok_or(SnapshotReadError::SourceUnavailable {
                offset: self.consumed,
            })?;
            if chunk.len() != len as usize {
                return Err(SnapshotReadError::ChunkLength {
                    offset: self.consumed,
                    expected: len,
                    actual: chunk.len(),
                });
            }
            if chunk.capacity() > self.options.chunk_bytes as usize {
                return Err(SnapshotReadError::ChunkCapacity {
                    actual: chunk.capacity(),
                    limit: self.options.chunk_bytes,
                });
            }
            self.buffer = chunk;
            self.cursor = 0;
            self.failure = None;
        }
        let count = bytes.len().min(self.buffer.len() - self.cursor);
        bytes[..count].copy_from_slice(&self.buffer[self.cursor..self.cursor + count]);
        self.checksum.update(&bytes[..count]);
        self.cursor += count;
        self.consumed += count as u64;
        Ok(count)
    }
}

impl Read for SnapshotPayloadReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.read_payload(bytes).map_err(|error| {
            self.failure = Some(error.clone());
            // Interrupted would cause standard copy/read helpers to retry a
            // permanently cancelled attempt forever.
            let kind = if error == SnapshotReadError::Cancelled {
                io::ErrorKind::Other
            } else {
                io::ErrorKind::InvalidData
            };
            io::Error::new(kind, error)
        })
    }
}

impl std::fmt::Debug for SnapshotPayloadReader<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotPayloadReader")
            .field("snapshot", &self.snapshot)
            .field("consumed", &self.consumed)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}
