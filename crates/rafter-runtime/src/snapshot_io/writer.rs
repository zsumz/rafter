//! Streaming creation computes a descriptor only for a successfully flushed sink.

use super::{SnapshotCancellation, SnapshotWriteError, SnapshotWriteOptions};
use rafter::{LogIndex, RaftSnapshot, RaftSnapshotMetadata};
use rafter_crc32::RunningCrc32;
use std::io::{self, Write};

/// Bounded `Write` adapter that constructs a snapshot descriptor as bytes are encoded.
///
/// The writer retains no payload buffer. It offers at most one configured chunk
/// per sink call, but the encoder and sink must bound their own allocations.
/// The sink must be private staging storage: failed or cancelled calls can have
/// written bytes, and neither dropping nor failing the writer rolls them back.
/// Cancellation after a successful sink write still reports its accepted count
/// to honor `Write`; it fences later calls and completion instead of hiding bytes.
/// A fresh payload normally starts at offset zero; a sink with a prefix must
/// expose only the newly written payload interval through its eventual source.
///
/// Keep the application quiescent while encoding exactly its supplied durable
/// floor. After `finish`, make the returned bytes available through an immutable
/// `SnapshotChunkSource` and the returned descriptor, for example to
/// `DurableService::compact_snapshot_with`. The writer does not fsync, publish,
/// or validate application schema, Raft terms, membership, or log eligibility.
///
/// ```
/// use std::io::{self, Write};
/// use rafter::{LogIndex, RaftSnapshot, RaftSnapshotMetadata};
/// use rafter_runtime::snapshot_io::{
///     SnapshotCancellation, SnapshotPayloadWriter, SnapshotWriteOptions,
/// };
///
/// fn encode_snapshot<W: Write>(
///     staging: W,
///     metadata: RaftSnapshotMetadata,
///     floor: LogIndex,
///     encode: impl FnOnce(&mut dyn Write) -> io::Result<()>,
/// ) -> Result<(W, RaftSnapshot), Box<dyn std::error::Error>> {
///     let mut writer = SnapshotPayloadWriter::new(
///         staging, metadata, floor, SnapshotWriteOptions::new(1024 * 1024 * 1024),
///         SnapshotCancellation::new(),
///     )?;
///     encode(&mut writer)?; // An encoder error must never fall through to finish.
///     Ok(writer.finish()?)  // Flushed, but not fsynced or published.
/// }
/// ```
pub struct SnapshotPayloadWriter<W> {
    sink: W,
    metadata: RaftSnapshotMetadata,
    options: SnapshotWriteOptions,
    cancellation: SnapshotCancellation,
    written: u64,
    checksum: RunningCrc32,
    failure: Option<SnapshotWriteError>,
}

impl<W: Write> SnapshotPayloadWriter<W> {
    /// Validates options and an exact application floor without calling the sink.
    ///
    /// `sink` is owned and dropped on refusal; pass a mutable reference when the
    /// embedding must retain it for cleanup. Staging cleanup remains caller-owned.
    ///
    /// # Errors
    /// Refuses zero chunk sizes, mismatched applied floors, or cancellation.
    pub fn new(
        sink: W,
        metadata: RaftSnapshotMetadata,
        applied_floor: LogIndex,
        options: SnapshotWriteOptions,
        cancellation: SnapshotCancellation,
    ) -> Result<Self, SnapshotWriteError> {
        if options.chunk_bytes == 0 {
            return Err(SnapshotWriteError::ZeroChunkSize);
        }
        if metadata.last_included_index != applied_floor {
            return Err(SnapshotWriteError::AppliedFloorMismatch {
                snapshot: metadata.last_included_index,
                applied: applied_floor,
            });
        }
        if cancellation.is_cancelled() {
            return Err(SnapshotWriteError::Cancelled);
        }
        Ok(Self {
            sink,
            metadata,
            options,
            cancellation,
            written: 0,
            checksum: RunningCrc32::new(),
            failure: None,
        })
    }

    /// Returns the bytes accepted before any terminal failure was observed.
    /// A failed or panicking sink call can have unknown effects beyond this count.
    #[must_use]
    pub const fn bytes_written(&self) -> u64 {
        self.written
    }

    /// Flushes staging and returns its sink and exact length/checksum descriptor.
    ///
    /// This retries sink `Interrupted` errors, checking cancellation each time.
    /// Successful flushing is not fsync or atomic publication. The application
    /// must finish its encoder successfully before calling this method; the
    /// writer cannot detect a semantic error outside its own `Write` calls.
    ///
    /// # Errors
    /// Returns a prior terminal failure, cancellation, or a flush error. On error
    /// the sink is dropped without a descriptor; no staged bytes may be published.
    pub fn finish(mut self) -> Result<(W, RaftSnapshot), SnapshotWriteError> {
        loop {
            match self.flush_payload() {
                Err(error) if error.retryable() => continue,
                result => result?,
            }
            break;
        }
        let snapshot = RaftSnapshot::new(self.metadata, self.written, self.checksum.value());
        Ok((self.sink, snapshot))
    }

    fn check_ready(&mut self) -> Result<(), SnapshotWriteError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.cancellation.is_cancelled() {
            return Err(self.fail(SnapshotWriteError::Cancelled));
        }
        Ok(())
    }

    fn fail(&mut self, error: SnapshotWriteError) -> SnapshotWriteError {
        self.failure = Some(error.clone());
        error
    }

    fn accept_sink_result<T>(&mut self, result: io::Result<T>) -> Result<T, SnapshotWriteError> {
        // The call returned: replace the unwind fence with its actual outcome.
        self.failure = None;
        result.map_err(|error| {
            if self.cancellation.is_cancelled() {
                return self.fail(SnapshotWriteError::Cancelled);
            }
            let error = SnapshotWriteError::Sink {
                written: self.written,
                error: std::sync::Arc::new(error),
            };
            if error.retryable() {
                error
            } else {
                self.fail(error)
            }
        })
    }

    fn write_payload(&mut self, bytes: &[u8]) -> Result<usize, SnapshotWriteError> {
        self.check_ready()?;
        if bytes.is_empty() {
            return Ok(0);
        }
        // Check the whole caller buffer before any partial write. This also
        // proves that adding a subsequently accepted count cannot overflow.
        let remaining = self.options.max_payload_bytes - self.written;
        if u64::try_from(bytes.len()).map_or(true, |len| len > remaining) {
            return Err(self.fail(SnapshotWriteError::PayloadLimit {
                written: self.written,
                attempted: bytes.len(),
                limit: self.options.max_payload_bytes,
            }));
        }
        let offered = bytes.len().min(self.options.chunk_bytes as usize);
        self.failure = Some(SnapshotWriteError::SinkInterrupted {
            written: self.written,
        });
        let result = self.sink.write(&bytes[..offered]);
        let count = self.accept_sink_result(result)?;
        if count == 0 {
            return Err(self.fail(SnapshotWriteError::WriteZero {
                written: self.written,
            }));
        }
        if count > offered {
            return Err(self.fail(SnapshotWriteError::InvalidWriteCount {
                offered,
                reported: count,
            }));
        }
        self.checksum.update(&bytes[..count]);
        self.written += count as u64;
        if self.cancellation.is_cancelled() {
            // `Write::write` must report bytes the sink accepted. Prevent the
            // next call/finish without claiming this successful write wrote none.
            self.fail(SnapshotWriteError::Cancelled);
        }
        Ok(count)
    }

    fn flush_payload(&mut self) -> Result<(), SnapshotWriteError> {
        self.check_ready()?;
        self.failure = Some(SnapshotWriteError::SinkInterrupted {
            written: self.written,
        });
        let result = self.sink.flush();
        self.accept_sink_result(result)?;
        self.check_ready()
    }
}

impl<W: Write> Write for SnapshotPayloadWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_payload(bytes)
            .map_err(SnapshotWriteError::into_io)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_payload().map_err(SnapshotWriteError::into_io)
    }
}

impl<W> std::fmt::Debug for SnapshotPayloadWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotPayloadWriter")
            .field("metadata", &self.metadata)
            .field("written", &self.written)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}
