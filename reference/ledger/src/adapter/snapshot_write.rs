//! Application-owned staging encodes stable ledger state through the runtime writer.

use std::{
    error::Error,
    fmt,
    io::{self, BufWriter, Write},
};

use rafter::{ApplicationSnapshotMetadata, LogIndex, RaftSnapshot, RaftSnapshotMetadata};
use rafter_runtime::snapshot_io::{
    SnapshotCancellation, SnapshotPayloadWriter, SnapshotWriteError, SnapshotWriteOptions,
};

use super::{codec, DurableLedgerStateMachine, LedgerCodecError, LedgerStateMachine};
use crate::{Ledger, LedgerConfig};

const BUFFER_BYTES: usize = 16 * 1024;

/// Why a ledger snapshot could not be completed in private staging.
#[derive(Debug)]
pub enum LedgerSnapshotWriteError {
    /// A previous application-store failure requires recovery first.
    RequiresReopen,
    /// The requested descriptor names an unsupported application schema.
    UnsupportedSchema(ApplicationSnapshotMetadata),
    /// A collection cannot be represented in the existing version-1 format.
    Codec(LedgerCodecError),
    /// Encoding or draining staging failed, with its original error chain.
    Write(io::Error),
    /// The bounded payload writer refused admission or completion.
    Payload(SnapshotWriteError),
}

impl fmt::Display for LedgerSnapshotWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequiresReopen => {
                f.write_str("ledger store requires reopening before snapshot creation")
            }
            Self::UnsupportedSchema(schema) => write!(
                f,
                "unsupported ledger snapshot schema {} version {}",
                schema.kind, schema.version
            ),
            Self::Codec(error) => write!(f, "ledger snapshot encoding failed: {error}"),
            Self::Write(error) => write!(f, "ledger snapshot staging failed: {error}"),
            Self::Payload(error) => write!(f, "ledger snapshot payload failed: {error}"),
        }
    }
}

impl Error for LedgerSnapshotWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::Write(error) => Some(error),
            Self::Payload(error) => Some(error),
            Self::RequiresReopen | Self::UnsupportedSchema(_) => None,
        }
    }
}

impl LedgerStateMachine {
    /// Encodes the current ledger directly into private staging, returning its descriptor.
    ///
    /// Borrows stable model state and retains one 16 KiB encoder buffer. The
    /// descriptor must name kind `ledger`, version 1, and the exact applied floor.
    /// Success flushes the sink; the caller still owns fsync, immutable chunk-source
    /// construction, cleanup, and publication. On failure no descriptor is returned.
    /// The sink is owned and dropped on error; pass a mutable reference to retain it.
    ///
    /// # Errors
    /// Refuses a schema/floor mismatch, unrepresentable counts, cancellation, or
    /// any write/flush failure. Partial staging must never be published on error.
    pub fn write_snapshot_to<W: Write>(
        &self,
        metadata: RaftSnapshotMetadata,
        sink: W,
        cancellation: SnapshotCancellation,
    ) -> Result<(W, RaftSnapshot), LedgerSnapshotWriteError> {
        write(
            &self.ledger,
            self.config,
            self.applied_index,
            metadata,
            sink,
            cancellation,
        )
    }
}

impl DurableLedgerStateMachine {
    /// Encodes healthy, quiescent durable state into private staging with bounded buffering.
    ///
    /// Uses the same schema and staging contract as [`LedgerStateMachine::write_snapshot_to`].
    /// A service compaction builder can call this while it owns the quiescent
    /// application, fsync the returned staging file, and expose it as an immutable
    /// chunk source to the existing runtime compaction path.
    ///
    /// # Errors
    /// Refuses a store requiring recovery, unsupported schema or floor, invalid
    /// counts, cancellation, and sink failures. Success does not fsync or publish.
    pub fn write_snapshot_to<W: Write>(
        &self,
        metadata: RaftSnapshotMetadata,
        sink: W,
        cancellation: SnapshotCancellation,
    ) -> Result<(W, RaftSnapshot), LedgerSnapshotWriteError> {
        if self.store().requires_reopen() {
            return Err(LedgerSnapshotWriteError::RequiresReopen);
        }
        write(
            self.ledger(),
            self.config(),
            self.store().applied_index(),
            metadata,
            sink,
            cancellation,
        )
    }
}

fn write<W: Write>(
    ledger: &Ledger,
    config: LedgerConfig,
    at: LogIndex,
    metadata: RaftSnapshotMetadata,
    sink: W,
    cancellation: SnapshotCancellation,
) -> Result<(W, RaftSnapshot), LedgerSnapshotWriteError> {
    if metadata.application.kind.as_str() != "ledger"
        || metadata.application.version.get() != u16::from(codec::SNAPSHOT_FORMAT_VERSION)
    {
        return Err(LedgerSnapshotWriteError::UnsupportedSchema(
            metadata.application,
        ));
    }
    let writer = SnapshotPayloadWriter::new(
        sink,
        metadata,
        at,
        SnapshotWriteOptions::new(codec::snapshot_payload_limit(config))
            .with_chunk_bytes(16 * 1024),
        cancellation,
    )
    .map_err(LedgerSnapshotWriteError::Payload)?;
    let mut buffered = BufWriter::with_capacity(BUFFER_BYTES, writer);
    if let Err(error) = codec::encode_snapshot_stream(&mut buffered, at.0, ledger) {
        // Disassemble explicitly: a failed attempt must not flush remaining
        // buffered bytes from BufWriter's Drop implementation.
        let _ = buffered.into_parts();
        return Err(match error {
            codec::SnapshotEncodingError::Codec(error) => LedgerSnapshotWriteError::Codec(error),
            codec::SnapshotEncodingError::Sink(error) => LedgerSnapshotWriteError::Write(error),
        });
    }
    let writer = buffered.into_inner().map_err(|error| {
        let (error, buffered) = error.into_parts();
        let _ = buffered.into_parts();
        LedgerSnapshotWriteError::Write(error)
    })?;
    writer.finish().map_err(LedgerSnapshotWriteError::Payload)
}
