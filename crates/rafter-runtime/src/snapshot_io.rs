//! Bounded payload reads for application-owned snapshot installation.
//!
//! Decode into temporary application state through [`crate::snapshot_io::SnapshotPayloadReader`],
//! then call [`crate::snapshot_io::SnapshotPayloadReader::finish`] before publishing it. Successful
//! reads alone do not verify a snapshot: its final checksum or cancellation may
//! still fail. The completion receipt binds the consumed bytes to their exact
//! descriptor. The application must additionally validate its schema and any
//! encoded applied index, then publish state and applied floor atomically.
//!
//! This layer opens no files and owns no database transaction. It uses the
//! existing [`rafter::SnapshotChunkSource`], so staged or current runtime stores
//! and application-owned immutable sources use the same path. CRC32 detects
//! accidental corruption; peer authentication remains a transport obligation.
//! Application decoders and temporary destinations must have their own memory
//! and storage limits; calling `read_to_end` still allocates in the caller.
//!
//! ```
//! use std::io::Write;
//! use rafter::{LogIndex, RaftSnapshot, SnapshotChunkSource};
//! use rafter_runtime::snapshot_io::{
//!     SnapshotCancellation, SnapshotPayloadReader, SnapshotReadOptions,
//!     VerifiedSnapshotPayload,
//! };
//!
//! fn stage_payload(
//!     snapshot: &RaftSnapshot,
//!     source: &dyn SnapshotChunkSource,
//!     applied_floor: LogIndex,
//!     private_staging: &mut impl Write,
//!     cancellation: SnapshotCancellation,
//! ) -> Result<VerifiedSnapshotPayload, Box<dyn std::error::Error>> {
//!     let mut reader = SnapshotPayloadReader::new(
//!         snapshot, source, applied_floor,
//!         SnapshotReadOptions::new(1024 * 1024 * 1024).with_chunk_bytes(16 * 1024),
//!         cancellation,
//!     )?;
//!     std::io::copy(&mut reader, private_staging)?;
//!     // Still validate application schema and durably publish state + floor.
//!     Ok(reader.finish()?)
//! }
//! ```

mod contract;
mod error;
mod reader;

pub use contract::{SnapshotCancellation, SnapshotReadOptions, VerifiedSnapshotPayload};
pub use error::SnapshotReadError;
pub use reader::SnapshotPayloadReader;
