//! Reading the header, then every committed frame, stopping at the first residue.
//!
//! One pass, and it is where the journal's shape becomes the two numbers
//! recovery acts on: how long the committed prefix is, and what — if anything —
//! follows it.

use rafter::LogIndex;

use crate::{adapter::codec::decode_snapshot, Ledger, LedgerConfig};

use super::{
    damage::TornTail,
    error::LedgerStoreError,
    format::HEADER_LEN,
    frame::{read_frame, verify_header},
};

/// Result of scanning a journal's bytes.
pub(super) struct JournalScan {
    /// Newest committed image, if the journal holds one.
    pub(super) image: Option<(Ledger, LogIndex)>,
    /// Number of committed frames.
    pub(super) committed_frames: u64,
    /// Byte length of the committed prefix.
    pub(super) committed_len: usize,
    /// Residue found after the committed prefix.
    pub(super) torn_tail: Option<TornTail>,
}

/// Reads the header, then every committed frame, stopping at the first residue.
pub(super) fn scan_journal(
    bytes: &[u8],
    config: LedgerConfig,
) -> Result<JournalScan, LedgerStoreError> {
    verify_header(bytes, config)?;

    let mut offset = HEADER_LEN;
    let mut image = None;
    let mut committed_frames = 0_u64;
    let mut previous_index: Option<LogIndex> = None;

    let torn_tail = loop {
        let rest = &bytes[offset..];
        if rest.is_empty() {
            break None;
        }
        let frame = match read_frame(rest) {
            Ok(frame) => frame,
            Err(tail) => break Some(tail),
        };

        let (applied_index, snapshot) =
            decode_snapshot(frame.image).map_err(LedgerStoreError::Image)?;
        let applied_index = LogIndex(applied_index);
        if let Some(previous) = previous_index {
            if applied_index < previous {
                return Err(LedgerStoreError::NonMonotonicAppliedIndex {
                    previous,
                    found: applied_index,
                });
            }
        }
        let ledger = Ledger::from_snapshot(config, snapshot).map_err(LedgerStoreError::Snapshot)?;

        previous_index = Some(applied_index);
        image = Some((ledger, applied_index));
        committed_frames += 1;
        offset += frame.len;
    };

    Ok(JournalScan {
        image,
        committed_frames,
        committed_len: offset,
        torn_tail,
    })
}
