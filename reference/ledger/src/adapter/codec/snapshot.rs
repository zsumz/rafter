//! One bounded snapshot grammar builds temporary, model-validated state.

use std::io::{self, Read};

use super::{
    input::{Input, Stream},
    read::Cursor,
    LedgerCodecError, SNAPSHOT_FORMAT_VERSION,
};
use crate::{model::Restore, AccountId, Ledger, LedgerConfig, SessionView, SnapshotError};

pub(crate) fn decode_snapshot<E>(payload: &[u8], config: LedgerConfig) -> Result<(u64, Ledger), E>
where
    E: From<LedgerCodecError> + From<SnapshotError>,
{
    decode(Cursor::new(payload), config)
}

pub(crate) fn decode_snapshot_stream<E>(
    reader: &mut impl Read,
    length: u64,
    config: LedgerConfig,
    map_error: impl Fn(io::Error) -> E,
) -> Result<(u64, Ledger), E>
where
    E: From<LedgerCodecError> + From<SnapshotError>,
{
    decode(
        Cursor::from_input(Stream {
            reader,
            remaining: length,
            map_error,
        }),
        config,
    )
}

fn decode<I, E>(mut cursor: Cursor<I>, config: LedgerConfig) -> Result<(u64, Ledger), E>
where
    I: Input,
    E: From<I::Error> + From<LedgerCodecError> + From<SnapshotError>,
{
    let version = cursor.take_u8()?;
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(LedgerCodecError::UnsupportedSnapshotVersion { version }.into());
    }
    let applied_index = cursor.take_u64()?;
    let successful_deposits = cursor.take_u128()?;
    let mut restore = Restore::new(config, successful_deposits);

    let account_count = cursor.take_len()?;
    cursor.require_records(account_count, 16, 4)?;
    restore.admit_accounts(account_count)?;
    for _ in 0..account_count {
        restore.account(AccountId::new(cursor.take_u64()?), cursor.take_u64()?)?;
    }

    let session_count = cursor.take_len()?;
    cursor.require_records(session_count, 13, 0)?;
    restore.admit_sessions(session_count)?;
    for _ in 0..session_count {
        restore.session(SessionView {
            client_id: cursor.take_client()?,
            session_epoch: cursor.take_session_epoch()?,
            cached: cursor.take_cached_completion()?,
        })?;
    }
    cursor.finish()?;
    Ok((applied_index, restore.finish()?))
}

/// Bound bytes by the configured state and version-1 record maxima before I/O.
/// Counts are independently checked again by the decoder before model insertion.
pub(crate) fn snapshot_payload_limit(config: LedgerConfig) -> u64 {
    let accounts = u64::try_from(config.max_accounts())
        .unwrap_or(u64::MAX)
        .min(u64::from(u32::MAX));
    // Header/counts: 33; account: 16; cached session: at most 63 bytes.
    // Both counts fit u32, so this sum is representable in u64.
    33 + 16 * accounts + 63 * u64::from(config.max_clients())
}
