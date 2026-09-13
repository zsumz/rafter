//! Checksummed atomic records containing existing RFLE/RFHS envelopes.
use crate::{
    crc32, decode_raft_hard_state, decode_raft_log_entry, encode_raft_hard_state,
    format::v1::log_entry::encode_raft_log_entry_appending, PersistedRaftLogEntry, RaftHardState,
};
use rafter::LogIndex;
use std::io;

pub(super) const MAGIC: &[u8; 8] = b"RFWB\0\0\0\x01";
pub(super) const HEADER: usize = 16;
pub(super) const TRAILER: usize = 8;
pub(super) const MAX_BODY: usize = 64 * 1024 * 1024;
pub(super) const MAX_RETAINED_ENCODE_BUFFER: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct Record {
    pub operation: u64,
    pub compact: Option<LogIndex>,
    pub truncate: Option<LogIndex>,
    pub hard: Option<RaftHardState>,
    pub entries: Vec<PersistedRaftLogEntry>,
}
pub(super) fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
pub(super) fn encode_reusing(record: &Record, frame: &mut Vec<u8>) -> io::Result<()> {
    frame.clear();
    frame.resize(HEADER, 0);
    frame.extend_from_slice(&record.operation.to_be_bytes());
    frame.extend_from_slice(&record.compact.map_or(u64::MAX, |i| i.0).to_be_bytes());
    frame.extend_from_slice(&record.truncate.map_or(u64::MAX, |i| i.0).to_be_bytes());
    frame.push(u8::from(record.hard.is_some()));
    if let Some(hard) = record.hard {
        frame.extend_from_slice(&encode_raft_hard_state(&hard));
    }
    frame.extend_from_slice(
        &u32::try_from(record.entries.len())
            .map_err(|_| invalid("too many WAL entries"))?
            .to_be_bytes(),
    );
    for entry in &record.entries {
        let size_offset = frame.len();
        frame.extend_from_slice(&[0; 4]);
        let size = encode_raft_log_entry_appending(entry, frame)
            .map_err(|error| invalid(error.to_string()))?;
        let size = u32::try_from(size).map_err(|_| invalid("WAL entry too large"))?;
        frame[size_offset..size_offset + 4].copy_from_slice(&size.to_be_bytes());
        if frame.len() - HEADER > MAX_BODY {
            return Err(invalid("WAL batch exceeds 64 MiB"));
        }
    }
    let size = u32::try_from(frame.len() - HEADER).map_err(|_| invalid("WAL batch too large"))?;
    frame[..4].copy_from_slice(b"BCH1");
    frame[4..8].copy_from_slice(&size.to_be_bytes());
    frame[8..12].copy_from_slice(&(!size).to_be_bytes());
    let header_checksum = crc32(&frame[..12]);
    frame[12..16].copy_from_slice(&header_checksum.to_be_bytes());
    let body_checksum = crc32(&frame[HEADER..]);
    frame.extend_from_slice(&body_checksum.to_be_bytes());
    frame.extend_from_slice(b"END1");
    Ok(())
}

pub(super) fn clear_encode_buffer(buffer: &mut Vec<u8>) {
    if buffer.capacity() > MAX_RETAINED_ENCODE_BUFFER {
        *buffer = Vec::new();
    } else {
        buffer.clear();
    }
}
pub(super) fn body_size(header: &[u8; HEADER]) -> io::Result<usize> {
    let size = u32::from_be_bytes(
        header[4..8]
            .try_into()
            .map_err(|_| invalid("invalid integer width"))?,
    );
    if &header[..4] != b"BCH1"
        || u32::from_be_bytes(
            header[8..12]
                .try_into()
                .map_err(|_| invalid("invalid integer width"))?,
        ) != !size
        || u32::from_be_bytes(
            header[12..]
                .try_into()
                .map_err(|_| invalid("invalid integer width"))?,
        ) != crc32(&header[..12])
    {
        return Err(invalid("corrupt WAL record header"));
    }
    let size = size as usize;
    if !(29..=MAX_BODY).contains(&size) {
        return Err(invalid("invalid WAL record size"));
    }
    Ok(size)
}
pub(super) fn decode(body: &[u8], trailer: [u8; TRAILER]) -> io::Result<Record> {
    if &trailer[4..] != b"END1"
        || u32::from_be_bytes(
            trailer[..4]
                .try_into()
                .map_err(|_| invalid("invalid integer width"))?,
        ) != crc32(body)
    {
        return Err(invalid("corrupt complete WAL record"));
    }
    let mut reader = Reader { bytes: body, at: 0 };
    let operation = reader.u64()?;
    let compact = optional_index(reader.u64()?);
    let truncate = optional_index(reader.u64()?);
    let hard = match reader.take(1)?[0] {
        0 => None,
        1 => Some(decode_raft_hard_state(reader.take(51)?).map_err(|e| invalid(e.to_string()))?),
        _ => return Err(invalid("invalid hard-state flag")),
    };
    let count = reader.u32()? as usize;
    if count > (body.len() - reader.at) / 4 {
        return Err(invalid("invalid WAL entry count"));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let size = reader.u32()? as usize;
        entries
            .push(decode_raft_log_entry(reader.take(size)?).map_err(|e| invalid(e.to_string()))?);
    }
    if reader.at != body.len() {
        return Err(invalid("extra bytes in WAL record"));
    }
    Ok(Record {
        operation,
        compact,
        truncate,
        hard,
        entries,
    })
}
fn optional_index(value: u64) -> Option<LogIndex> {
    (value != u64::MAX).then_some(LogIndex(value))
}
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, size: usize) -> io::Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(size)
            .ok_or_else(|| invalid("WAL offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| invalid("short WAL body"))?;
        self.at = end;
        Ok(bytes)
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| invalid("invalid integer width"))?,
        ))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| invalid("invalid integer width"))?,
        ))
    }
}
