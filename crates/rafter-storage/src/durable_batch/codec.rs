//! Checksummed atomic records containing existing RFLE/RFHS envelopes.
use crate::{
    crc32, decode_raft_hard_state, decode_raft_log_entry, encode_raft_hard_state,
    encode_raft_log_entry, PersistedRaftLogEntry, RaftHardState,
};
use rafter::LogIndex;
use std::io;

pub(super) const MAGIC: &[u8; 8] = b"RFWB\0\0\0\x01";
pub(super) const HEADER: usize = 16;
pub(super) const TRAILER: usize = 8;
pub(super) const MAX_BODY: usize = 64 * 1024 * 1024;

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
pub(super) fn encode(record: &Record) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    body.extend_from_slice(&record.operation.to_be_bytes());
    body.extend_from_slice(&record.compact.map_or(u64::MAX, |i| i.0).to_be_bytes());
    body.extend_from_slice(&record.truncate.map_or(u64::MAX, |i| i.0).to_be_bytes());
    body.push(u8::from(record.hard.is_some()));
    if let Some(hard) = record.hard {
        body.extend_from_slice(&encode_raft_hard_state(&hard));
    }
    body.extend_from_slice(
        &u32::try_from(record.entries.len())
            .map_err(|_| invalid("too many WAL entries"))?
            .to_be_bytes(),
    );
    for entry in &record.entries {
        let bytes = encode_raft_log_entry(entry).map_err(|e| invalid(e.to_string()))?;
        body.extend_from_slice(
            &u32::try_from(bytes.len())
                .map_err(|_| invalid("WAL entry too large"))?
                .to_be_bytes(),
        );
        body.extend_from_slice(&bytes);
        if body.len() > MAX_BODY {
            return Err(invalid("WAL batch exceeds 64 MiB"));
        }
    }
    let size = u32::try_from(body.len()).map_err(|_| invalid("WAL batch too large"))?;
    let mut frame = Vec::with_capacity(HEADER + body.len() + TRAILER);
    frame.extend_from_slice(b"BCH1");
    frame.extend_from_slice(&size.to_be_bytes());
    frame.extend_from_slice(&(!size).to_be_bytes());
    frame.extend_from_slice(&crc32(&frame).to_be_bytes());
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&crc32(&body).to_be_bytes());
    frame.extend_from_slice(b"END1");
    Ok(frame)
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
