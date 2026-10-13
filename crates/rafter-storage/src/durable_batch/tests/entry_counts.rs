//! Malformed entry counts keep complete WAL batches authoritative and untouched.
use super::*;
use crate::{
    crc32, decode_raft_log_entry, durable_batch::reclamation as wal, encode_raft_log_entry,
};
use std::io;

// Independent outer RFWB record; nested envelopes retain their own checksums.
fn frame(operation: u64, count: u32, entries: &[u8]) -> Vec<u8> {
    let mut body = operation.to_be_bytes().to_vec();
    body.extend_from_slice(&u64::MAX.to_be_bytes());
    body.extend_from_slice(&u64::MAX.to_be_bytes());
    body.push(0);
    body.extend_from_slice(&count.to_be_bytes());
    body.extend_from_slice(entries);
    let size = u32::try_from(body.len()).unwrap();
    let mut bytes = b"BCH1".to_vec();
    bytes.extend_from_slice(&size.to_be_bytes());
    bytes.extend_from_slice(&(!size).to_be_bytes());
    bytes.extend_from_slice(&crc32(&bytes).to_be_bytes());
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&crc32(&body).to_be_bytes());
    bytes.extend_from_slice(b"END1");
    bytes
}

fn framed_entry(entry: &PersistedRaftLogEntry) -> Vec<u8> {
    let encoded = encode_raft_log_entry(entry).unwrap();
    let mut bytes = u32::try_from(encoded.len()).unwrap().to_be_bytes().to_vec();
    bytes.extend_from_slice(&encoded);
    bytes
}

#[test]
fn rejected_counts_preserve_exact_diagnostics_and_never_repair_complete_records() {
    for reclaimed in [false, true] {
        let dir = Directory::new();
        let (h, mut l, mut s) = dir.open();
        publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"two")], 2, None).unwrap();
        if reclaimed {
            s.write_snapshot(snapshot(1)).unwrap();
            l.compact_prefix_through(LogIndex(1)).unwrap();
        }
        drop((h, l, s));
        let manifest = wal::read_manifest(&dir.0).unwrap();
        let (path, operation) = manifest.map_or_else(
            || (dir.0.join("hard-state"), 2),
            |m| (wal::segment_path(&dir.0, m.generation), m.operation + 1),
        );
        let original = fs::read(&path).unwrap();
        let empty_entry_error = decode_raft_log_entry(&[]).unwrap_err().to_string();
        let mut valid_then_empty = framed_entry(&entry(3, b"three"));
        valid_then_empty.resize(valid_then_empty.len() + 4096 * 4, 0);
        let mut invalid_size = vec![0; 4096 * 4];
        invalid_size[..4].copy_from_slice(&u32::MAX.to_be_bytes());
        let cases = [
            (4096, vec![0; 4096 * 4], empty_entry_error.clone()),
            (4097, valid_then_empty, empty_entry_error),
            (4096, invalid_size, "short WAL body".to_owned()),
            (u32::MAX, vec![0; 4], "invalid WAL entry count".to_owned()),
            (0, vec![0; 4], "extra bytes in WAL record".to_owned()),
        ];
        for (count, entries, expected) in cases {
            let mut bytes = original.clone();
            bytes.extend_from_slice(&frame(operation, count, &entries));
            fs::write(&path, &bytes).unwrap();
            let error = WalRaftNodeStores::open(&dir.0).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(error.to_string(), expected);
            assert_eq!(fs::read(&path).unwrap(), bytes);
            assert_eq!(wal::read_manifest(&dir.0).unwrap(), manifest);
        }
        fs::write(&path, original).unwrap();
        let (h, l, s) = dir.open();
        assert_eq!(h.current(), hard(2));
        assert_eq!(
            l.replay_entries(),
            if reclaimed {
                vec![entry(2, b"two")]
            } else {
                vec![entry(1, b"one"), entry(2, b"two")]
            }
        );
        drop((h, l, s));
    }
}

#[test]
fn outer_checksum_still_precedes_entry_count_and_nested_entry_errors() {
    let dir = Directory::new();
    drop(dir.open());
    for count in [0, 4096, u32::MAX] {
        let mut record = frame(1, count, &[0; 32]);
        let checksum = record.len() - 8;
        record[checksum] ^= 1;
        let mut bytes = codec::MAGIC.to_vec();
        bytes.extend_from_slice(&record);
        fs::write(dir.0.join("hard-state"), &bytes).unwrap();
        let error = WalRaftNodeStores::open(&dir.0).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "corrupt complete WAL record");
        assert_eq!(fs::read(dir.0.join("hard-state")).unwrap(), bytes);
    }
}

#[test]
fn large_valid_entry_counts_recover_and_match_the_existing_encoder() {
    let entries: Vec<_> = (1..=4096)
        .map(|i| PersistedRaftLogEntry::noop(LogIndex(i), Term(1)))
        .collect();
    let nested: Vec<_> = entries.iter().flat_map(framed_entry).collect();
    let forged = frame(1, 4096, &nested);
    let record = codec::Record {
        operation: 1,
        compact: None,
        truncate: None,
        hard: None,
        entries: entries.clone(),
    };
    let mut encoded = Vec::new();
    codec::encode_reusing(&record, &mut encoded).unwrap();
    assert_eq!(forged, encoded);
    let dir = Directory::new();
    drop(dir.open());
    let mut bytes = codec::MAGIC.to_vec();
    bytes.extend_from_slice(&forged);
    // An empty record is also valid and must not consume an entry index.
    bytes.extend_from_slice(&frame(2, 0, &[]));
    fs::write(dir.0.join("hard-state"), &bytes).unwrap();
    for _ in 0..2 {
        let (_, l, _) = dir.open();
        assert_eq!(l.next_index(), LogIndex(4097));
        assert_eq!(l.replay_entries(), entries);
        assert_eq!(fs::read(dir.0.join("hard-state")).unwrap(), bytes);
    }
}
