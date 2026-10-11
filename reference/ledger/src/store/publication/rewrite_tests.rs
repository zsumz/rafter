//! Independent bytes, bounded write calls, and no retry after failed staging.

use std::io::{self, Write};

use rafter::LogIndex;

use crate::store::{format::Checksum, frame::encode_frame, frame_output::FramePlan};
use crate::{
    AccountId, ClientId, Command, Ledger, LedgerConfig, Mutation, RequestIdentity, Sequence,
    SessionEpoch,
};

use super::write_staged;

fn ledger(accounts: usize) -> Ledger {
    let mut ledger = Ledger::new(LedgerConfig::new(1, accounts).unwrap());
    let client_id = ClientId::new(0);
    let session_epoch = SessionEpoch::new(1).unwrap();
    ledger.apply(Command::OpenSession {
        client_id,
        session_epoch,
    });
    for id in 0..accounts as u64 {
        ledger.apply(Command::Execute {
            request: RequestIdentity {
                client_id,
                session_epoch,
                sequence: Sequence::new(id + 1).unwrap(),
            },
            mutation: Mutation::OpenAccount {
                account_id: AccountId::new(id),
            },
        });
    }
    ledger
}

#[derive(Default)]
struct Counting {
    calls: usize,
    max: usize,
    len: u64,
    checksum: Option<Checksum>,
}
impl Write for Counting {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        self.max = self.max.max(bytes.len());
        self.len += bytes.len() as u64;
        self.checksum
            .get_or_insert_with(Checksum::new)
            .update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_two_mebibyte_journal_uses_bounded_staging_without_an_encoded_image() {
    let ledger = ledger(131_072);
    let frame = FramePlan::new(&ledger, LogIndex(131_073)).unwrap();
    let config = LedgerConfig::new(1, 131_072).unwrap();
    let sink = write_staged(Counting::default(), config, &frame).unwrap();
    assert_eq!(sink.len, 2_097_267);
    assert_eq!(frame.len() + 21, sink.len);
    // Independent Python zlib CRC32 of v1 header/config and 131072 zero-balance
    // accounts with the last OpenAccount cached at sequence 131072, floor131073.
    assert_eq!(sink.checksum.unwrap().value(), 0x5a1b_cc02);
    assert!(sink.max <= 16 * 1024);
    assert!((129..=130).contains(&sink.calls));
}

#[test]
fn streamed_framing_preserves_independent_version_one_journal_bytes() {
    let ledger = ledger(1);
    let frame = FramePlan::new(&ledger, LogIndex(2)).unwrap();
    let bytes = write_staged(Vec::new(), LedgerConfig::new(1, 1).unwrap(), &frame).unwrap();
    let hex = "524c444701000000010000000000000001078106be524c4247010000005066bbe74ba9961dab0100000000000000020000000000000000000000000000000000000001000000000000000000000000000000000000000100000000000000000000000101000000000000000101000000000000000001524c434d01e59729a9cd6b6ec2";
    let expected: Vec<u8> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    assert_eq!(bytes, expected);
    assert_eq!(encode_frame(&ledger, LogIndex(2)).unwrap(), expected[21..]);
}

struct Partial {
    bytes: Vec<u8>,
    interrupted: bool,
}
impl Write for Partial {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !std::mem::replace(&mut self.interrupted, true) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let n = bytes.len().min(3);
        self.bytes.extend_from_slice(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn frame_checksum_accounts_only_for_bytes_accepted_by_a_partial_sink() {
    let ledger = ledger(2);
    let plan = FramePlan::new(&ledger, LogIndex(3)).unwrap();
    let mut sink = Partial {
        bytes: Vec::new(),
        interrupted: false,
    };
    assert!(plan.write_to(&mut sink).is_ok());
    assert_eq!(sink.bytes, encode_frame(&ledger, LogIndex(3)).unwrap());
}

struct Failure {
    calls: usize,
}
impl Write for Failure {
    fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        Err(io::ErrorKind::BrokenPipe.into())
    }
    fn flush(&mut self) -> io::Result<()> {
        panic!("no flush after write failure")
    }
}

#[test]
fn failed_encoding_and_final_buffer_drain_do_not_retry_on_drop() {
    for count in [1, 2048] {
        let ledger = ledger(count);
        let frame = FramePlan::new(&ledger, LogIndex(count as u64 + 1)).unwrap();
        let mut sink = Failure { calls: 0 };
        assert!(write_staged(&mut sink, LedgerConfig::new(1, count).unwrap(), &frame).is_err());
        assert_eq!(sink.calls, 1);
    }
}
