//! Allocation-counted rejection of malformed WAL counts and checkpoint lengths.
//!
//! Counts stay modest so this gate also runs safely against the prior eager
//! reservation. Measurements cover Rust allocation requests during open, not
//! RSS, valid log growth, or physical I/O latency.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    fs, io,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use rafter::{LogIndex, Term};
use rafter_storage::{
    crc32, decode_raft_log_entry, durable_batch::WalRaftNodeStores, encode_raft_log_entry,
    PersistedRaftLogEntry,
};

#[path = "check-wal-recovery-memory/checkpoint.rs"]
mod checkpoint;

struct CountingAllocator;
static COUNTING: AtomicBool = AtomicBool::new(false);
static LARGEST: AtomicUsize = AtomicUsize::new(0);
static REQUESTED: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn record(pointer: *mut u8, size: usize) {
    if !pointer.is_null() && COUNTING.load(Ordering::Relaxed) {
        LARGEST.fetch_max(size, Ordering::Relaxed);
        REQUESTED.fetch_add(size, Ordering::Relaxed);
    }
}

// SAFETY: Every operation delegates unchanged to System. Observations use only
// atomics, allocate nothing, and cannot unwind inside allocator callbacks.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        record(pointer, layout.size());
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        record(pointer, layout.size());
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(pointer, layout, size) };
        record(replacement, size);
        replacement
    }
}

struct Measurement;
impl Measurement {
    fn start() -> Self {
        LARGEST.store(0, Ordering::Relaxed);
        REQUESTED.store(0, Ordering::Relaxed);
        COUNTING.store(true, Ordering::Relaxed);
        Self
    }
}
impl Drop for Measurement {
    fn drop(&mut self) {
        COUNTING.store(false, Ordering::Relaxed);
    }
}

fn main() {
    for count in [4096, 65_536] {
        for valid_prefix in [0, 8] {
            check(count, valid_prefix);
        }
    }
    checkpoint::check();
}

fn check(count: u32, valid_prefix: u32) {
    let directory = Scratch::new(count, valid_prefix);
    drop(WalRaftNodeStores::open(&directory.0).expect("initialize independent WAL"));
    let path = directory.0.join("hard-state");
    let (bytes, body_size) = malformed_wal(count, valid_prefix);
    fs::write(&path, &bytes).expect("write malformed complete batch");

    let measurement = Measurement::start();
    let result = WalRaftNodeStores::open(&directory.0);
    drop(measurement);
    let largest = LARGEST.load(Ordering::Relaxed);
    let requested = REQUESTED.load(Ordering::Relaxed);
    let error = result.expect_err("a complete batch with an empty entry must fail");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        decode_raft_log_entry(&[])
            .expect_err("empty envelope")
            .to_string()
    );
    assert_eq!(fs::read(&path).expect("unchanged WAL"), bytes);

    // Allow body buffering, decoded prefix growth, paths and error messages.
    // Both bounds reject the old count * size_of::<PersistedRaftLogEntry>()
    // reservation without depending on that type's host-specific layout.
    let largest_limit = body_size * 2;
    let requested_limit = body_size * 4 + 64 * 1024;
    println!(
        "WAL count={count}, valid_prefix={valid_prefix}, body={body_size}, \
         largest_request={largest}/{largest_limit}, total_requested={requested}/{requested_limit}"
    );
    assert!(
        largest <= largest_limit,
        "malformed count amplified one allocation"
    );
    assert!(
        requested <= requested_limit,
        "malformed count amplified cumulative allocations"
    );
}

fn malformed_wal(count: u32, valid_prefix: u32) -> (Vec<u8>, usize) {
    assert!(valid_prefix < count);
    // Independent RFWB/BCH1 framing with valid outer checksums, followed by
    // zero-sized nested entries. Count admission alone cannot validate them.
    let mut body = 1_u64.to_be_bytes().to_vec();
    body.extend_from_slice(&u64::MAX.to_be_bytes());
    body.extend_from_slice(&u64::MAX.to_be_bytes());
    body.push(0);
    body.extend_from_slice(&count.to_be_bytes());
    for index in 1..=valid_prefix {
        let entry = encode_raft_log_entry(&PersistedRaftLogEntry::noop(
            LogIndex(u64::from(index)),
            Term(1),
        ))
        .expect("valid prefix envelope");
        body.extend_from_slice(
            &u32::try_from(entry.len())
                .expect("small entry")
                .to_be_bytes(),
        );
        body.extend_from_slice(&entry);
    }
    body.resize(
        body.len() + usize::try_from(count - valid_prefix).expect("small count") * 4,
        0,
    );
    let size = u32::try_from(body.len()).expect("small record");
    let mut header = b"BCH1".to_vec();
    header.extend_from_slice(&size.to_be_bytes());
    header.extend_from_slice(&(!size).to_be_bytes());
    header.extend_from_slice(&crc32(&header).to_be_bytes());
    let mut bytes = b"RFWB\0\0\0\x01".to_vec();
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&crc32(&body).to_be_bytes());
    bytes.extend_from_slice(b"END1");
    (bytes, body.len())
}

struct Scratch(PathBuf);
impl Scratch {
    fn new(count: u32, valid_prefix: u32) -> Self {
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rafter-wal-memory-{}-{time}-{count}-{valid_prefix}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("new fixture directory");
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
