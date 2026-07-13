//! Nightly-scale end-to-end synthetic snapshot streaming.

use super::support::*;
use super::*;

#[test]
#[ignore = "nightly-scale: streams a 4.5 GiB synthetic payload end to end"]
fn multi_gigabyte_snapshot_transfer_stays_bounded() {
    let total_payload_len = 4_831_850_496_u64;
    assert!(total_payload_len > u64::from(u32::MAX));
    assert_ne!(total_payload_len % SNAPSHOT_CHUNK_BYTES, 0);
    assert_full_transfer_stays_bounded(total_payload_len);
}
