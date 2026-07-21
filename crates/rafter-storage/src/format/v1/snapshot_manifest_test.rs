//! Scenarios for version-1 current-snapshot manifest behavior.

use super::snapshot_manifest::{
    decode_manifest, encode_manifest, RaftSnapshotManifestDecodeError, SnapshotManifest,
    SNAPSHOT_MANIFEST_MAGIC, SNAPSHOT_MANIFEST_VERSION,
};
use crate::format::{finish_checksummed, Writer};

fn manifest() -> SnapshotManifest {
    SnapshotManifest {
        sequence: 7,
        file_name: "snapshot-7-42-8-1.rfsn".to_owned(),
    }
}

#[test]
fn snapshot_manifest_round_trips_through_rfsm() {
    let manifest = manifest();
    let encoded = encode_manifest(&manifest).expect("manifest encodes");

    assert_eq!(decode_manifest(&encoded), Ok(manifest));
}

#[test]
fn snapshot_manifest_rejects_a_path_instead_of_a_plain_file_name() {
    let mut writer = Writer::new();
    writer.bytes(&SNAPSHOT_MANIFEST_MAGIC);
    writer.u8(SNAPSHOT_MANIFEST_VERSION);
    writer.u64(7);
    writer.u16(11);
    writer.bytes(b"../bad.rfsn");
    let encoded = finish_checksummed(writer);

    assert_eq!(
        decode_manifest(&encoded),
        Err(RaftSnapshotManifestDecodeError::InvalidFileName)
    );
}
