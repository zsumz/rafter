//! Receive-limit arithmetic for transports.
//!
//! The codec imposes no receive limit; a transport must enforce one before
//! allocating a frame. This module is that recipe as code, so a transport can
//! size against a number instead of re-deriving one from prose.
//!
//! Every constant here is a claim about a frame a conforming v1 peer can emit,
//! and every one is pinned by a test that encodes the frame and measures it.
//! Where a claim rests on something outside this crate, the dependency is
//! named — see [`MAX_INSTALL_SNAPSHOT_CHUNK_FRAME_BYTES`] and
//! [`MAX_CONFIGURATION_APPEND_FRAME_BYTES`], whose bounds live in `rafter`.

/// Bytes an `AppendEntries` frame adds around one maximum-size application
/// entry, over and above the configured append-entries budget.
///
/// A leader admits an application proposal when
/// `LogEntry::replication_bytes` fits the budget, so the largest application
/// payload is `budget - 64`. Framing that entry costs 75 bytes (frame header,
/// entry header, blob length, trailing fields and checksum), so the frame is
/// `budget + 11`.
///
/// That the frame lands *under* `budget + 75` is not an accident of framing:
/// `replication_bytes` charges an application entry 64 bytes of overhead
/// against a wire cost of 13. See the crate's `WIRE_FORMAT_V1.md` section on
/// receive limits for why that over-charge is what keeps the append bound
/// correct, and `tests::receive_limits` for the test that holds it.
const APPLICATION_APPEND_FRAME_OVERHEAD_BYTES: usize = 11;

/// The largest `AppendEntries` frame carrying one joint configuration entry.
///
/// Configuration entries are exempt from the append-entries budget in both
/// directions: a leader checks only application payloads against it, and batch
/// assembly always includes the first entry whatever its size. So this bound
/// does not scale with the budget — it is set by how large a membership can
/// be, and `rafter`'s `MembershipSet::new` imposes no size limit. The only
/// ceiling is this format's `u16` member counts: 65,535 voters and 65,535
/// learners in each of the four halves of a joint configuration.
pub const MAX_CONFIGURATION_APPEND_FRAME_BYTES: usize = 2_097_207;

/// The largest `InstallSnapshotChunk` frame.
///
/// Chunk metadata embeds a committed configuration, which embeds a joint
/// membership, so this is dominated by the same unbounded-membership term as
/// [`MAX_CONFIGURATION_APPEND_FRAME_BYTES`] rather than by the chunk payload.
///
/// The payload term is the one promise this crate cannot check from here: it
/// assumes a sender that caps chunks at 64 KiB, which is `rafter`'s private
/// `INSTALL_SNAPSHOT_CHUNK_BYTES` in `node/replication/snapshot/send.rs`. The
/// codec accepts any chunk length, so a transport that must tolerate a peer
/// built from different source should size against its own maximum instead of
/// this constant.
pub const MAX_INSTALL_SNAPSHOT_CHUNK_FRAME_BYTES: usize = 2_163_036;

/// The largest frame whose size does not scale with the append-entries
/// budget, folded at compile time so the two membership-driven terms stay
/// comparable as either one moves.
const MAX_MEMBERSHIP_DRIVEN_FRAME_BYTES: usize =
    if MAX_CONFIGURATION_APPEND_FRAME_BYTES > MAX_INSTALL_SNAPSHOT_CHUNK_FRAME_BYTES {
        MAX_CONFIGURATION_APPEND_FRAME_BYTES
    } else {
        MAX_INSTALL_SNAPSHOT_CHUNK_FRAME_BYTES
    };

/// The largest peer frame a conforming v1 leader can emit when the cluster's
/// append-entries budget is `max_append_entries_bytes`.
///
/// This is the number a transport's receive limit must accommodate. It covers
/// every frame kind in the v1 tag registry, not only application entries:
/// `tests::receive_limits` enumerates the registry and fails if any frame kind
/// exceeds this, and stops compiling when a new frame kind is added.
///
/// Pass `NodeConfig::max_append_entries_bytes()`. At `rafter`'s default budget
/// of 512 KiB this returns 2,163,036 bytes — the append term (524,299) is not
/// the maximum; a configuration or snapshot frame is.
#[must_use]
pub const fn max_receive_frame_bytes(max_append_entries_bytes: usize) -> usize {
    let application =
        max_append_entries_bytes.saturating_add(APPLICATION_APPEND_FRAME_OVERHEAD_BYTES);
    if application > MAX_MEMBERSHIP_DRIVEN_FRAME_BYTES {
        application
    } else {
        MAX_MEMBERSHIP_DRIVEN_FRAME_BYTES
    }
}
