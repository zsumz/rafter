//! The journal format's byte-level vocabulary.
//!
//! Everything a reader has to agree with the bytes about before anything can be
//! decoded: the two file names, the three magics, where each record's fields
//! sit, the two values a frame's first byte takes, and the checksum every
//! record is sealed with. The `# Format` section of the
//! [module documentation](super) is this module's specification, and the
//! section after it is why that first byte carries two meanings.

/// Fixed length of the journal header, in bytes.
pub const HEADER_LEN: usize = 21;
/// Fixed length of a transaction begin record, in bytes.
pub const BEGIN_LEN: usize = 17;
/// Fixed length of a transaction commit record, in bytes.
pub const COMMIT_LEN: usize = 13;

pub(super) const JOURNAL_MAGIC: [u8; 4] = *b"RLDG";
pub(super) const BEGIN_MAGIC: [u8; 4] = *b"RLBG";
pub(super) const COMMIT_MAGIC: [u8; 4] = *b"RLCM";

/// First byte of a frame whose transaction is sealed: the begin magic's leading
/// byte.
pub(super) const SEALED_FRAME_MARK: u8 = BEGIN_MAGIC[0];

/// First byte of a frame that is still being appended.
///
/// An append writes this value before any other byte of the frame and replaces
/// it with `SEALED_FRAME_MARK` only after every other byte is durable, so
/// every interrupted append leaves it behind. The converse does not hold, and
/// assuming it was the defect this store was corrected for: a tail carrying
/// this value may equally be a committed frame whose mark byte rotted to zero.
/// Truncating needs [`TornTail::is_interrupted_append`], which asks a second
/// question beside this byte, not this byte alone.
///
/// It is zero because that is also what a filesystem leaves when a crash
/// extended a file without persisting its data. The coincidence is convenient
/// and is not itself a rule — the sentence that used to finish this paragraph,
/// that such residue "reads as exactly what it is", is the one the module doc
/// retracts, because it made zeros landing over a *committed* frame read as
/// residue too. What the value earns is a place in `verify_identity`'s first
/// test. Everything past that is decided by `read_frame`'s ordering and by
/// [`TornTail::is_truncatable_residue`].
pub(super) const UNSEALED_FRAME_MARK: u8 = 0x00;

/// Version byte of every record this build writes.
pub(super) const JOURNAL_FORMAT_VERSION: u8 = 1;

/// Stable name of the journal inside the store's directory.
pub(super) const JOURNAL_FILE_NAME: &str = "ledger.journal";

/// Stable name of the file a rewrite or a creation stages beside the journal.
///
/// There is no process ID in it. An abandoned staging file is by definition the
/// work of a process that died, so a name only its author could recognize is a
/// name nobody ever removes; exclusivity comes from the directory ownership
/// discipline instead, and the sweep at open removes whatever it finds.
pub(super) const STAGED_FILE_NAME: &str = "ledger.journal.tmp";

pub(super) const CRC32_POLYNOMIAL: u32 = 0xEDB8_8320;

/// CRC-32/IEEE over `bytes`.
///
/// This is the bitwise reference form rather than a table-driven one. The store
/// commits bounded images, so the byte-scanning cost is irrelevant next to
/// being auditable at a glance, and `checksums_match_the_published_vector`
/// pins it to the standard check value.
pub(super) fn crc32(bytes: &[u8]) -> u32 {
    let mut state = 0xFFFF_FFFF_u32;
    for byte in bytes {
        state ^= u32::from(*byte);
        for _ in 0..8 {
            state = if state & 1 == 1 {
                (state >> 1) ^ CRC32_POLYNOMIAL
            } else {
                state >> 1
            };
        }
    }
    !state
}

pub(super) fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("callers pass four bytes"))
}

pub(super) fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("callers pass eight bytes"))
}
