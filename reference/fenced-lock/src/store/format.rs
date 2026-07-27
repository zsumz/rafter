//! The slot format's byte-level vocabulary.
//!
//! Everything a reader has to agree with the bytes about before anything can be
//! decoded: which two files a store owns, where each header field sits, the two
//! values byte zero takes, and the checksum the whole format is sealed with.
//! The `# Format` section of the [module documentation](super) is this module's
//! specification, and the section after it is why byte zero carries two
//! meanings.

use std::fmt;

/// Fixed length of a slot header, in bytes.
pub const SLOT_HEADER_LEN: usize = 37;
/// Fixed length of a slot's trailing commit checksum, in bytes.
pub const SLOT_TRAILER_LEN: usize = 4;

/// Offset of the header's own checksum within the header.
pub(super) const HEADER_CHECKSUM_OFFSET: usize = SLOT_HEADER_LEN - 4;
/// Offset of the header's applied-index field.
pub(super) const HEADER_APPLIED_INDEX_OFFSET: usize = 13;

pub(super) const SLOT_MAGIC: [u8; 4] = *b"RFLK";

/// First byte of a slot whose image is sealed: the magic's leading byte.
///
/// See `UNSEALED_MARK` for what the same byte says while an image is being
/// written, and the module's recovery section for why this byte is half of the
/// skip rule rather than the whole of it.
pub(super) const SEALED_MARK: u8 = SLOT_MAGIC[0];

/// First byte of a slot whose image is being written.
///
/// A publication writes this value before any other byte of the image and
/// replaces it with `SEALED_MARK` only after every other byte is durable, so
/// every interrupted publication leaves it behind. The converse does not hold,
/// and assuming it was the defect this store was corrected for: a slot carrying
/// this value may equally hold a sealed image whose mark byte rotted to zero.
/// Skipping needs [`SlotDamage::is_publication_residue`], which asks a second
/// question beside this byte, not this byte alone.
pub(super) const UNSEALED_MARK: u8 = 0x00;

/// Content of a slot file the moment it is created.
///
/// One unsealed mark and nothing else. A created slot is therefore not an empty
/// file, which is what lets a slot file of zero bytes be damage rather than the
/// ordinary state of a store that has never committed.
pub(super) const CREATION_MARK: [u8; 1] = [UNSEALED_MARK];

/// Version byte of every slot this build writes.
pub(super) const SLOT_FORMAT_VERSION: u8 = 1;

/// Stable names of the two slots inside the store's directory.
const SLOT_FILE_NAMES: [&str; 2] = ["lock-state.0", "lock-state.1"];

/// Which of the two slots a value names.
///
/// There are exactly two, forever: this store's atomicity argument is that the
/// live image is never the one being written, and one spare slot is all that
/// takes. Which of the two a later opener may skip is decided by the
/// publication mark rather than by that argument.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SlotIndex {
    /// `lock-state.0`.
    Zero,
    /// `lock-state.1`.
    One,
}

impl SlotIndex {
    /// Returns the slot a publication writes when this one is authoritative.
    #[must_use]
    pub const fn other(self) -> Self {
        match self {
            Self::Zero => Self::One,
            Self::One => Self::Zero,
        }
    }

    /// Returns the slot's stable file name.
    #[must_use]
    pub const fn file_name(self) -> &'static str {
        SLOT_FILE_NAMES[self.position()]
    }

    pub(super) const fn position(self) -> usize {
        match self {
            Self::Zero => 0,
            Self::One => 1,
        }
    }
}

impl fmt::Display for SlotIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.file_name())
    }
}

pub(super) const CRC32_POLYNOMIAL: u32 = 0xEDB8_8320;

/// Half-byte residue table for [`crc32`], folded at compile time.
const CRC32_NIBBLES: [u32; 16] = {
    let mut table = [0_u32; 16];
    let mut index = 0_usize;
    let mut nibble = 0_u32;
    while index < 16 {
        let mut residue = nibble;
        let mut bit = 0;
        while bit < 4 {
            residue = if residue & 1 == 1 {
                (residue >> 1) ^ CRC32_POLYNOMIAL
            } else {
                residue >> 1
            };
            bit += 1;
        }
        table[index] = residue;
        index += 1;
        nibble += 1;
    }
    table
};

/// CRC-32/IEEE over `bytes`.
///
/// Folded a nibble at a time through a sixteen-entry table that is built at
/// compile time from the polynomial, so the constant a reviewer has to trust is
/// the polynomial itself rather than 256 pre-computed words.
/// `checksums_match_the_published_vector` pins the result to the standard check
/// value, and `checksums_agree_with_an_independently_written_implementation`
/// compares it against a bit-at-a-time version written the other way round.
pub(super) fn crc32(bytes: &[u8]) -> u32 {
    let mut state = 0xFFFF_FFFF_u32;
    for byte in bytes {
        state ^= u32::from(*byte);
        state = (state >> 4) ^ CRC32_NIBBLES[(state & 0x0F) as usize];
        state = (state >> 4) ^ CRC32_NIBBLES[(state & 0x0F) as usize];
    }
    !state
}

pub(super) fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("callers pass four bytes"))
}

pub(super) fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("callers pass eight bytes"))
}

pub(super) fn as_u64(value: usize) -> u64 {
    u64::try_from(value).expect("slot sizes fit a u64")
}
