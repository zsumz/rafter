# rafter-crc32

Table-driven CRC-32/IEEE for Rafter's corruption-detection envelopes.

This crate is deliberately tiny and dependency-free. It exists so that the two
format boundaries needing a checksum compute byte-identical values from one
implementation instead of two that can drift: `rafter-codec` checksums peer
message frames, and `rafter-storage` checksums on-disk hard-state, log, and
snapshot envelopes.

`crc32` checksums a single slice. `RunningCrc32` accumulates the same value
across slices supplied one at a time, so a caller that never holds a whole
snapshot payload in memory still produces the checksum of the concatenation.

The polynomial is IEEE 802.3 CRC-32, also known as CRC-32/ISO-HDLC. These
checksums catch torn writes, short files, stale artifacts, misframing, and
accidental media or transport corruption in a non-Byzantine deployment. They
are **not** authentication tags or tamper evidence, and a deployment with an
adversarial storage or network threat model must authenticate below this crate
rather than rely on them.

Checksum coverage is specified per artifact by the crates that do the framing:
`WIRE_FORMAT_V1.md` in `rafter-codec` for peer frames, and
`STORAGE_FORMAT_V1.md` in `rafter-storage` for on-disk envelopes. This crate
fixes only the function, never which bytes a given artifact protects.
