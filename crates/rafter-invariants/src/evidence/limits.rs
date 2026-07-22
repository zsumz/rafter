//! Resource ceilings for serialized evidence crossing the trust boundary.

/// Largest single artifact accepted or emitted by the invariant contract.
pub(crate) const MAX_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;

/// Largest exact verifier archive, including its manifest.
pub(crate) const MAX_VERIFIER_ARCHIVE_BYTES: usize = 256 * 1024 * 1024;

/// Largest exact verifier archive inventory, including its manifest.
pub(crate) const MAX_VERIFIER_ARCHIVE_FILES: usize = 512;

/// Largest number of artifact references accepted in one result bundle.
///
/// Simulator checks bind their shared model artifacts and their individual
/// detector transcript into every check receipt. The reviewed PR inventory
/// currently needs 484 references, so retain one bounded power-of-two ceiling
/// above that producer shape.
pub(crate) const MAX_ARTIFACT_REFS_PER_BUNDLE: usize = 512;

/// Largest number of distinct artifacts representable in one verdict report.
pub(crate) const MAX_VERDICT_ARTIFACT_REFS: usize = 4_096;

/// Largest repository-relative path represented in a receipt.
pub(crate) const MAX_EVIDENCE_PATH_BYTES: usize = 4_096;

/// Largest artifact-kind identifier represented in a receipt.
pub(crate) const MAX_ARTIFACT_KIND_BYTES: usize = 128;
