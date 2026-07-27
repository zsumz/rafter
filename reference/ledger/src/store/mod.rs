//! The ledger's durable transactional application backend.
//!
//! [`LedgerStore`] holds every fact the contract enumerates — account
//! balances, active sessions, the deduplication cache with its exact cached
//! mutation and cached result, the external deposit total, and the applied Raft
//! index — and moves all of them across one atomic, durable commit point. A
//! reader auditing this store should be able to answer, from this file alone,
//! what the commit point is and what a crash on either side of it leaves.
//!
//! # Why a journal of whole images
//!
//! The contract requires one transaction to bind four different kinds of fact
//! together: account mutations, the session and deduplication mutation, the
//! cached command result, and the applied Raft index. A write-ahead journal
//! makes that binding a single record. The transaction is committed exactly
//! when its commit record is present and both of its checksums verify, so the
//! four facts are reachable together or not at all, and there is one byte
//! offset in the file where that changes.
//!
//! Each frame carries the whole application state rather than a delta. The
//! ledger's durable state is bounded by [`LedgerConfig`], so a whole image is
//! affordable, and it buys the property that every committed frame is
//! independently complete: recovery decodes the newest committed frame and
//! stops. A delta journal would need a base image, a checkpoint protocol, and
//! a rule for ordering the two on recovery — three mechanisms to audit instead
//! of one, and three chances for a torn tail to strand a delta whose base is
//! gone.
//!
//! The image is exactly the adapter's application snapshot frame. The contract
//! enumerates the same facts for the durable transaction and for the
//! application snapshot, so encoding them twice would be two chances to forget
//! the deduplication cache. Recovery decodes an image through
//! [`Ledger::from_snapshot`], which is the model's own validating restore path,
//! so a frame whose checksums verify still cannot produce a ledger that
//! violates a resource or supply invariant.
//!
//! Renaming is used for exactly one job: replacing the journal wholesale, when
//! the new content does not extend the old. Snapshot install and compaction are
//! both that job, so they share one mechanism rather than growing a second one.
//!
//! # Format
//!
//! The store owns one directory containing the journal `ledger.journal`.
//! Anything that does not extend the journal — a rewrite, and the creation of
//! the journal itself — stages `ledger.journal.tmp` beside it and renames it
//! into place. No other file is durable state, and a staging file present at
//! open is removed.
//!
//! The sweep removes exactly that one name and nothing else. An earlier shape
//! of it removed anything beside the journal whose name began with the
//! journal's name and a dot, on the reasoning that a staging file is always
//! somebody's abandoned work and the widest rule leaks the least. That
//! reasoning had the direction of its own proof backwards, in the same way the
//! tail classifier did: every staging file this store writes matches the
//! prefix, and matching the prefix was then taken as proof that a file was one.
//! It is not. When the journal will not open, the process tells an operator to
//! run a repair; the obvious first move is to copy the journal aside, and the
//! obvious name for the copy begins with the journal's name and a dot. Opening
//! the store deleted the backup the store's own instructions invited, and
//! reported it as one boolean.
//!
//! Leaking is the smaller failure. A file this store cannot have written is
//! somebody's evidence, and a name only its author could recognize is a name
//! only its author should remove.
//!
//! Removing this store's own staging file is safe because there is no other
//! writer. Ownership of the directory is assumed rather than enforced here —
//! two live stores over one directory would interleave appends and corrupt each
//! other, and nothing in this file stops them — but the composition supplies
//! it, and the sweep leans on that rather than on a name.
//!
//! The process composition supplies the missing exclusion without changing this
//! file. A replica process takes `rafter-storage`'s operating-system lock over
//! its Raft store directory *before* it opens this journal, and holds it for
//! the process's life, so a second process is refused at the sibling directory
//! and never reaches this one. That is an ordering discipline stated in
//! `CONTRACT.md` rather than a lock this store holds, and the difference
//! matters to anyone embedding [`LedgerStore`] on its own: alone, it defends
//! nothing. It is also what the staging sweep above rests on: at the moment the
//! sweep runs there is exactly one live writer for this directory, so every
//! staging file it finds was abandoned by an incarnation that is gone.
//!
//! # Creating the journal
//!
//! Creation is a rename, for the same reason a rewrite is. Writing a header
//! into a freshly created file leaves a window between the two in which the
//! directory holds a journal too short to be one — and a later open cannot
//! recover from that, because the file exists, so creation never runs again and
//! the header is never written. The directory would be bricked by a crash in a
//! two-statement function.
//!
//! So creation stages the header in `ledger.journal.tmp`, syncs it, renames it
//! to `ledger.journal`, and syncs the directory. The journal therefore appears
//! with its header or does not appear at all, an interrupted creation leaves
//! only a staging file the next open sweeps, and the next open then creates the
//! journal properly.
//!
//! A journal shorter than its header is consequently not a state this store can
//! produce, and [`LedgerStoreError::HeaderTruncated`] stays a refusal rather
//! than becoming a reason to create the journal again. Re-creating it would be
//! the same mistake this file's recovery rules exist to avoid, in its plainest
//! form: a file that has been truncated to nothing is unreadable, not absent,
//! and nothing about it says whether it once held committed transactions.
//! Opening a fresh empty ledger over it would be a silent history deletion with
//! no corrupted byte anywhere. The crash window is closed by the rename; the
//! refusal is what covers everything the rename does not explain.
//!
//! Unless a record says otherwise:
//!
//! - integers are unsigned and big-endian;
//! - records are packed with no alignment or padding;
//! - a magic or version other than the one named here is rejected, and never
//!   quietly discarded as a tail;
//! - each record's trailing `crc32` is CRC-32/IEEE over every preceding byte of
//!   that record, and checksum coverage ends immediately before `crc32`; and
//! - CRC-32 is an accidental-corruption check, not an authentication tag.
//!
//! ## Journal header (`RLDG`)
//!
//! The header has a fixed size of 21 bytes and appears once, at offset zero.
//!
//! ```text
//! magic          [4]   "RLDG"
//! version        u8    1
//! max_clients    u32
//! max_accounts   u64
//! crc32          u32
//! ```
//!
//! `max_clients` and `max_accounts` are the [`LedgerConfig`] the journal was
//! created under. Opening a journal under different bounds is rejected rather
//! than reinterpreted, because the bounds decide which images are valid.
//!
//! ## Transaction frame
//!
//! Every byte after the header belongs to a transaction frame. One frame is a
//! begin record, then its image, then a commit record.
//!
//! ### Begin record (`RLBG`)
//!
//! The begin record has a fixed size of 17 bytes.
//!
//! ```text
//! magic          [4]   "RLBG", with byte 0 held at 0x00 until the frame seals
//! version        u8    1
//! image_len      u32
//! image_crc32    u32
//! crc32          u32
//! ```
//!
//! The magic's first byte doubles as the **append mark**. An append writes it
//! as `0x00` and promotes it to `b'R'` once the rest of the frame is durable,
//! so a frame says of itself whether it was ever sealed. That one byte is *part*
//! of recovery's truncation rule and never the whole of it — the three magic
//! bytes beside it are read first, and see the section on it below. A sealed
//! frame is byte-for-byte what it would be without the mark, and every checksum
//! is computed over the sealed form, so an unsealed frame cannot accidentally
//! verify.
//!
//! `image_crc32` covers the image bytes that follow this record. The record's
//! own `crc32` covers the preceding 13 bytes, which is what makes `image_len`
//! safe to trust: recovery uses that length to find the commit record, and a
//! corrupt length would otherwise send it to a wild offset.
//!
//! ### Image
//!
//! `image_len` bytes, holding exactly one application snapshot frame. The image
//! carries its own leading version byte and its own applied Raft index, so it
//! is self-describing independently of this journal's framing.
//!
//! ### Commit record (`RLCM`)
//!
//! The commit record has a fixed size of 13 bytes.
//!
//! ```text
//! magic          [4]   "RLCM"
//! version        u8    1
//! frame_crc32    u32
//! crc32          u32
//! ```
//!
//! `frame_crc32` covers the begin record and the image together. It is not
//! redundant with the two checksums above it: it binds this commit record to
//! this begin record and this image, so a commit record surviving from an
//! abandoned tail cannot seal a different frame that happens to end at the same
//! offset.
//!
//! # Crash contract
//!
//! The authoritative artifact is the journal. The logical commit point of a
//! transaction is the return of the second `sync_data`: the one that follows
//! the single byte sealing a frame already made durable by the first. The
//! logical commit point of a rewrite is the return of the directory sync that
//! follows its rename. `Ok` means the new state is visible to a fresh opener.
//! `Err` means the outcome is unknown, and reopening is the oracle that decides
//! it — never an inference that `Err` left no bytes changed.
//!
//! A crash at any byte boundary leaves the store recoverable to exactly the
//! pre-transaction or the post-transaction state, never between:
//!
//! - Before the first byte of a frame, the journal is unchanged, so recovery
//!   sees the pre-transaction state with a clean tail.
//! - From that first byte to the *second to last*, **when the bytes reached the
//!   medium in the order they were written**, the tail carries the unsealed
//!   append mark and is not a whole frame. Recovery stops at the last committed
//!   frame — the pre-transaction state — and reports
//!   [`TornTail::UnsealedAppend`]. The qualification is load-bearing and is not
//!   a hedge: the same interrupted append with its *leading* bytes still in a
//!   lost cache leaves a foreign begin magic, which is
//!   [`TornTail::NotALedgerFrame`] and refuses. That is fail-closed rather than
//!   pre-or-post, and it is enumerated under
//!   [`TornTail::is_truncatable_residue`] rather than left to be met.
//! - With the whole frame durable and the seal not yet written, the transaction
//!   is written but not committed, which is the same pre-transaction state — and
//!   this is the one boundary `open` will not resolve on its own. These bytes
//!   are also exactly what a committed frame whose mark byte rotted looks like,
//!   so recovery refuses rather than guessing which it is, reporting
//!   [`TornTail::UnsealedCompleteFrame`] through
//!   [`LedgerStoreError::UnreadableFrame`].
//!   [`LedgerStore::open_and_repair`] resolves it to the pre-transaction state
//!   for a caller who has decided. The narrower promise this bullet makes, and
//!   why it is narrower, is argued under "Which residue `open` may truncate".
//! - After the seal's sync returns, the frame is committed, so recovery sees
//!   the post-transaction state.
//!
//! An interrupted rewrite leaves either the original journal or the staged
//! file, never a partial journal, because the staged file is only named
//! `ledger.journal` by an atomic rename. A crash between that rename and the
//! directory sync may leave either the old or the new name durable; both are
//! whole, valid journals, so the pre-or-post property still holds and only the
//! `Ok` is withheld.
//!
//! Recovery truncates a torn tail before the store accepts another
//! transaction, so an append can never follow abandoned bytes. It is
//! idempotent: a crash during it leaves work a later open repeats.
//!
//! # Which residue [`LedgerStore::open`] may truncate
//!
//! **Opening this store is not a read.** It is nearly one, and the exception is
//! the subject of this section, so it is stated first and without a softening
//! clause: `open` shortens the journal in two cases, and in one of the two the
//! bytes it deletes may be a transaction this replica already acknowledged to a
//! client. No flag gates that case. [`LedgerStore::open_and_repair`] is a
//! different and larger loss, not the only one.
//!
//! Keeping the truncation honest needs a rule sharper than "the scan stopped
//! here". Recovery walks frames from the header and stops at the first one it
//! cannot read. Where it stopped is *not* evidence about whether the bytes
//! beyond it were committed: a frame in the middle of a long journal can become
//! unreadable, and everything after it — whole, correctly sealed, acknowledged
//! transactions — is then unreachable too, because the next frame's offset is
//! only knowable through the one that cannot be read.
//!
//! So `open` truncates under exactly two rules, and they carry two different
//! guarantees. The first is a proof that no commit point covered the bytes. The
//! second is not a proof and does not claim to be. Both are stated in the
//! direction they are used, and [`TornTail::is_truncatable_residue`] is where
//! the pair is written down with each one's boundary.
//!
//! An earlier shape of this store argued the other direction. It enumerated
//! what an interrupted append leaves — fewer bytes than a begin record, a
//! partial image, a whole image with no commit record, a partial commit record
//! — showed the list was exhaustive, and then treated finding one of those
//! shapes as proof that an append had been interrupted. That converse is false,
//! and one byte is enough to show it. A journal that loses its last byte ends
//! in a partial commit record. It is a strict prefix of a frame this build
//! wrote; it carries this store's magic and this build's version; every byte
//! present was written and no checksum over present bytes fails. It satisfies
//! the enumeration exactly, and the frame it ends in was committed and
//! acknowledged. Truncating it deleted a whole transaction from the medium
//! during an operation the caller asked for as a read, and reported the loss as
//! "bytes no commit point ever covered".
//!
//! The same argument ran backwards at the other end. A tail of zero bytes —
//! what a crash on a delayed-allocation filesystem actually leaves, when the
//! file's size reached the medium and its data did not — was benign below a
//! begin record's length and corruption at or above it, because that is where
//! the zeros started failing a magic test. One kind of residue, two opposite
//! verdicts, decided by how many bytes happened to land.
//!
//! No test on the bytes can separate a truncated committed frame from an
//! interrupted append, because they *are* the same bytes. So the write path
//! puts the distinction into the artifact instead of asking recovery to infer
//! it from where the scan got to.
//!
//! **A frame's first byte is its append mark.** It is `0x00` while the frame is
//! being appended and `b'R'` — the begin magic's leading byte — once the frame
//! is durable. An append writes the unsealed mark before any other byte of the
//! frame, makes the whole frame durable, and only then writes the one byte that
//! seals it. Since a crash leaves a prefix of what was written, the mark is
//! written first and *every* interrupted append leaves it unsealed.
//!
//! ## The identity is asked before the mark
//!
//! The mark is a zero byte, and the sentence that used to stand here — "zeros
//! are the unsealed mark, so the ordinary residue of a delayed allocation reads
//! as exactly what it is, at every length" — is the same sentence that made
//! zeros landing over a *committed* frame read as residue too. It was true of
//! the residue it was written about and claimed a scope one step wider than the
//! mechanism reached.
//!
//! So the mark is no longer the first thing read. Bytes one through three of the
//! begin magic are the frame's **identity**, they are not the mark, and no
//! append this store performs ever leaves them wrong: the append writes the
//! whole begin record with byte zero held at `0x00`, so the first write that
//! reaches byte one carries `b'L'`. Recovery asks the identity at every length,
//! *above* the mark, and a tail that fails it is
//! [`TornTail::NotALedgerFrame`] — a refusal, never residue.
//!
//! That closes the two-byte case the mark alone could not. One zeroed byte is
//! the mark and is refused as a whole unsealed frame. Two zeroed bytes reach the
//! identity and are refused as a foreign begin magic. Seventeen — one begin
//! record, and anything up to a sector — likewise. The sibling lock store has
//! read its magic above its mark since the generation that fixed it there; this
//! is that store's `verify_identity`, statement for statement, and
//! `the_recognition_order_matches_the_sibling_lock_store` in each store's tests
//! is what keeps the two from drifting apart again on a third byte.
//!
//! One residue has to survive the identity test, and it is named rather than
//! smuggled through: a tail that is **zeros all the way to the end of the file**
//! is [`TornTail::ZeroFilledToEnd`] and is truncated. That is what a crash on a
//! delayed-allocation filesystem leaves when the file's size reached the medium
//! and its data did not, and refusing it would brick a replica on the most
//! ordinary crash there is.
//!
//! This is the case where `open` may delete an acknowledged transaction, and
//! the trade is made deliberately. The premise underneath it is a claim about
//! the physical world rather than about this program, and
//! [`TornTail::is_truncatable_residue`] states it, states the loss it admits — a
//! committed final region that a zeroed sector erased is discarded — and states
//! the bound: the loss can never reach a byte that is not itself zero, and never
//! a frame beyond the damage. [`RecoveryReport::discarded_without_proof`] is how
//! a caller learns it happened without having to match on the variant and read
//! this page.
//!
//! Every *other* zero run refuses, but not for the reason this paragraph used to
//! give. It said such a run is "every zero run that has a committed frame behind
//! it", and that is a different set: an interrupted append whose leading bytes
//! never landed is a zero run with non-zero bytes after it and nothing committed
//! behind it at all, and it refuses too. Refusing is fail-closed and costs an
//! operator action on a crash that lost nothing; the enumeration is under
//! [`TornTail::is_truncatable_residue`].
//!
//! ## The mark is half the rule, not the rule
//!
//! An earlier shape of this store took that paragraph and read it backwards: it
//! proved `interrupted ⇒ unsealed`, took the contrapositive `sealed ⇒ not
//! interrupted`, and then truncated on an unsealed mark — which needs `unsealed
//! ⇒ was being written`, a third statement neither of the first two implies. It
//! is also false, and one byte shows it. `b'R'` is `0x52`. Rot it to `0x00` in a
//! committed frame and that frame reads as an interrupted append; `open`
//! truncated from there to the end of the file, returned `Ok`, and reported the
//! deleted transactions as "bytes no commit point ever covered". Every *other*
//! byte of the same begin record is protected by a checksum and refuses the
//! store. The mark byte was the one byte no checksum was ever consulted for,
//! because the mark test returned first.
//!
//! So the mark is asked a narrower question now, and a second question is asked
//! beside it. **Truncating requires the unsealed mark *and* positive evidence
//! that the bytes are not a whole frame.** Recovery reads the tail a second time
//! with the mark restored to its sealed value — the value every checksum in the
//! frame was computed over — and only the bytes that still fail to be a whole
//! frame are residue. That is [`TornTail::is_interrupted_append`], whose
//! documentation states the proof in the direction the truncation actually uses
//! it, and names the single-fault assumption it rests on rather than leaving it
//! implicit.
//!
//! The second reading is only reached by bytes that already carry this build's
//! begin identity, because `verify_identity` runs above the mark test. Three
//! shapes come out of it, and they are three different facts:
//!
//! - **Not a whole frame**, at a step this build can read: too short for a begin
//!   record, a begin record that does not verify, a partial or mismatched image,
//!   a missing or partial commit record. With the unsealed mark, this is
//!   [`TornTail::UnsealedAppend`] and it is truncated.
//! - **A whole frame that verifies**, with only the mark reading unsealed. Two
//!   histories leave exactly these bytes — the write-ahead window, and a
//!   committed frame whose mark rotted — and nothing in the bytes tells them
//!   apart. Truncating is right under one and deletes acknowledged history under
//!   the other, so recovery refuses:
//!   [`TornTail::UnsealedCompleteFrame`]. Refusing is recoverable under both
//!   readings, and truncating is recoverable under only one.
//! - **A version this build cannot read**, which stops the second reading before
//!   it can say anything: this build does not know the layout, so it cannot
//!   produce the evidence truncating requires.
//!
//! Every shape with a *sealed* mark is what some completed append sealed, and
//! any damage to it happened afterwards. Such a frame may sit at or below the
//! last commit point, and it makes everything after it unreachable regardless.
//! `open` refuses with [`LedgerStoreError::UnreadableFrame`] rather than
//! treating it as a tail, because treating it as a tail means deleting
//! acknowledged history from the medium during what the caller asked for as a
//! *read*.
//!
//! One shape is refused by *both* entry points rather than being damage a
//! repair may clear: a frame declaring a format version this build cannot read,
//! whether its mark is sealed or not. That needs no corruption at all — a newer
//! build appending over a header this one still reads produces it from healthy
//! bytes — so it is a newer build's committed work, and the remedy for damage
//! must not delete it. It is
//! [`LedgerStoreError::UnsupportedFrameVersion`], separate from the corruption
//! it used to be folded into. The version byte is therefore one place where a
//! single altered byte can make a journal unopenable by either entry point; that
//! is a refusal rather than a loss, and the order is kept deliberately, because
//! the alternative trades an unopenable file for a repair that can delete a
//! newer build's work.
//!
//! # Repairing, as a separate act
//!
//! Discarding a region a commit point may have covered is sometimes the only
//! way forward, so it is available — as [`LedgerStore::open_and_repair`], never
//! as a side effect of opening. That entry point discards from the unreadable
//! frame to the end of the file and records what it did in
//! [`RecoveryReport::repair`]: the offset, the corruption that stopped the
//! scan, and the byte count.
//!
//! It cannot report how many *transactions* were in that region, and that is
//! the honest limit rather than an omission: the frames past a corrupt one
//! cannot be located, let alone counted, which is precisely why discarding them
//! has to be something a caller asks for by name.
//!
//! After any write error the handle is poisoned and every later mutation is
//! refused with [`LedgerStoreError::StoreRequiresReopen`], because a store that
//! failed mid-publication cannot say where its file ends.
//!
//! # What the crash tests do not prove
//!
//! `durable_crash.rs` interrupts publications inside one live process, so it
//! proves which bytes reached the file and what a fresh opener makes of them.
//! It cannot prove that a barrier reached the medium: a process that never dies
//! reads its own writes back through the page cache whether or not `sync_data`
//! ran. Deleting a `sync_data` or a directory sync from this file leaves the
//! suite green. Those calls are justified by the ordering argument above and by
//! review, not by a test, and a claim that this store survives power loss on a
//! particular filesystem needs evidence this suite does not supply.
//!
//! # Where this argument's code lives
//!
//! The sections above are stated once, here, because they are one argument.
//! The code each of them is about lives in a submodule named for it:
//!
//! - `format` — the byte-level vocabulary the `# Format` section specifies: the
//!   journal and staging names, the three magics, the two values a frame's
//!   first byte takes, and the checksum every record is sealed with.
//! - `frame` — the codec over that layout, and the recognition order the
//!   section above on which residue an opening may truncate describes.
//! - `scan` — reading the header and then every committed frame, stopping at
//!   the first residue.
//! - `damage` — [`TornTail`], the vocabulary that section is written in.
//! - `error` — [`LedgerStoreError`] and how each refusal renders.
//! - `fault` — the deterministic crash seam: [`WriteFault`] and [`FaultPlan`].
//! - `domination` — the check a republication at an unchanged applied index
//!   must pass, and [`DeduplicationProgress`].
//! - `report` — [`RecoveryReport`] and [`Repair`].
//! - `journal_file` — the file itself and the one name staged beside it:
//!   creating, truncating, sweeping, and making the directory durable.
//! - `open` — the two entry points the `# Repairing, as a separate act`
//!   section names, and the single branch they differ in.
//! - `publication` — the two write paths, append and rewrite, and the crash
//!   contract's byte order in each.
//! - `handle` — what an open [`LedgerStore`] reports about itself.
//! - [`raw_journal`] — direct byte access, for crash tests only.

use std::path::PathBuf;

use rafter::LogIndex;

use crate::{Ledger, LedgerConfig};

mod damage;
mod domination;
mod error;
mod fault;
mod format;
mod frame;
mod handle;
mod journal_file;
mod open;
mod publication;
pub mod raw_journal;
mod report;
mod scan;

#[cfg(test)]
mod tests;

pub use damage::TornTail;
pub use domination::DeduplicationProgress;
pub use error::LedgerStoreError;
pub use fault::{FaultPlan, WriteFault};
pub use format::{BEGIN_LEN, COMMIT_LEN, HEADER_LEN};
pub use report::{RecoveryReport, Repair};

/// Whether this handle may still publish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Health {
    Healthy,
    ReopenRequired,
}

/// A durable, transactional ledger store over one journal file.
///
/// See the [module documentation](self) for the format and the crash contract.
#[derive(Debug)]
pub struct LedgerStore {
    directory: PathBuf,
    journal_path: PathBuf,
    config: LedgerConfig,
    ledger: Ledger,
    applied_index: LogIndex,
    journal_len: u64,
    health: Health,
    faults: FaultPlan,
    /// Write plans this handle has started, which is what [`FaultPlan`] keys on.
    write_plans: u64,
    fired_fault: Option<WriteFault>,
    recovery: RecoveryReport,
}
