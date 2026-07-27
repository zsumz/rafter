//! The fenced lock service's durable transactional application backend.
//!
//! [`LockStore`] holds every fact the contract enumerates — the lock table,
//! every tracked resource's fencing high-water mark, all sessions with their
//! cached operation, fingerprint, and result, the replicated logical time, and
//! the applied Raft index — and moves all of them across one atomic, durable
//! commit point. A reader auditing this store should be able to answer, from
//! this file alone, what the commit point is and what a crash on either side of
//! it leaves.
//!
//! # Why two alternating whole images
//!
//! The contract requires one transaction to bind six kinds of fact together,
//! and it singles out one of them: a per-resource high-water mark that must
//! never decrease, through release, expiration, recreation, snapshot,
//! compaction, and restart. Losing a mark reissues a token that a guarded
//! resource has already accepted, which is the exact failure fencing exists to
//! prevent. That requirement, rather than throughput, decided this design.
//!
//! The store keeps two slot files and publishes into whichever one is *not*
//! currently authoritative. A slot is a whole image with a trailing checksum,
//! and the newest slot whose checksum verifies wins. This buys three things:
//!
//! 1. **There is only one write path.** A publication never renames, never
//!    stages a third file, never appends after earlier bytes, and never
//!    truncates a tail it has to reason about. Applying a batch and installing
//!    a snapshot are the same publication with different applied-index rules,
//!    so there is one crash argument to audit instead of two.
//! 2. **The authoritative image is never the one being written.** That is this
//!    store's own atomicity argument: a crash at any byte of a publication
//!    leaves the previous image untouched and readable, because the file
//!    holding it was not open for writing.
//!
//!    It is an argument about what *this store* does to the files, and it is
//!    worth being precise about what it therefore does not cover. It does not
//!    say the authoritative image is intact — a medium can lose bytes from a
//!    file nobody has open — and recovery must not read it as though it did.
//!    Distinguishing "this slot was being written" from "this slot was whole
//!    and then lost bytes" needs evidence in the bytes, not this sentence; the
//!    publication mark below is that evidence.
//! 3. **The previous image survives the commit that replaces it**, which is
//!    what lets recovery *prove* rather than assume that marks did not regress
//!    across the transaction it is recovering. A design that discarded history
//!    on every commit would have to take the newest image's word for it.
//!
//! A write-ahead journal of appended frames — the shape the sibling ledger
//! consumer chose — was the alternative. It is a better fit for state that
//! grows by deltas or that wants an audit trail, and it makes the "written but
//! not committed" window explicit in the file layout. It was rejected here for
//! two reasons. The lock's durable state is bounded twice over by
//! [`LockConfig`] and, crucially, its tracked-resource table never shrinks, so
//! an appending journal would need a compaction mechanism forever just to bound
//! a state that is already bounded — and compaction is a second publication
//! path, in the one system where a bookkeeping slip loses a mark. And an
//! appending journal keeps history only until that compaction runs, so the
//! cross-check in (3) would hold only sometimes.
//!
//! What this design gives up, stated plainly: it overwrites in place, so it
//! relies on the argument in (2) rather than on append-only immutability, and
//! it rewrites the whole image for a one-field change. The first is checked by
//! the crash sweep in `durable_crash.rs`; the second is affordable because the
//! image is config-bounded and small.
//!
//! # Mark durability
//!
//! Two checks defend the marks at the narrowest points available:
//!
//! - **Every publication** must dominate the marks the store has already
//!   durably acknowledged: every resource tracked in the live image must be
//!   tracked in the proposed one with a mark at least as high. A commit or a
//!   snapshot install that would lower one is refused with
//!   [`LockStoreError::MarkRegression`] before a byte is written.
//! - **Every recovery** that finds both slots intact re-runs that comparison
//!   across the commit boundary it is recovering, using the older slot the
//!   design happens to preserve.
//!
//! Neither check can substitute for the model's own bookkeeping, and neither is
//! meant to: [`LockService`] remains the semantic authority on which token a
//! resource issues next. These are the durability boundary refusing to publish
//! or adopt a state that contradicts contract invariant 2.
//!
//! # Republishing one commit point
//!
//! [`LockStore::install`] may republish the applied index the store already
//! holds, because adopting the state a replica already has must not require
//! inventing an index. That is the one publication whose freshness the applied
//! index cannot judge — two images can name the same index and still disagree
//! about which requests have completed.
//!
//! The session cache is what disagrees. Dropping a client slot's cached
//! completion makes an acknowledged operation executable again, and for an
//! acquisition that mints a *second* fencing token for one tenure — the same
//! failure as a lost mark, reached by another road. So an install at an
//! unchanged applied index must also dominate the durable session cache, slot
//! by slot, through [`SessionProgress`]: the session epoch first, then the
//! highest completed sequence under it. Opening a newer epoch is what
//! legitimately clears an older epoch's cache, which is why the epoch outranks
//! the sequence rather than sitting beside it.
//!
//! The check is scoped to an unchanged index deliberately. Above that index the
//! model has legitimately advanced, and it — not the durability boundary — is
//! the authority on which sessions it retired or replaced along the way. At an
//! unchanged index nothing legitimately changed, so a state that lost a
//! completion is simply a poorer image of the same commit point, and
//! [`LockStoreError::SessionCacheRegression`] refuses it before a byte is
//! written.
//!
//! # Format
//!
//! The store owns one directory containing exactly two files, `lock-state.0`
//! and `lock-state.1`. Nothing else in the directory is durable state, and no
//! temporary or staging file is ever created, so there is nothing to clean up
//! at open.
//!
//! Ownership of that directory is assumed rather than enforced. Two live stores
//! over one directory would publish into the same slots and destroy each
//! other's images, and nothing here stops them. A durable process composition
//! needs a real exclusive lock.
//!
//! This crate's `lock-node` binary supplies one, and it is worth being exact
//! about what that does and does not mean for a store opened anywhere else.
//! That process takes `rafter-storage`'s operating-system lock over its Raft
//! directory *before* it opens the store beside it, and holds it for the life
//! of the replica; a second process is refused there and never reaches these
//! slots. Nothing in this module participates in that. An embedder that opens a
//! store without an equivalent exclusion gets no complaint from here, and the
//! first symptom is two interleaved publications. The exclusion is the
//! embedder's, and it is stated in that direction because that is the direction
//! the code enforces it in: not at all.
//!
//! Unless a record says otherwise:
//!
//! - integers are unsigned and big-endian;
//! - records are packed with no alignment or padding;
//! - a magic or version other than the one named here refuses the store, and is
//!   never quietly skipped in favour of the other slot;
//! - checksums are CRC-32/IEEE, an accidental-corruption check and not an
//!   authentication tag; and
//! - a slot file of zero bytes is damage: creation writes a mark into each
//!   slot, so an empty slot is not a state this store ever leaves behind.
//!
//! ## Slot header (`RFLK`)
//!
//! The header has a fixed size of [`SLOT_HEADER_LEN`] bytes at offset zero.
//!
//! ```text
//! magic          [4]   "RFLK", with byte 0 held at 0x00 until the image seals
//! version        u8    1
//! generation     u64
//! applied_index  u64
//! max_clients    u32
//! max_resources  u32
//! payload_len    u32
//! crc32          u32
//! ```
//!
//! The magic's first byte doubles as the **publication mark**. A publication
//! writes it as `0x00` and promotes it to `b'R'` once the rest of the image is
//! durable, so a slot says of itself whether it was ever sealed. That one byte
//! is half of recovery's skip rule; see the section on it below. A sealed
//! slot is byte-for-byte what it would be without the mark, and both checksums
//! are computed over the sealed form, so an unsealed image cannot accidentally
//! verify either.
//!
//! `generation` is what orders the two slots, and it is why the header carries
//! its own checksum: recovery must not choose between two images using bytes it
//! has not verified. The same checksum is what makes `payload_len` safe to
//! trust when locating the trailer.
//!
//! `max_clients` and `max_resources` are the [`LockConfig`] the slot was
//! written under. Opening under different bounds is rejected rather than
//! reinterpreted, because the bounds decide which images are valid.
//!
//! `applied_index` is deliberately duplicated here and inside the payload. This
//! copy is what recovery orders, reports, and range-checks without decoding
//! anything; the payload's copy is what the Raft snapshot install path
//! cross-checks against its descriptor. A slot whose two copies disagree is
//! refused rather than reconciled.
//!
//! ## Payload
//!
//! `payload_len` bytes holding exactly one application snapshot frame, as
//! produced by the adapter's `encode_snapshot`. The store commits the same
//! bytes the Raft install path carries: the contract enumerates one set of
//! facts for the durable transaction and for the application snapshot, so
//! encoding them twice would be two chances to forget a high-water mark.
//!
//! ## Trailer
//!
//! ```text
//! crc32          u32
//! ```
//!
//! [`SLOT_TRAILER_LEN`] bytes covering the header and the payload together. A
//! slot's image counts only when its header verifies, its payload is entirely
//! present, and this checksum matches everything before it. Covering the header
//! as well as the payload is what stops a payload from one generation being
//! read under a header from another.
//!
//! The trailer is not the commit marker, and the difference matters: a trailer
//! is at the end, so losing bytes destroys it, and an artifact whose only proof
//! of completeness sits where truncation lands cannot tell "never finished"
//! from "finished and then cut". The publication mark in byte zero is what says
//! an image was sealed; the trailer is what says the sealed bytes are intact.
//!
//! # Crash contract
//!
//! The authoritative artifact is the pair of slot files. The logical commit
//! point of a publication is the return of the second `sync_data`: the one that
//! follows the single byte sealing an image already made durable by the first.
//! `Ok` means the new state is what a fresh opener sees. `Err` means the
//! outcome is unknown, and reopening is the oracle that decides it — never an
//! inference that `Err` left no bytes changed.
//!
//! A crash at any byte boundary leaves the store recoverable to exactly the
//! pre-transaction or the post-transaction state, never between:
//!
//! - Before the stale slot is opened, both files are unchanged.
//! - From the first byte of the new image to the *second to last*, that slot
//!   carries the unsealed mark and holds no whole image, whatever mixture of new
//!   prefix and older tail it is. That is
//!   [`SlotDamage::UnsealedPublication`]: it cannot be chosen, and the live slot
//!   still holds the pre-transaction state.
//! - With the whole image durable and the seal not yet written, the image is
//!   written but not committed — and this is the one boundary
//!   [`LockStore::open`] will not resolve on its own. These bytes are also
//!   exactly what a committed slot whose mark byte rotted leaves, so recovery
//!   refuses rather than guessing which it is, reporting
//!   [`SlotDamage::UnsealedCompleteImage`] through
//!   [`LockStoreError::UnreadableSlot`]. [`LockStore::open_and_repair`] resolves
//!   it to the pre-transaction state for a caller who has decided. The narrower
//!   promise this bullet makes, and why it is narrower, is argued in the section
//!   below.
//! - After the seal's sync returns, the new slot is committed, outranks the old
//!   one by generation, and is what recovery adopts.
//!
//! Nothing is truncated or rewritten at open. The next publication overwrites
//! the slot it does not adopt, so the store heals itself; the repair entry point
//! chooses which of two readings of a store to open under, and writes nothing
//! itself.
//!
//! # Which unreadable slots recovery may skip
//!
//! Recovery is allowed to skip a slot it cannot read only when it can *prove*
//! that slot was not the live image. This section is that proof, and it is
//! written in the direction the proof is used.
//!
//! An earlier shape of this store argued the other direction and got away with
//! it for a while. It enumerated what an interrupted publication leaves — a
//! short header, a short payload, a payload with no trailer, a torn trailer —
//! showed the list was exhaustive, and then treated finding one of those shapes
//! as proof that a publication had been interrupted. That converse is false,
//! and one byte is enough to show it. A sealed image that loses its last byte
//! is a torn trailer. It is a strict prefix of an image this build wrote; it
//! carries this store's magic and this build's version; every byte present was
//! written and no checksum over present bytes fails. It satisfies the
//! enumeration exactly, and it is the live image. Skipping it adopts the stale
//! partner, drops an acknowledged fencing high-water mark, and reissues a token
//! a guarded resource has already accepted — the exact failure this design
//! exists to prevent, reached through the rule that was supposed to prevent it.
//!
//! No test on the bytes can separate those two cases, because they *are* the
//! same bytes: "a slot holding a prefix of the image with generation g, beside
//! a sealed image with generation g-1" describes both an interrupted
//! publication and a torn-off live image, and the generations differ by one
//! either way. So the write path puts the distinction into the artifact instead
//! of asking recovery to infer it.
//!
//! **A slot's first byte is its publication mark.** It is `0x00` while an image
//! is being written and `b'R'` — the magic's leading byte — once that image is
//! sealed. A publication writes the unsealed mark before any other byte of the
//! image, makes the whole image durable, and only then writes the one byte that
//! seals it. Since a crash leaves a prefix of what was written, byte zero is
//! written first and *every* interrupted publication leaves the unsealed mark.
//!
//! ## The mark is half the rule, not the rule
//!
//! Reading it as the whole rule is how this section was wrong the second time.
//! `interrupted ⇒ unsealed` is what the paragraph above proves; its
//! contrapositive is `sealed ⇒ not interrupted`; and *skipping* on an unsealed
//! mark needs neither of those but `unsealed ⇒ was being written`, which is a
//! third statement, and false. One byte shows it. The sealed mark is `0x52` and
//! the unsealed value is `0x00`, so a live slot whose first byte rots between
//! them reads as residue. Recovery then adopted the stale partner: an
//! acknowledged fencing high-water mark regressed by a generation, the token it
//! had reached was reissued to a fresh tenure, and a guarded resource accepted
//! two independent tenures under one token — the exact failure this design
//! exists to prevent, reached through the rule that was supposed to prevent it.
//! Every *other* byte of the same header is under a checksum and refuses the
//! store. The mark byte was the only header byte no checksum was ever consulted
//! for, because the mark test returned first.
//!
//! So skipping now requires the unsealed mark **and** positive evidence that the
//! bytes are not a whole image. Recovery reads the slot a second time with the
//! mark restored to the value both checksums were computed over, and skips only
//! what still fails to verify at a step this build can read. Three outcomes,
//! three different facts:
//!
//! - **Not a whole image**: a header cut short, a header checksum over bytes
//!   that are all present, a payload that is not all there, no trailer, a torn
//!   trailer, a trailer that seals nothing, bytes past the seal. With the
//!   unsealed mark, that is [`SlotDamage::UnsealedPublication`] and it is
//!   skipped. This is the ordinary residue of an interrupted publication.
//! - **A whole image that verifies**, with only the mark reading unsealed. Two
//!   histories leave exactly these bytes — the written-but-not-committed window,
//!   and a live slot whose mark rotted — and nothing in them separates the two,
//!   the generations included: the slot being written carries the live slot's
//!   generation plus one under both readings. That is
//!   [`SlotDamage::UnsealedCompleteImage`] and recovery refuses. Refusing is
//!   recoverable under both readings and skipping is recoverable under only one,
//!   and the choice is made on that asymmetry rather than on a guess about which
//!   history is likelier.
//! - **A version this build cannot read**, which stops the second reading before
//!   it can say anything at all.
//!
//! [`SlotDamage::is_publication_residue`] states the proof in the direction it
//! is used and names the single-fault assumption it rests on. **Every other
//! damage refuses the whole store.** A foreign magic, a version this build does
//! not write, a checksum over bytes that are all present, bytes beyond the seal,
//! a sealed image cut short, an emptied file: none of them can be shown to be
//! the slot that was being written, so adopting its partner would silently roll
//! the store back one generation. That is [`LockStoreError::UnreadableSlot`],
//! and it is a refusal to open rather than damage to skip.
//!
//! That the mark byte is now no weaker than its neighbours is a claim about
//! every byte of an image, so it is checked as one:
//! `no_single_byte_change_to_a_sealed_image_is_ever_publication_residue` alters
//! every byte of a sealed image to every other value it could take and requires
//! that none of the results is residue. A rule this narrow is exactly the kind
//! that decays quietly, and a paragraph would not have noticed.
//!
//! Two consequences of that ordering are worth stating on their own, because
//! both were wrong when the shapes were doing the work:
//!
//! - **The magic and the version are tested at every length**, before anything
//!   classifies a slot by how many bytes it has. The old order put both behind
//!   a full-header slice, so twenty bytes of a foreign format were read as this
//!   build's own residue, and the same version byte was refused at one length
//!   and ignored at another. The argument for refusing a version is about the
//!   field, so it has to hold wherever the field is present — and it does, on
//!   both sides of the seal test, because the second reading of an unsealed slot
//!   goes through the same version gate.
//! - **A slot file of zero bytes is damage.** Creation writes the unsealed mark
//!   into each slot, and no publication ever shortens a slot to nothing, so an
//!   empty slot file is not a state this store leaves behind at any point in
//!   its life. A pair of them is not a store that has never committed; it is a
//!   store whose files were emptied, and opening a fresh service over them
//!   would discard every fencing high-water mark with nothing reported.
//!   Likewise a slot file that should exist and does not:
//!   [`LockStoreError::MissingSlot`].
//!
//! One byte in this store is still outside every checksum, and it is the one
//! byte that cannot be inside one: a slot of length one is the creation mark,
//! and the whole artifact is the byte. What holds it up is that a publication
//! never shortens a slot below one header and one trailer, so the only one-byte
//! slot this store writes is the one creation writes — a sealed image cut to one
//! byte is [`SlotDamage::HeaderIncomplete`], and reaching the benign answer from
//! a sealed image needs the truncation *and* a change to the surviving byte.
//! `a_one_byte_slot_is_benign_only_at_the_creation_mark` pins all 256 values.
//!
//! A format-version mismatch is still worth naming on its own, because it needs
//! no corruption at all: a binary downgrade produces it from two entirely
//! healthy files. It is always a refusal, and it is the one refusal
//! [`LockStore::open_and_repair`] will not clear either. That order has a cost,
//! and the cost is named rather than left to be discovered: the version byte is
//! read before the checksum that covers it, so a single altered version byte
//! makes a slot unreadable by both entry points. The alternative trades that for
//! a repair that can discard a newer build's committed work, which is worse.
//!
//! "A refusal and not a loss" is what this used to add, on the strength of every
//! byte still being on the medium. That is true of the bytes and says nothing
//! about the store, which no reading entry point will open. The way forward is
//! the real one or none: run the build whose version that is, or
//! [`LockStore::discard_and_reseed`] and let the log refill the replica.
//! Nothing here reads a version it does not know, and nothing here repairs one.
//!
//! # Repairing, as a separate act
//!
//! Giving up a slot that may have held the newest committed state is sometimes
//! the only way forward, so it is available — as
//! [`LockStore::open_and_repair`], never as a side effect of opening. That entry
//! point adopts the readable partner of a slot this build cannot read and
//! records what it did in [`RecoveryReport::repair`]: which slot was given up,
//! what it held, and the generation adopted in its place.
//!
//! ## What a repair may give up
//!
//! Not everything, and the limit is checked rather than described. **Wherever an
//! image is discarded or set aside, its fencing high-water marks are compared
//! against the image adopted in its place**, and a discard the adopted image
//! cannot dominate is refused — by *both* entry points.
//!
//! That rule closes a hole this section used to describe its way past. The
//! sentence below said reading the discarded slot is exactly what failed, so
//! nobody can say what was in it. True of every damage but one, and the
//! exception is the one the ordinary crash produces:
//! [`SlotDamage::UnsealedCompleteImage`] is a whole image that *verified* under
//! the restored mark, decoded far enough to report a generation. The repair
//! adopted the stale partner anyway, reported a generation delta, and an
//! acknowledged `FencingToken(3)` became `FencingToken(2)` while a guarded
//! resource accepted two independent tenures under one token — the failure this
//! design exists to prevent, reached through the entry point added to give its
//! refusals a way forward.
//!
//! Whether a repair that must discard a higher-marked image is ever legitimate
//! is argued, and answered no, on `verify_discard_preserves_marks`, together
//! with what the refusal costs and the way forward it leaves. Two boundaries of
//! that rule are stated there and tested on both sides: the session cache is
//! deliberately *not* required to survive a repair, and an image this build
//! cannot decode cannot be compared at all.
//!
//! For that second case the old sentence is the true one, and
//! [`Repair::marks_cross_checked`] now says which case a caller is in rather
//! than leaving the report to imply a check that did not run. The bound in
//! either case is one publication, because generations are strictly increasing
//! and only two slots exist.
//!
//! The store did without this while its refusals were rarer. What changed is
//! that [`SlotDamage::UnsealedCompleteImage`] turns an ordinary crash into a
//! refusal, and a store whose ordinary crash residue needs an operator with no
//! documented way forward is worse than one that names the way forward and
//! reports what it costs. The sibling ledger reached the same shape from the
//! same argument.
//!
//! ## Three entry points, because there are three decisions
//!
//! [`LockStore::open_and_repair`] does not reach all of that crash, and the
//! half it leaves is the load-bearing one. The mark rule above refuses a
//! discard the adopted image cannot dominate, and an interrupted **acquisition**
//! is exactly that shape: it raises a mark, the interrupted image is the newer
//! one, and no older partner carries a mark that image was the first to hold.
//! So a crash mid-acquisition — the operation a fencing lock exists to perform
//! — is refused by `open` *and* by the repair, with no byte moved either way.
//!
//! [`LockStore::discard_and_reseed`] is the third decision and the only one
//! that opens that directory: it deletes both slot files and lets the
//! replicated log refill the store. It is sound because this store publishes
//! only what the log has already committed, so nothing it deletes is a mark the
//! log cannot return; the argument, what it costs, and the premise about the
//! group that nothing here can check are all on the entry point.
//!
//! Reading them as a ladder: `open` reads, the repair chooses between two
//! readings, and the re-seed keeps neither. Each gives up more than the one
//! before it and each is a separate call, so no caller reaches a later rung by
//! retrying an earlier one.
//!
//! Recovery also refuses when **both** slots are damaged, whatever the damage,
//! rather than starting empty. A lock service that cannot read any image cannot
//! know its high-water marks, and one that started empty would hand out token 1
//! for a resource whose guarded downstream has already accepted a far higher
//! token. That is [`LockStoreError::NoReadableImage`].
//!
//! A store that has never committed is a different case and is not refused: its
//! slots carry their creation marks, which is not damage.
//!
//! What holds generally is narrower than "the unadopted slot holds nothing
//! sealed", because two cases break that: a slot holding a whole unsealed image
//! is set aside when the partner's sealed generation is strictly greater, and
//! [`LockStore::open_and_repair`] adopts while the unadopted slot still holds
//! the damage it gave up on. What holds in every case is that the adopted image
//! is the newest one recovery can show any publication sealed — the set-aside
//! slot is older by generation, and the given-up slot is one the caller asked
//! recovery to stop reading.
//!
//! [`RecoveryReport::damaged_slot`] names **which** slot was damaged as well as
//! how. That index is the load-bearing half: it is what tells a caller the
//! residue sat in the slot that was being written, rather than leaving benign
//! crash residue and a lost generation looking alike.
//!
//! After any write error the handle is poisoned and every later publication is
//! refused with [`LockStoreError::StoreRequiresReopen`], because a store that
//! failed mid-publication cannot say what its stale slot now contains.
//!
//! # What the crash tests do not prove
//!
//! `durable_crash.rs` interrupts publications inside one live process, so it
//! proves which bytes reached the file and what a fresh opener makes of them.
//! It cannot prove that a barrier reached the medium: a process that never dies
//! reads its own writes back through the page cache whether or not `sync_data`
//! ran. Deleting a `sync_data` from this file leaves the suite green. Those
//! calls are justified by the ordering argument above and by review, not by a
//! test, and a claim that this store survives power loss on a particular
//! filesystem needs evidence this suite does not supply.
//!
//! # Where this argument's code lives
//!
//! The sections above are stated once, here, because they are one argument.
//! The code each of them is about lives in a submodule named for it:
//!
//! - `format` — the byte-level vocabulary the `# Format` section specifies: the
//!   two slot names, the header offsets, the two values byte zero takes, and
//!   the CRC-32 fold.
//! - `image` — the codec over that layout, and the recognition order the
//!   section on skipping describes.
//! - `damage` — [`SlotDamage`] and [`SlotState`], the vocabulary that section
//!   is written in.
//! - `error` — [`LockStoreError`] and how each refusal renders.
//! - `fault` — the deterministic crash seam: [`WriteFault`] and [`FaultPlan`].
//! - `domination` — the two checks the `# Mark durability` section names,
//!   the discard rule the `# Repairing` section argues, and [`SessionProgress`].
//! - `report` — [`RecoveryReport`], [`Repair`], and [`Reseed`].
//! - `slot_file` — the two files themselves: establishing them, reading them,
//!   and making the directory durable.
//! - `open`, `reseed`, `adopt` — the three entry points the
//!   `# Three entry points, because there are three decisions` section names,
//!   split by what they do rather than by name: `open` holds the two that read
//!   a store, `reseed` holds the one that keeps neither reading, and `adopt`
//!   holds the single choice the two readers share. Repairing is a branch of
//!   that choice rather than a module, which is what "one opening path,
//!   parameterized" means in code.
//! - `publication` — the one write path, and the crash contract's byte order.
//! - `handle` — what an open [`LockStore`] reports about itself.
//! - [`raw_slot`] — direct byte access, for crash tests only.

use std::{collections::BTreeMap, path::PathBuf};

use rafter::LogIndex;

use crate::{FencingToken, LockConfig, LockService, ResourceName};

mod adopt;
mod damage;
mod domination;
mod error;
mod fault;
mod format;
mod handle;
mod image;
mod open;
mod publication;
pub mod raw_slot;
mod report;
mod reseed;
mod slot_file;

#[cfg(test)]
mod tests;

pub use damage::{SlotDamage, SlotState};
pub use domination::SessionProgress;
pub use error::LockStoreError;
pub use fault::{FaultPlan, WriteFault};
pub use format::{crc32, SlotIndex, SLOT_HEADER_LEN, SLOT_TRAILER_LEN};
pub use report::{RecoveryReport, Repair, Reseed};

/// Whether this handle may still publish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Health {
    Healthy,
    ReopenRequired,
}

/// A durable, transactional lock store over two alternating slot files.
///
/// See the [module documentation](self) for the format, the crash contract, and
/// the argument for this shape.
#[derive(Debug)]
pub struct LockStore {
    directory: PathBuf,
    config: LockConfig,
    service: LockService,
    applied_index: LogIndex,
    generation: u64,
    live_slot: Option<SlotIndex>,
    /// Every fencing high-water mark this store has durably acknowledged.
    acknowledged_marks: BTreeMap<ResourceName, FencingToken>,
    health: Health,
    faults: FaultPlan,
    /// Publications this handle has started, which is what [`FaultPlan`] keys on.
    publications: u64,
    fired_fault: Option<WriteFault>,
    recovery: RecoveryReport,
}
