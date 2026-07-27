//! What the bytes past the last committed frame turned out to be.
//!
//! [`TornTail`] is the vocabulary the [module documentation](super)'s section
//! on which residue an opening may truncate is written in: one variant per boundary an
//! interrupted append can stop at, plus the shapes a damaged committed frame
//! leaves that look the same on the medium. Two of them may be truncated and
//! the rest refuse, and [`TornTail::is_truncatable_residue`] carries the
//! argument for each.

use std::fmt;

// Imported for the intra-doc links the prose below carries.
#[allow(unused_imports)]
use super::{error::LedgerStoreError, report::RecoveryReport, LedgerStore};

/// Why recovery stopped before the end of the journal.
///
/// This is reported here because the report says what recovery *found*, not
/// because finding one is benign. Each variant names the byte boundary the
/// interrupted write reached, which is what lets a crash test prove that its
/// injection bit where it aimed.
///
/// **Two** of these variants are truncated by [`LedgerStore::open`], and they
/// are truncated on different grounds — see
/// [`TornTail::is_truncatable_residue`], which is the predicate the destructive
/// branch reads. Only one of the two carries a proof that no commit point
/// covered the bytes. This paragraph used to say only one variant truncates at
/// all and every other refuses, which was written when that was true and left
/// standing when [`TornTail::ZeroFilledToEnd`] joined the disjunction.
///
/// Exactly one of these variants is residue an interrupted append can be shown
/// to have left; see [`TornTail::is_interrupted_append`]. The rest name either
/// damage to a frame that was sealed, a whole frame this store cannot claim was
/// never committed, or bytes whose identity is gone — and reaching one of those
/// refuses the store.
///
/// "Exactly one" is a closure claim, so it is checked rather than asserted:
/// `exactly_one_torn_tail_is_residue_an_interrupted_append_leaves` matches on
/// every variant by name, so a variant added later does not compile until
/// somebody has decided which side of the rule it falls on.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TornTail {
    /// An append wrote part of a frame and never sealed it.
    ///
    /// Both halves of that sentence are checked. The frame carries
    /// `UNSEALED_FRAME_MARK` in its first byte, *and* the bytes present are
    /// not a whole frame: with the mark restored to its sealed value they still
    /// fail to verify, at a step this build can read. `present` is how many
    /// bytes past the last committed frame it reached.
    ///
    /// This is the only variant recovery may truncate. The mark alone would not
    /// earn that, and used to be asked to.
    UnsealedAppend {
        /// Bytes present past the last committed frame.
        present: u64,
    },
    /// A whole frame that verifies, whose mark says it was never sealed.
    ///
    /// Two histories produce these exact bytes and nothing in them tells the
    /// two apart:
    ///
    /// - an append that wrote the whole frame, reached its durability barrier,
    ///   and died before the one byte that seals it — the write-ahead window;
    ///   or
    /// - a committed, acknowledged frame whose one mark byte later rotted from
    ///   `b'R'` to `0x00`.
    ///
    /// They are the same bytes because the mark is the only difference between
    /// the two states on the medium, and a single byte cannot record which of
    /// them it is. Truncating is right under the first reading and deletes an
    /// acknowledged transaction under the second, so recovery refuses and says
    /// so: [`LedgerStoreError::UnreadableFrame`] names it, and
    /// [`LedgerStore::open_and_repair`] is where a caller who has decided which
    /// reading applies says so by name. Refusing is recoverable under both
    /// readings; truncating is recoverable under only one.
    ///
    /// It is a separate variant from [`TornTail::UnsealedAppend`] because the
    /// two are separate facts, and a report that called this one an interrupted
    /// append would be claiming to know which history happened.
    UnsealedCompleteFrame {
        /// Length of the whole frame that verified.
        len: u64,
    },
    /// Every byte from here to the end of the file is zero.
    ///
    /// This is the residue a crash on a delayed-allocation filesystem leaves:
    /// the file's size reached the medium and its data did not. It is truncated,
    /// and the argument for truncating it is *not* the one behind
    /// [`TornTail::UnsealedAppend`] — see [`TornTail::is_truncatable_residue`],
    /// which states the two separately because they rest on different premises
    /// and fail in different places.
    ZeroFilledToEnd {
        /// Bytes present past the last committed frame, all of them zero.
        present: u64,
    },
    /// The bytes here do not carry this build's begin identity.
    ///
    /// Byte zero is neither frame mark, or the magic bytes beside it are not
    /// this store's — as far as the bytes present go.
    ///
    /// This used to say neither shape is something an append can produce,
    /// because an append writes the whole begin magic with byte zero held
    /// unsealed, so bytes one through three carry the identity from the first
    /// write that reaches them. That is a statement about the order the bytes
    /// were *written*, and this variant is decided by the order they reached
    /// the medium. An append interrupted with its leading bytes still in a
    /// cache the crash took leaves exactly this: `[0, 0, b'B', b'G']`, an
    /// identity destroyed by an append that wrote it correctly.
    ///
    /// So one physical event — an interrupted append — lands in
    /// [`TornTail::UnsealedAppend`] and truncates when its bytes arrived front
    /// to back, and here and refuses when they did not. That is fail-closed and
    /// stays fail-closed: the same bytes are also what a committed final frame
    /// leaves when a zeroed region takes its identity and stops short of its
    /// end, and nothing present distinguishes the two. Refusing costs an
    /// operator action on a crash that lost nothing; accepting would delete a
    /// transaction that was acknowledged. The boundary is enumerated under
    /// [`TornTail::is_truncatable_residue`] and walked by
    /// `gen5_an_interrupted_append_is_truncated_or_refused_by_sector_order_alone`.
    ///
    /// This is the sibling lock store's [`SlotDamage::NotALockImage`], under the
    /// name this format uses. The two stores are meant to answer the same byte
    /// pattern the same way, and
    /// `the_recognition_order_matches_the_sibling_lock_store` is where that is
    /// checked rather than hoped for.
    ///
    /// [`SlotDamage::NotALockImage`]: https://docs.rs/rafter-reference-fenced-lock
    NotALedgerFrame {
        /// The four bytes where the begin magic belongs, zero-padded when fewer
        /// than four are present.
        magic: [u8; 4],
    },
    /// A sealed frame holds fewer bytes than one begin record needs.
    PartialBeginRecord,
    /// A sealed frame's begin record magic or own checksum does not verify.
    BeginRecordCorrupt,
    /// A sealed frame declares a format version this build cannot read.
    ///
    /// This needs no corruption at all: a newer build appending a frame over a
    /// header this one still reads produces it from entirely healthy bytes. It
    /// is separated from [`TornTail::BeginRecordCorrupt`] so that a downgrade
    /// cannot be mistaken for a torn write and answered with a repair that
    /// discards a newer build's committed work.
    UnsupportedFrameVersion {
        /// Version byte found in the begin record.
        version: u8,
    },
    /// A sealed frame's image is not fully present.
    PartialImage,
    /// A sealed frame's image is complete but does not match its checksum.
    ImageCorrupt,
    /// A sealed frame's image is complete and no commit record follows it.
    MissingCommitRecord,
    /// A sealed frame holds fewer bytes than one commit record needs.
    PartialCommitRecord,
    /// A sealed frame's commit record is complete but does not seal it.
    CommitRecordCorrupt,
}

impl TornTail {
    /// Whether an interrupted append of *this* build left this.
    ///
    /// This is used in one direction only — a tail may be truncated **because**
    /// it is an interrupted append — so the implication that has to hold is the
    /// one the caller relies on, written here in that direction:
    ///
    /// > **If this returns `true`, no commit point covered those bytes.**
    ///
    /// The proof, stated forwards rather than as somebody else's
    /// contrapositive. Suppose this returns `true`. Then the tail is
    /// [`TornTail::UnsealedAppend`], and `classify_unsealed` produces that
    /// variant only when **both** of these hold of the bytes past the last
    /// committed frame:
    ///
    /// 1. the first byte is `UNSEALED_FRAME_MARK`; and
    /// 2. read again with that byte restored to `SEALED_FRAME_MARK` — the
    ///    value every checksum in a frame is computed over — the bytes still do
    ///    not verify as a whole frame, and they fail at a step whose meaning
    ///    this build knows.
    ///
    /// Now suppose, for contradiction, that some commit point *did* cover them.
    /// A commit point is the promotion of a frame's mark after the whole frame
    /// is durable, so those bytes began as a whole frame that verified with a
    /// sealed mark. By (2) they no longer verify with a sealed mark, so at least
    /// one byte other than the mark has been lost or altered since. By (1) the
    /// mark byte has *also* changed, from `b'R'` to `0x00`. That is two
    /// independent alterations. The crash contract this store rests on admits
    /// exactly one failure — a crash leaves a prefix of what was written — and a
    /// prefix cannot alter a byte it never reached. So no commit point covered
    /// them. ∎
    ///
    /// The assumption in that last step is the honest limit, and it is stated
    /// rather than hidden: a medium that alters the mark byte *and* corrupts the
    /// frame beside it defeats this rule, as it defeats every checksum-based
    /// rule with a single check. What is now excluded is the single-fault case,
    /// which is what a one-byte rot is, and
    /// `no_single_byte_change_to_a_sealed_frame_is_ever_truncatable`
    /// checks that exhaustively rather than asserting it here.
    ///
    /// There is a second thing to say about that assumption, and it is the one
    /// this store went longest without saying. "A crash leaves a prefix of what
    /// was written" is a *stronger* model than the one
    /// [`TornTail::is_truncatable_residue`]'s rule two rests on, which exists
    /// precisely because a crash can leave a file extended with its data
    /// missing. The two are not the same physics, and this predicate does not
    /// get to borrow rule two's model or lend it this proof.
    ///
    /// Where they come apart is visible: an append whose *leading* bytes did not
    /// reach the medium is not a prefix of what was written, and this proof says
    /// nothing about it. It is `verify_identity`'s answer instead — a foreign
    /// begin magic, refused — which is fail-closed and costs nothing this
    /// predicate promised. That case is enumerated on
    /// [`TornTail::is_truncatable_residue`] rather than left to be met, and the
    /// proof above is stated for the model it holds under and no wider.
    ///
    /// The two things this deliberately does **not** cover are
    /// [`TornTail::UnsealedCompleteFrame`] — a whole frame with an unsealed
    /// mark, where step (2) fails and the answer is a refusal — and every shape
    /// with a sealed mark, where the bytes are what some completed append sealed
    /// and any damage to them happened afterwards.
    ///
    /// It also does not cover [`TornTail::ZeroFilledToEnd`], which is truncated
    /// too but earns it a different way. That separation is deliberate: this
    /// predicate's proof would be false of those bytes, and one predicate
    /// carrying two proofs is how the scope of a rule drifts past the mechanism
    /// that implements it. [`TornTail::is_truncatable_residue`] is the
    /// disjunction the truncating branch actually reads.
    #[must_use]
    pub const fn is_interrupted_append(self) -> bool {
        matches!(self, Self::UnsealedAppend { .. })
    }

    /// Whether [`LedgerStore::open`] may shorten the file here.
    ///
    /// This is the predicate the destructive branch reads, and it is a
    /// disjunction of **two** rules with two different scopes and two different
    /// failure boundaries. They are stated apart because a single sentence
    /// covering both would be true of neither.
    ///
    /// **Rule one — [`TornTail::UnsealedAppend`].** Scope: bytes carrying this
    /// build's begin identity with an unsealed mark that are provably not a
    /// whole frame. Guarantee: *no commit point covered them*, proved in
    /// [`TornTail::is_interrupted_append`] from the single-fault assumption.
    /// Outside that scope, named rather than implied:
    ///
    /// - Two independent alterations to one committed frame — its mark byte
    ///   rotted to zero **and** some other byte of the same frame lost or
    ///   altered — satisfy both halves and are truncated. One physical event
    ///   that does this is a torn or zeroed region beginning at the frame's
    ///   first byte, and such a region takes the identity bytes with it, so it
    ///   reaches [`TornTail::NotALedgerFrame`] and refuses — except at length
    ///   one, where the region *is* the mark byte, the identity survives, and
    ///   the re-read makes it [`TornTail::UnsealedCompleteFrame`], which also
    ///   refuses. What remains outside is two *disjoint* events, which the crash
    ///   contract does not admit.
    ///   `a_zero_run_over_a_committed_frame_is_refused_at_every_length` walks
    ///   both of those on each side of the line.
    ///
    /// - An interrupted append whose **leading** bytes did not reach the medium
    ///   is not in this scope either, and it is the case neither this list nor
    ///   rule two's used to hold. It is one append, and its verdict flips on
    ///   writeback order alone: front to back it is rule one and truncates,
    ///   and with the first two bytes lost it is a foreign begin magic and
    ///   refuses. No commit point covered those bytes, so refusing costs an
    ///   operator action on a crash that lost nothing — and it stays a refusal,
    ///   because the same bytes are what a committed final frame leaves when a
    ///   zeroed region takes its identity and stops before its end. Nothing
    ///   present separates the two, and only one of them can be truncated
    ///   without deleting an acknowledged transaction.
    ///   `gen5_an_interrupted_append_is_truncated_or_refused_by_sector_order_alone`
    ///   is the pair, and `LedgerStore::open_and_repair` is the way out of the
    ///   refusing half — where the `Repair` it reports is an upper bound on the
    ///   loss and, in this case, an upper bound on nothing.
    ///
    /// **Rule two — [`TornTail::ZeroFilledToEnd`].** Scope: every byte from here
    /// to the end of the file is zero. Guarantee is weaker and is stated as the
    /// weaker thing it is: *every byte discarded is a zero, and no byte beyond
    /// them exists to lose*. It is not "no commit point covered them", and it
    /// cannot be — a committed final frame that a zeroed sector erased leaves
    /// exactly these bytes, and nothing distinguishes it from the delayed
    /// allocation this rule exists for. What is outside this scope:
    ///
    /// - Committed frames at the end of the journal that a zeroed region erased
    ///   are discarded, and the transactions in them are lost — including
    ///   transactions this replica already acknowledged to a client. **This
    ///   happens under [`LedgerStore::open`], with no flag.** It is the residual
    ///   loss of the whole design, it is bounded by the zeroed region — the loss
    ///   can never reach a byte that is not itself zero, and never a frame
    ///   beyond the damage — and it is bounded by nothing else.
    ///   `a_zeroed_final_frame_is_lost_and_the_loss_stops_there` pins it on both
    ///   sides of the boundary. [`RecoveryReport::discarded_without_proof`]
    ///   reports it, because a loss a caller has to match on a variant to learn
    ///   about is a loss most callers will not learn about.
    /// - The premise underneath rule two is a claim about the physical world,
    ///   not about this program: that a crash which extends a file without
    ///   persisting its data leaves zeros, and that such a tail is far more
    ///   often residue than an erased commit. It is not a proof and rule one's
    ///   proof does not reach it; the two are kept apart for that reason.
    ///
    /// **Why rule two is not behind the repair flag.** It was considered and
    /// rejected, and the reason is worth stating because the flag is where a
    /// reader will expect to find it. Gating it would refuse the store on the
    /// most ordinary crash on a delayed-allocation filesystem, so a replica
    /// would need an operator after an ordinary power cut, and the operator's
    /// only available remedy would be the flag — which discards strictly more.
    /// A gate whose expected outcome is that everyone always passes it is not a
    /// decision point; it is a slower default with a worse remedy attached.
    ///
    /// What that argument does *not* license is the sentence it used to sit
    /// under, that opening is a read and only the flag can lose acknowledged
    /// work. So the trade is made and the report names it, rather than the
    /// report staying quiet and the prose covering for it.
    #[must_use]
    pub const fn is_truncatable_residue(self) -> bool {
        matches!(
            self,
            Self::UnsealedAppend { .. } | Self::ZeroFilledToEnd { .. }
        )
    }
}

impl fmt::Display for TornTail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsealedAppend { present } => {
                return write!(formatter, "{present} bytes of an unsealed append")
            }
            Self::UnsealedCompleteFrame { len } => {
                return write!(
                    formatter,
                    "a whole {len} byte frame whose append mark reads unsealed"
                )
            }
            Self::ZeroFilledToEnd { present } => {
                return write!(formatter, "{present} zero bytes to the end of the file")
            }
            Self::NotALedgerFrame { magic } => {
                return write!(formatter, "foreign begin magic {magic:?}")
            }
            Self::UnsupportedFrameVersion { version } => {
                return write!(formatter, "a frame of format version {version}")
            }
            _ => {}
        }
        formatter.write_str(match self {
            Self::PartialBeginRecord => "a sealed frame cut inside its begin record",
            Self::BeginRecordCorrupt => "a sealed frame with a corrupt begin record",
            Self::PartialImage => "a sealed frame cut inside its image",
            Self::ImageCorrupt => "a sealed frame with a corrupt image",
            Self::MissingCommitRecord => "a sealed frame with no commit record",
            Self::PartialCommitRecord => "a sealed frame cut inside its commit record",
            Self::CommitRecordCorrupt => "a sealed frame whose commit record seals nothing",
            Self::UnsealedAppend { .. }
            | Self::UnsealedCompleteFrame { .. }
            | Self::ZeroFilledToEnd { .. }
            | Self::NotALedgerFrame { .. }
            | Self::UnsupportedFrameVersion { .. } => unreachable!("handled above"),
        })
    }
}
