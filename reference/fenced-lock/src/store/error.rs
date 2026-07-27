//! Every way a durable lock store operation refuses, and how each one reads.
//!
//! The refusals are the store's contract with an operator, so the rendering is
//! part of the artifact rather than a debug aid: the two errors that name a
//! mark regression share one clause and supply their own subjects, and
//! `renders_a_mark_regression_as_one_sentence_after_either_subject` pins both
//! readings after both subjects.

use std::{error::Error, fmt, path::PathBuf};

use rafter::LogIndex;

use crate::{ClientId, FencingToken, LockCodecError, ResourceName, SnapshotError};

use super::{
    damage::{SlotDamage, SlotState},
    domination::SessionProgress,
    fault::WriteFault,
    format::SlotIndex,
};

// Imported for the intra-doc links the prose below carries.
#[allow(unused_imports)]
use super::{fault::FaultPlan, LockStore};

/// Failure of a durable lock store operation.
///
/// This enum is exhaustive because the slot format is closed over these
/// corruption, configuration, and publication failures, and because a caller
/// deciding whether a transaction committed has to be able to match on all of
/// them.
///
/// It is deliberately neither `Clone` nor `Eq`: a variant carrying a live
/// [`std::io::Error`] has no meaningful value equality, and pretending
/// otherwise would invite tests to assert on a projection of an operating
/// system's diagnostics.
#[derive(Debug)]
pub enum LockStoreError {
    /// A filesystem operation failed.
    Io {
        /// Lowercase verb phrase naming the attempted operation.
        operation: &'static str,
        /// Path the operation addressed.
        path: PathBuf,
        /// Underlying filesystem failure.
        source: std::io::Error,
    },
    /// Neither slot holds a readable image, and both are damaged.
    ///
    /// A lock service that cannot read an image cannot know its fencing
    /// high-water marks. Opening empty would hand out token 1 for a resource
    /// whose guarded downstream has already accepted a higher token, so this
    /// fails closed instead.
    NoReadableImage {
        /// What each slot looked like, indexed by [`SlotIndex`].
        slots: [SlotState; 2],
    },
    /// One slot holds bytes recovery cannot show an interrupted publication left.
    ///
    /// Recovery cannot rule out that this slot was the live image, so adopting
    /// its partner would silently roll the store back one generation: an
    /// acknowledged fencing high-water mark would drop and the next acquisition
    /// would reissue a token a guarded resource has already accepted. A slot
    /// this build cannot read is a refusal to open, never residue to skip.
    ///
    /// See [`SlotDamage::is_publication_residue`] for which damage this catches
    /// and why the rest is safe to skip.
    UnreadableSlot {
        /// Slot that could not be read.
        slot: SlotIndex,
        /// Why it could not be read.
        damage: SlotDamage,
        /// What the partner slot held — the image recovery declined to adopt in
        /// its place.
        other: SlotState,
    },
    /// A slot file that should exist does not.
    ///
    /// Every store this build creates has both slot files from its first
    /// instant, so a directory holding one of them lost the other. Recreating
    /// it and adopting the survivor would open the store one generation back —
    /// an acknowledged fencing high-water mark dropped, with `is_clean()` true
    /// and nothing reported. A slot that should exist and does not is
    /// unreadable rather than absent.
    MissingSlot {
        /// Slot whose file is gone.
        slot: SlotIndex,
        /// What the surviving partner held — the image recovery declined to
        /// adopt in its place.
        other: SlotState,
    },
    /// Both slots claim the same generation.
    ///
    /// Publications assign strictly increasing generations, so this is
    /// corruption rather than a crash residue, and it leaves recovery no rule
    /// for choosing between two images.
    AmbiguousGeneration {
        /// Generation both slots declare.
        generation: u64,
    },
    /// A slot was written under different resource bounds.
    ConfigMismatch {
        /// Slot the mismatch was found in.
        slot: SlotIndex,
        /// Client-slot bound recorded in the image.
        image_max_clients: u32,
        /// Tracked-resource bound recorded in the image.
        image_max_resources: u32,
        /// Client-slot bound the caller opened with.
        requested_max_clients: u32,
        /// Tracked-resource bound the caller opened with.
        requested_max_resources: u32,
    },
    /// A verified slot's payload is not a decodable application snapshot.
    ///
    /// The trailing checksum already proved these are the bytes that were
    /// written, so this is a build or encoding fault rather than a torn write,
    /// and it is an error rather than a reason to ignore the slot.
    Image {
        /// Slot the payload came from.
        slot: SlotIndex,
        /// Why the payload could not be decoded.
        source: LockCodecError,
    },
    /// A verified slot's payload violates a lock service invariant.
    Snapshot {
        /// Slot the payload came from.
        slot: SlotIndex,
        /// Which invariant the restored state broke.
        source: SnapshotError,
    },
    /// A slot's header and payload disagree about the applied Raft index.
    AppliedIndexDisagreement {
        /// Slot the disagreement was found in.
        slot: SlotIndex,
        /// Index the header declares.
        header_index: LogIndex,
        /// Index the payload declares.
        payload_index: LogIndex,
    },
    /// A publication or recovery would move the applied floor backwards.
    ///
    /// Recovery in this shape would make an acknowledged command executable
    /// again, which reissues a fencing token.
    AppliedIndexRegression {
        /// Applied index already durable.
        previous: LogIndex,
        /// Applied index that was offered.
        found: LogIndex,
    },
    /// A publication or recovery would lower a fencing high-water mark.
    ///
    /// This is contract invariant 2 refused at the durability boundary. A
    /// resource that vanishes from the state is the same failure as one whose
    /// mark decreases, and both are reported here.
    MarkRegression {
        /// Resource whose mark would move backwards.
        resource: ResourceName,
        /// Mark this store has durably acknowledged.
        acknowledged: FencingToken,
        /// Mark the offered state carries, if it tracks the resource at all.
        offered: Option<FencingToken>,
    },
    /// Giving up or setting aside a slot would lower a fencing high-water mark.
    ///
    /// This is [`LockStoreError::MarkRegression`] reached from recovery's other
    /// side. That one refuses a *publication* whose new state would drop a mark;
    /// this one refuses to discard an image whose marks the image being adopted
    /// in its place does not carry.
    ///
    /// It names the resource and both marks rather than the two generations,
    /// because the generations are not the loss. A repair used to report that it
    /// had moved from generation 6 to generation 5 while an acknowledged
    /// `FencingToken(3)` became `FencingToken(2)`, and a guarded resource then
    /// accepted two independent tenures under one token — the exact failure this
    /// design exists to prevent, reached through the entry point added to give
    /// its refusals a way forward.
    ///
    /// Both entry points refuse it, including [`LockStore::open_and_repair`].
    /// The argument for refusing rather than repairing-and-reporting is on
    /// `verify_discard_preserves_marks`, with what it costs and what it
    /// deliberately does not cover.
    ///
    /// An ordinary crash during an acquisition raises this every time, so it is
    /// not a corner: a store in this state is refused by both entry points that
    /// read it, and [`LockStore::discard_and_reseed`] is the only call that
    /// opens the directory afterwards.
    DiscardWouldRegressMark {
        /// Slot whose image would have been given up or set aside.
        slot: SlotIndex,
        /// Why that slot could not simply be read.
        damage: SlotDamage,
        /// Slot that would have been adopted in its place.
        adopted: SlotIndex,
        /// Resource whose mark would move backwards.
        resource: ResourceName,
        /// Mark the discarded image carries.
        acknowledged: FencingToken,
        /// Mark the adopted image carries, if it tracks the resource at all.
        offered: Option<FencingToken>,
    },
    /// A republication at an unchanged applied index would move a client slot's
    /// session cache backwards.
    ///
    /// The applied Raft index is not the whole ordering key for the session
    /// cache: two images can name the same index and still disagree about which
    /// requests have completed. Adopting the poorer one makes an acknowledged
    /// operation executable again, and for an acquisition that mints a second
    /// fencing token for one tenure.
    SessionCacheRegression {
        /// Client slot whose session cache would move backwards.
        client: ClientId,
        /// Progress this store has durably acknowledged for that slot.
        acknowledged: SessionProgress,
        /// Progress the offered state carries, if it holds the slot at all.
        offered: Option<SessionProgress>,
    },
    /// An image is larger than the slot header's length field can describe.
    ImageTooLarge {
        /// Encoded length of the payload.
        length: u64,
    },
    /// An earlier write left this handle unable to say what its stale slot
    /// holds.
    ///
    /// Reopen the store; recovery is the only thing that can decide what the
    /// interrupted publication left behind.
    StoreRequiresReopen,
    /// A deterministic fault from the store's test construction fired.
    ///
    /// This is the injected-crash seam described on [`FaultPlan`]. It is a
    /// write failure like any other: the handle is poisoned and reopening
    /// decides what the interrupted publication left behind.
    InjectedFault {
        /// The fault that fired.
        fault: WriteFault,
        /// One-based ordinal of the publication it fired on.
        publication: u64,
    },
}

impl fmt::Display for LockStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} at {}: {source}",
                path.display()
            ),
            Self::NoReadableImage { slots } => write!(
                formatter,
                "no slot holds a readable lock image: {} holds {}, {} holds {}",
                SlotIndex::Zero,
                slots[0],
                SlotIndex::One,
                slots[1]
            ),
            Self::UnreadableSlot {
                slot,
                damage,
                other,
            } => write!(
                formatter,
                "{slot} holds {damage}, which recovery cannot show an interrupted publication \
                 left, so it may have been the live image; {} holds {other} and is not adopted in \
                 its place",
                slot.other()
            ),
            Self::MissingSlot { slot, other } => write!(
                formatter,
                "{slot} is missing, so it may have been the live image; {} holds {other} and is not \
                 adopted in its place",
                slot.other()
            ),
            Self::AmbiguousGeneration { generation } => write!(
                formatter,
                "both slots claim generation {generation}, so neither outranks the other"
            ),
            Self::ConfigMismatch {
                slot,
                image_max_clients,
                image_max_resources,
                requested_max_clients,
                requested_max_resources,
            } => write!(
                formatter,
                "{slot} was written for {image_max_clients} clients and {image_max_resources} resources, \
                 but was opened for {requested_max_clients} clients and {requested_max_resources} resources"
            ),
            Self::Image { slot, source } => {
                write!(formatter, "malformed image in {slot}: {source}")
            }
            Self::Snapshot { slot, source } => {
                write!(formatter, "invalid image in {slot}: {source:?}")
            }
            Self::AppliedIndexDisagreement {
                slot,
                header_index,
                payload_index,
            } => write!(
                formatter,
                "{slot} declares applied index {header_index} in its header and {payload_index} in its payload"
            ),
            Self::AppliedIndexRegression { previous, found } => write!(
                formatter,
                "applied index {found} does not advance on the durable {previous}"
            ),
            Self::MarkRegression {
                resource,
                acknowledged,
                offered,
            } => {
                formatter.write_str("the state offered would ")?;
                write_mark_regression_clause(formatter, *resource, *acknowledged, *offered)
            }
            Self::DiscardWouldRegressMark { .. } => write_discard_regression(formatter, self),
            Self::SessionCacheRegression {
                client,
                acknowledged,
                offered,
            } => write_session_cache_regression(formatter, *client, *acknowledged, *offered),
            Self::ImageTooLarge { length } => write!(
                formatter,
                "image of {length} bytes exceeds the slot header's length field"
            ),
            Self::StoreRequiresReopen => formatter.write_str(
                "an earlier write failed mid-publication; reopen the store before mutating it",
            ),
            Self::InjectedFault { fault, publication } => {
                write!(formatter, "injected {fault} on publication {publication}")
            }
        }
    }
}

/// Renders a discard refused for regressing a mark, naming the slots first and
/// then completing the clause every mark regression shares.
fn write_discard_regression(
    formatter: &mut fmt::Formatter<'_>,
    error: &LockStoreError,
) -> fmt::Result {
    let LockStoreError::DiscardWouldRegressMark {
        slot,
        damage,
        adopted,
        resource,
        acknowledged,
        offered,
    } = error
    else {
        unreachable!("only its own variant reaches here");
    };
    write!(
        formatter,
        "giving up {slot}, which holds {damage}, and adopting {adopted} in its place would "
    )?;
    write_mark_regression_clause(formatter, *resource, *acknowledged, *offered)
}

/// Renders the loss a mark regression is, as a verb phrase its caller completes.
///
/// It is a phrase rather than a sentence because two errors name the same loss
/// after two different subjects, and it used to be a sentence. That produced
/// `"…adopting lock-state.0 in its place would resource orders/shard-0 would
/// drop from fencing high-water mark 2 to 1"` — two subjects and two verbs
/// spliced into one clause, in the one refusal an operator has to act on. A
/// phrase with no subject cannot be composed that way, because neither caller
/// can supply one.
///
/// It reads differently when the resource vanished from the offered state than
/// when its mark merely dropped, which is the same distinction the variant's
/// `offered` field carries. `renders_a_mark_regression_as_one_sentence_after_
/// either_subject` pins both readings after both subjects.
fn write_mark_regression_clause(
    formatter: &mut fmt::Formatter<'_>,
    resource: ResourceName,
    acknowledged: FencingToken,
    offered: Option<FencingToken>,
) -> fmt::Result {
    match offered {
        Some(offered) => write!(
            formatter,
            "drop resource {} from fencing high-water mark {} to {}",
            resource.as_str(),
            acknowledged.get(),
            offered.get()
        ),
        None => write!(
            formatter,
            "lose resource {}'s fencing high-water mark of {}",
            resource.as_str(),
            acknowledged.get()
        ),
    }
}

/// Renders a session cache regression, which reads differently when the client
/// slot vanished from the offered state than when its progress merely dropped.
fn write_session_cache_regression(
    formatter: &mut fmt::Formatter<'_>,
    client: ClientId,
    acknowledged: SessionProgress,
    offered: Option<SessionProgress>,
) -> fmt::Result {
    match offered {
        Some(offered) => write!(
            formatter,
            "client slot {} would drop from {acknowledged} to {offered} at an unchanged applied index",
            client.get()
        ),
        None => write!(
            formatter,
            "client slot {} would lose its session cache at {acknowledged}",
            client.get()
        ),
    }
}

impl Error for LockStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Image { source, .. } => Some(source),
            Self::NoReadableImage { .. }
            | Self::UnreadableSlot { .. }
            | Self::MissingSlot { .. }
            | Self::AmbiguousGeneration { .. }
            | Self::ConfigMismatch { .. }
            | Self::Snapshot { .. }
            | Self::AppliedIndexDisagreement { .. }
            | Self::AppliedIndexRegression { .. }
            | Self::MarkRegression { .. }
            | Self::DiscardWouldRegressMark { .. }
            | Self::SessionCacheRegression { .. }
            | Self::ImageTooLarge { .. }
            | Self::StoreRequiresReopen
            | Self::InjectedFault { .. } => None,
        }
    }
}
