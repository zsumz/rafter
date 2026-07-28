//! What this driver tells its link layer about who may speak.
//!
//! Split from [`super::state`] along the line that file's own header draws: that
//! one answers "what does a step do", and this concern answers "who is allowed to
//! send one". Everything between a committed configuration and the one statement
//! the transport is owed for it — a [`crate::transport::PeerPolicy`], which is
//! the authorized principals beside the retirement floor — is here, and the step
//! loop reaches it through one call.
//!
//! It used to be two statements: a peer set, plus a permanent per-principal fence
//! per committed removal, owed until the link layer accepted it. That second one
//! was an *operation* rather than a derivation, so the driver had to remember
//! which removals it had already acted on — and it answered "has this fence been
//! made" with the same bit that answers "may this identity be admitted again".
//! Publishing a floor makes retirement a function of state the driver still
//! holds, which is what deletes the ledger and everything that could go wrong
//! inside it.
//!
//! # The four files, and the seams between them
//!
//! This one is the facade: it declares the parts and states how they compose, and
//! holds no rule of its own. That is the whole of the eleventh round's
//! maintainability change — the concern had grown into one file where a fact, a
//! transaction, and a derivation were interleaved, and a reader checking any one
//! of them had to read all three.
//!
//! * [`super::observation`] — **what a fact says.** One membership event in, one
//!   [`super::observation::CommittedObservation`] or effective set out. Total,
//!   side-effect free, and reads no driver state, so the one place a new
//!   `MembershipEvent` variant can be missed is a match with nothing else in it.
//! * [`super::reconciliation`] — **what a batch of facts does.** The staged
//!   transaction: clone the membership fields into a candidate, fold every fact
//!   of one batch into it, install only a candidate that survived all of them,
//!   and state the result exactly once. Adoption, one live report, a recovery
//!   replay, and the error-path reconciliation are four callers of one rule.
//! * [`super::policy`] — **what the result licenses.** The authorized set, the
//!   retirement floor, the inbound admission check, and this replica's own
//!   service state. Reads state and writes only the record of what the link layer
//!   accepted.
//! * [`super::checkpoint`] — **what survives a crash.** The durable record, what
//!   makes one valid, the one merge every pair of observations goes through, and
//!   the chain rule that decides which records may be joined at all.
//!
//! The join in `checkpoint` and the fold in `reconciliation` are the same algebra
//! reached from two directions — one merges two records, the other merges a
//! record and a fact — so the rules that make either safe are stated once, there,
//! and read here.
//!
//! # Where the rules are pinned
//!
//! Every rule is pinned from outside the crate, through `deliver`, `tick`, and
//! adoption — `tests/transport_membership.rs` for what the event stream does to
//! the peer set, `tests/transport_identity.rs` for what a committed removal does
//! to an identity, `tests/transport_authorization.rs` for what a record standing
//! ahead of the runtime authorizes, and
//! `tests/transport_membership_transaction.rs` for what a refused batch leaves
//! behind. This concern used to carry its own test file whose header explained
//! that the widening branch had no public entry point, and that was true and was
//! the defect: the app layer reported an effective change only for a step
//! carrying a local membership request, so no follower could reach the branch its
//! tests were passing. Scripting the router directly also let those fixtures
//! state membership sequences no correct cluster produces, and two of them did.
