//! The deterministic crash seam.
//!
//! A publication's interesting boundaries are byte offsets, not wall-clock
//! moments, so a crash test names the boundary it means to stop at rather than
//! racing for one. [`WriteFault`] is the boundary, [`FaultPlan`] is the
//! schedule, and `WriteFaultSite` is where the write path asks whether the
//! step it is about to take is the one a plan armed.
//!
//! The whole seam is public, and deliberately: the crash suite is a separate
//! crate-level test binary, so nothing narrower would reach it. What keeps it
//! honest is that a plan travels with the handle it was built for.

use std::fmt;

/// A deterministic fault injected into one of the store's publications.
///
/// Each variant names a boundary inside a publication rather than a wall-clock
/// moment, so a crash test reproduces an exact byte offset instead of racing
/// for one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteFault {
    /// Fail before the stale slot is opened.
    ///
    /// The stale slot keeps whatever earlier image it held, which is the case
    /// that proves recovery orders by generation rather than by recency.
    BeforeFirstByte,
    /// Emit the first `bytes` bytes of the unsealed image, make them durable,
    /// then fail.
    ///
    /// The emitted prefix is synced deliberately. The interesting recovery case
    /// is the one where a partial write did reach the medium, and a test that
    /// left the prefix in a write-back cache would be proving something weaker
    /// than it claims. `AfterBytes(0)` writes nothing, so the stale slot keeps
    /// the earlier image it held.
    AfterBytes(u64),
    /// Emit every byte of the unsealed image, then fail its durability barrier.
    AtSlotSync,
    /// Make the whole unsealed image durable, then fail before it is sealed.
    ///
    /// This is the window the seal exists to make representable: every byte of
    /// the new image is on the medium and none of it counts.
    BeforeSeal,
    /// Seal the image, then fail the barrier that makes the seal durable.
    ///
    /// Either outcome is legal — the seal may or may not have reached the
    /// medium — which is exactly why the caller is told the result is unknown.
    AtSealSync,
}

impl fmt::Display for WriteFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeFirstByte => formatter.write_str("failure before the first byte"),
            Self::AfterBytes(bytes) => write!(formatter, "failure after {bytes} bytes"),
            Self::AtSlotSync => formatter.write_str("failure at the slot sync"),
            Self::BeforeSeal => formatter.write_str("failure before the seal"),
            Self::AtSealSync => formatter.write_str("failure at the seal sync"),
        }
    }
}

/// Deterministic fault schedule attached to one store instance.
///
/// Injection is part of a store's construction rather than a process-wide
/// switch: a plan travels with the handle it was built for, so two stores in
/// one test — or two tests in one process — cannot observe each other's faults.
///
/// Plans are addressed by publication ordinal. Every publication the store
/// performs, whether an apply commit or a snapshot install, consumes the next
/// ordinal starting at one, so a scenario names the exact transaction it means
/// to interrupt.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPlan {
    faults: Vec<(u64, WriteFault)>,
}

impl FaultPlan {
    /// A plan that injects nothing.
    #[must_use]
    pub const fn none() -> Self {
        Self { faults: Vec::new() }
    }

    /// A plan that injects `fault` on the `publication`-th publication.
    #[must_use]
    pub fn at(publication: u64, fault: WriteFault) -> Self {
        Self::none().and(publication, fault)
    }

    /// Adds another injection to this plan.
    #[must_use]
    pub fn and(mut self, publication: u64, fault: WriteFault) -> Self {
        self.faults.push((publication, fault));
        self
    }

    pub(super) fn fault_for(&self, publication: u64) -> Option<WriteFault> {
        self.faults
            .iter()
            .find(|(ordinal, _)| *ordinal == publication)
            .map(|(_, fault)| *fault)
    }
}

impl fmt::Display for FaultPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.faults.is_empty() {
            return formatter.write_str("no injected faults");
        }
        let mut separator = "";
        for (publication, fault) in &self.faults {
            write!(formatter, "{separator}publication {publication}: {fault}")?;
            separator = ", ";
        }
        Ok(())
    }
}

/// The step a fault is armed at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WriteFaultSite {
    BeforeFirstByte,
    AfterBytes,
    AtSlotSync,
    BeforeSeal,
    AtSealSync,
}

impl WriteFaultSite {
    pub(super) const fn matches(self, fault: WriteFault) -> bool {
        matches!(
            (self, fault),
            (Self::BeforeFirstByte, WriteFault::BeforeFirstByte)
                | (Self::AfterBytes, WriteFault::AfterBytes(_))
                | (Self::AtSlotSync, WriteFault::AtSlotSync)
                | (Self::BeforeSeal, WriteFault::BeforeSeal)
                | (Self::AtSealSync, WriteFault::AtSealSync)
        )
    }
}
