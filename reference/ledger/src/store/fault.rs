//! The deterministic crash seam.
//!
//! A write plan's interesting boundaries are byte offsets and rename points,
//! not wall-clock moments, so a crash test names the boundary it means to stop
//! at rather than racing for one. [`WriteFault`] is the boundary, [`FaultPlan`]
//! is the schedule, and `WriteFaultSite` is where a write path asks whether the
//! step it is about to take is the one a plan armed.
//!
//! The whole seam is public, and deliberately: the crash suite is a separate
//! crate-level test binary, so nothing narrower would reach it. What keeps it
//! honest is that a plan travels with the handle it was built for.

use std::fmt;

/// A deterministic fault injected into one of the store's write plans.
///
/// Each variant names a boundary inside a publication rather than a wall-clock
/// moment, so a crash test reproduces an exact byte offset instead of racing
/// for one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteFault {
    /// Fail before the plan emits its first byte.
    BeforeFirstByte,
    /// Emit the first `bytes` bytes of the plan, make them durable, then fail.
    ///
    /// The emitted prefix is synced deliberately. The interesting recovery case
    /// is the one where a partial write did reach the medium, and a test that
    /// left the prefix in a write-back cache would be proving something weaker
    /// than it claims.
    AfterBytes(u64),
    /// Emit every byte of the plan, then fail its file durability barrier.
    AtFileSync,
    /// Make the whole unsealed frame durable, then fail before it is sealed.
    ///
    /// This is the write-ahead window the journal exists to make
    /// representable: every byte of the transaction is on the medium and none
    /// of it counts. Only an append seals, so this never fires on a rewrite.
    BeforeSeal,
    /// Seal the frame, then fail the barrier that makes the seal durable.
    ///
    /// Either outcome is legal — the seal may or may not have reached the
    /// medium — which is exactly why the caller is told the result is unknown.
    /// Only an append seals, so this never fires on a rewrite.
    AtSealSync,
    /// Emit and sync the staged file, then fail before the rename publishes it.
    ///
    /// Only a rewrite renames, so this never fires on an append.
    BeforeRename,
    /// Rename the staged file, then fail before the directory entry is durable.
    ///
    /// Only a rewrite renames, so this never fires on an append.
    AfterRename,
}

impl fmt::Display for WriteFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeFirstByte => formatter.write_str("failure before the first byte"),
            Self::AfterBytes(bytes) => write!(formatter, "failure after {bytes} bytes"),
            Self::AtFileSync => formatter.write_str("failure at the file sync"),
            Self::BeforeSeal => formatter.write_str("failure before the seal"),
            Self::AtSealSync => formatter.write_str("failure at the seal sync"),
            Self::BeforeRename => formatter.write_str("failure before the rename"),
            Self::AfterRename => formatter.write_str("failure after the rename"),
        }
    }
}

/// Deterministic fault schedule attached to one store instance.
///
/// Injection is part of a store's construction rather than a process-wide
/// switch: a plan travels with the handle it was built for, so two stores in
/// one test — or two tests in one process — cannot observe each other's faults.
///
/// Plans are addressed by write-plan ordinal. Every publication the store
/// performs, whether an append or a rewrite, consumes the next ordinal starting
/// at one, so a scenario names the exact transaction it means to interrupt.
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

    /// A plan that injects `fault` on the `plan`-th write plan.
    #[must_use]
    pub fn at(plan: u64, fault: WriteFault) -> Self {
        Self::none().and(plan, fault)
    }

    /// Adds another injection to this plan.
    #[must_use]
    pub fn and(mut self, plan: u64, fault: WriteFault) -> Self {
        self.faults.push((plan, fault));
        self
    }

    pub(super) fn fault_for(&self, plan: u64) -> Option<WriteFault> {
        self.faults
            .iter()
            .find(|(ordinal, _)| *ordinal == plan)
            .map(|(_, fault)| *fault)
    }
}

impl fmt::Display for FaultPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.faults.is_empty() {
            return formatter.write_str("no injected faults");
        }
        let mut separator = "";
        for (plan, fault) in &self.faults {
            write!(formatter, "{separator}plan {plan}: {fault}")?;
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
    AtFileSync,
    BeforeSeal,
    AtSealSync,
    BeforeRename,
    AfterRename,
}

impl WriteFaultSite {
    pub(super) const fn matches(self, fault: WriteFault) -> bool {
        matches!(
            (self, fault),
            (Self::BeforeFirstByte, WriteFault::BeforeFirstByte)
                | (Self::AfterBytes, WriteFault::AfterBytes(_))
                | (Self::AtFileSync, WriteFault::AtFileSync)
                | (Self::BeforeSeal, WriteFault::BeforeSeal)
                | (Self::AtSealSync, WriteFault::AtSealSync)
                | (Self::BeforeRename, WriteFault::BeforeRename)
                | (Self::AfterRename, WriteFault::AfterRename)
        )
    }
}
