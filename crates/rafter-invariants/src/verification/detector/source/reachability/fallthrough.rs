//! Fallthrough algebra for recursive detector call-graph expansion.

use super::super::{FunctionFacts, FunctionFallthrough};

#[derive(Clone, Copy)]
pub(super) struct Fallthrough {
    pub(super) may: bool,
    pub(super) guaranteed: bool,
}

impl Fallthrough {
    pub(super) const fn guaranteed() -> Self {
        Self {
            may: true,
            guaranteed: true,
        }
    }

    pub(super) const fn none() -> Self {
        Self {
            may: false,
            guaranteed: false,
        }
    }

    pub(super) const fn conditional() -> Self {
        Self {
            may: true,
            guaranteed: false,
        }
    }

    pub(super) const fn and(self, other: Self) -> Self {
        Self {
            may: self.may && other.may,
            guaranteed: self.guaranteed && other.guaranteed,
        }
    }

    pub(super) const fn from_facts(facts: &FunctionFacts) -> Self {
        match facts.fallthrough {
            FunctionFallthrough::Never => Self {
                may: false,
                guaranteed: false,
            },
            FunctionFallthrough::Conditional => Self {
                may: true,
                guaranteed: false,
            },
            FunctionFallthrough::Guaranteed => Self {
                may: true,
                guaranteed: true,
            },
        }
    }
}
