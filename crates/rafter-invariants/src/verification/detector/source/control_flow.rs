//! Reachability state used while traversing fixture control flow.

#[derive(Clone, Copy)]
pub(super) enum PathReachability {
    Reachable,
    Unreachable,
}

impl PathReachability {
    pub(super) const fn is_reachable(self) -> bool {
        matches!(self, Self::Reachable)
    }
}

impl From<bool> for PathReachability {
    fn from(reachable: bool) -> Self {
        if reachable {
            Self::Reachable
        } else {
            Self::Unreachable
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct StatementState {
    pub(super) may_exit: bool,
    pub(super) may_diverge: bool,
}

#[derive(Clone, Copy, Default)]
pub(super) struct FunctionState {
    pub(super) may_diverge: bool,
    pub(super) normal_return_seen: bool,
}
