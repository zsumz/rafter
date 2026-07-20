//! Function identities and conservative call-target candidates.

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::verification::detector::source) struct FunctionId {
    pub(in crate::verification::detector::source) module: Vec<String>,
    pub(in crate::verification::detector::source) name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::verification::detector::source) struct CallTarget {
    pub(super) candidates: Vec<FunctionId>,
    pub(in crate::verification::detector::source) name: String,
    pub(in crate::verification::detector::source) opaque_local_module: bool,
    pub(in crate::verification::detector::source) imprecise_dispatch: bool,
}
