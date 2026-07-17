use std::collections::BTreeSet;

use super::CallTarget;

#[derive(Default)]
pub(super) struct FunctionFacts {
    pub(super) detector_test_attributes: usize,
    pub(super) conditional_compilation: bool,
    pub(super) untrusted_attributes: bool,
    pub(super) shadowed_values: BTreeSet<String>,
    pub(super) potential_callable_arguments: Vec<CallTarget>,
    pub(super) events: Vec<FunctionEvent>,
    pub(super) guaranteed_called_parameters: BTreeSet<usize>,
    pub(super) conditional_called_parameters: BTreeSet<usize>,
    pub(super) fallthrough: FunctionFallthrough,
    pub(super) defects: BTreeSet<SourceDefect>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum FunctionFallthrough {
    #[default]
    Never,
    Conditional,
    Guaranteed,
}

impl FunctionFallthrough {
    pub(super) const fn from_analysis(falls_through: bool, guaranteed: bool) -> Self {
        match (falls_through, guaranteed) {
            (false, _) => Self::Never,
            (true, false) => Self::Conditional,
            (true, true) => Self::Guaranteed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum FunctionEvent {
    Call {
        call: FunctionCall,
        guaranteed: bool,
    },
    Invocation {
        invocation: InvocationCall,
        guaranteed: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FunctionCall {
    pub(super) target: CallTarget,
    pub(super) arguments: Vec<CallableArgument>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum CallableArgument {
    Known(CallTarget),
    InlineClosure,
    Parameter(usize),
    Opaque,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum SourceDefect {
    ForbiddenWitness,
    MalformedInvocationMacro,
    OpaqueCallable,
    OpaqueMacro,
    UntrustedOracleMacro,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InvocationKind {
    ExpectErr,
    Recorder,
}

impl InvocationKind {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::ExpectErr => "expect-err",
            Self::Recorder => "recorder",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct InvocationCall {
    pub(super) kind: InvocationKind,
    pub(super) target: CallTarget,
}
