//! Typed detector evidence accumulated during one fixture invocation.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DetectorWitness {
    kind: WitnessKind,
    identity: DetectorIdentity,
}

impl DetectorWitness {
    pub(super) const fn expected_rejection(identity: &'static str) -> Self {
        Self {
            kind: WitnessKind::ExpectedRejection,
            identity: DetectorIdentity::new(identity),
        }
    }

    pub(super) const fn recorder_invocation(identity: &'static str) -> Self {
        Self {
            kind: WitnessKind::RecorderInvocation,
            identity: DetectorIdentity::new(identity),
        }
    }

    pub(super) const fn is_expected_rejection(self) -> bool {
        matches!(self.kind, WitnessKind::ExpectedRejection)
    }

    pub(super) const fn kind(self) -> &'static str {
        self.kind.as_wire_name()
    }

    pub(super) const fn identity(self) -> &'static str {
        self.identity.as_wire_name()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DetectorIdentity(&'static str);

impl DetectorIdentity {
    const fn new(identity: &'static str) -> Self {
        Self(identity)
    }

    const fn as_wire_name(self) -> &'static str {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WitnessKind {
    ExpectedRejection,
    RecorderInvocation,
}

impl WitnessKind {
    const fn as_wire_name(self) -> &'static str {
        match self {
            Self::ExpectedRejection => "expect-err",
            Self::RecorderInvocation => "recorder",
        }
    }
}
