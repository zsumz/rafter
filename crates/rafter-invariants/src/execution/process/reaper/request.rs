//! Typed quarantine requests and active worker ownership.

use std::{os::unix::net::UnixStream, process::Child};

use super::super::{
    ProcessLeaseState, ProcessLifetimeLease, TargetLeaseState, TargetLifetimeLease,
};

pub(super) struct ChildReapRequest {
    child: Child,
    role: &'static str,
}

impl ChildReapRequest {
    pub(super) fn new(child: Child, role: &'static str) -> Self {
        Self { child, role }
    }

    pub(super) fn child_id(&self) -> u32 {
        self.child.id()
    }

    pub(super) fn role(&self) -> &'static str {
        self.role
    }

    pub(super) fn into_failure(self, detail: String) -> (Child, String) {
        (self.child, detail)
    }
}

pub(super) struct LeasedChildReapRequest {
    child: Child,
    lifetime: ProcessLifetimeLease,
}

impl LeasedChildReapRequest {
    pub(super) fn new(child: Child, lifetime: ProcessLifetimeLease) -> Self {
        Self { child, lifetime }
    }

    pub(super) fn child_id(&self) -> u32 {
        self.child.id()
    }

    pub(super) fn into_failure(self, detail: String) -> (Child, ProcessLifetimeLease, String) {
        (self.child, self.lifetime, detail)
    }
}

pub(super) struct AnchoredGroupReapRequest {
    child: Child,
    control: UnixStream,
    lifetime: TargetLifetimeLease,
}

impl AnchoredGroupReapRequest {
    pub(super) fn new(child: Child, control: UnixStream, lifetime: TargetLifetimeLease) -> Self {
        Self {
            child,
            control,
            lifetime,
        }
    }

    pub(super) fn child_id(&self) -> u32 {
        self.child.id()
    }

    pub(super) fn into_failure(
        self,
        detail: String,
    ) -> (Child, UnixStream, TargetLifetimeLease, String) {
        (self.child, self.control, self.lifetime, detail)
    }
}

#[derive(Debug)]
pub(super) enum ReapRequest {
    Child {
        child: Child,
        role: &'static str,
        retry_error_reported: bool,
    },
    LeasedChild {
        child: Child,
        lifetime: ProcessLifetimeLease,
        retry_error_reported: bool,
        lease_error_reported: bool,
    },
    AnchoredGroup {
        child: Child,
        control: Option<UnixStream>,
        lifetime: TargetLifetimeLease,
        retry_error_reported: bool,
        lease_error_reported: bool,
    },
}

impl From<ChildReapRequest> for ReapRequest {
    fn from(request: ChildReapRequest) -> Self {
        Self::Child {
            child: request.child,
            role: request.role,
            retry_error_reported: false,
        }
    }
}

impl From<LeasedChildReapRequest> for ReapRequest {
    fn from(request: LeasedChildReapRequest) -> Self {
        Self::LeasedChild {
            child: request.child,
            lifetime: request.lifetime,
            retry_error_reported: false,
            lease_error_reported: false,
        }
    }
}

impl From<AnchoredGroupReapRequest> for ReapRequest {
    fn from(request: AnchoredGroupReapRequest) -> Self {
        Self::AnchoredGroup {
            child: request.child,
            control: Some(request.control),
            lifetime: request.lifetime,
            retry_error_reported: false,
            lease_error_reported: false,
        }
    }
}

impl ReapRequest {
    pub(super) fn child_mut(&mut self) -> &mut Child {
        match self {
            Self::Child { child, .. }
            | Self::LeasedChild { child, .. }
            | Self::AnchoredGroup { child, .. } => child,
        }
    }

    pub(super) fn child_id(&self) -> u32 {
        match self {
            Self::Child { child, .. }
            | Self::LeasedChild { child, .. }
            | Self::AnchoredGroup { child, .. } => child.id(),
        }
    }

    pub(super) fn role(&self) -> &'static str {
        match self {
            Self::Child { role, .. } => role,
            Self::LeasedChild { .. } => "internal observer command",
            Self::AnchoredGroup { .. } => "target-group anchor",
        }
    }

    pub(super) fn retry_error_reported(&mut self) -> &mut bool {
        match self {
            Self::Child {
                retry_error_reported,
                ..
            }
            | Self::LeasedChild {
                retry_error_reported,
                ..
            }
            | Self::AnchoredGroup {
                retry_error_reported,
                ..
            } => retry_error_reported,
        }
    }

    pub(super) fn release_lease_if_quiescent(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        match self {
            Self::Child { .. } => Ok(true),
            Self::LeasedChild { lifetime, .. } => match lifetime.observe()? {
                ProcessLeaseState::Held => Ok(false),
                ProcessLeaseState::Released => Ok(true),
            },
            Self::AnchoredGroup {
                control, lifetime, ..
            } => {
                if control.is_none() {
                    return Ok(true);
                }
                match lifetime.observe()? {
                    TargetLeaseState::Held => Ok(false),
                    TargetLeaseState::Released => {
                        control.take();
                        Ok(true)
                    }
                }
            }
        }
    }

    pub(super) fn mark_lease_error_reported(&mut self) -> bool {
        let lease_error_reported = match self {
            Self::Child { .. } => return false,
            Self::LeasedChild {
                lease_error_reported,
                ..
            }
            | Self::AnchoredGroup {
                lease_error_reported,
                ..
            } => lease_error_reported,
        };
        !std::mem::replace(lease_error_reported, true)
    }

    pub(super) fn has_lifetime_lease(&self) -> bool {
        !matches!(self, Self::Child { .. })
    }
}
