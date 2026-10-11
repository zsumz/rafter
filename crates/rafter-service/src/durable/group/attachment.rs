//! Failed attachment preserves both resources for retry or explicit disposal.
use rafter_runtime::service::ServiceState;

/// A backend cannot yet supply the initial managed metrics observation.
/// Neither the owner nor the configured transport is discarded on refusal.
pub struct DurableDriverAttachError<O, Tr> {
    owner: Box<O>,
    transport: Tr,
    state: ServiceState,
}
impl<O, Tr> DurableDriverAttachError<O, Tr> {
    pub(super) fn new(owner: O, transport: Tr, state: ServiceState) -> Self {
        Self {
            owner: Box::new(owner),
            transport,
            state,
        }
    }
    /// Backend state observed at the refused attachment.
    #[must_use]
    pub const fn state(&self) -> ServiceState {
        self.state
    }
    /// Returns the unchanged owner and transport for retry or disposal.
    #[must_use]
    pub fn into_parts(self) -> (O, Tr) {
        (*self.owner, self.transport)
    }
}
impl<O, Tr> std::fmt::Debug for DurableDriverAttachError<O, Tr> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableDriverAttachError")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
impl<O, Tr> std::fmt::Display for DurableDriverAttachError<O, Tr> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "durable driver attachment requires observable live state; current state: {:?}",
            self.state
        )
    }
}
impl<O, Tr> std::error::Error for DurableDriverAttachError<O, Tr> {}
