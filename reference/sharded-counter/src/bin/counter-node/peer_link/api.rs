//! Process-facing frame and admission-error vocabulary.

use std::{error::Error, fmt};

use rafter::{Message, NodeId};
use rafter_reference_sharded_counter::{GroupId, GroupIncarnation};
use rafter_transport_tls::TlsTransportError;

/// One peer frame in the counter consumer's lifecycle vocabulary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerFrame {
    pub group_id: GroupId,
    pub incarnation: GroupIncarnation,
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

/// Typed synchronous admission refusal from the public transport.
#[derive(Debug)]
pub struct LinkError {
    source: TlsTransportError,
}

impl LinkError {
    pub(super) const fn new(source: TlsTransportError) -> Self {
        Self { source }
    }
}

impl fmt::Display for LinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "authenticated peer transport refused the frame: {}",
            self.source
        )
    }
}

impl Error for LinkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
