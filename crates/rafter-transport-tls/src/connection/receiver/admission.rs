//! Per-frame group identity, recipient, authorization, and retirement checks.

use rafter_service::AuthenticatedPeerEnvelope;

use crate::directory::InboundRoute;
use crate::{PeerFrame, PeerId, TlsPeerDirectory};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AdmissionRefusal {
    Identity,
    Unauthorized,
    Retired,
    Terminal,
}

pub(super) fn admit_frame<G>(
    directory: &TlsPeerDirectory<G>,
    local_peer: &PeerId,
    authenticated_peer: &PeerId,
    frame: PeerFrame<G>,
) -> Result<AuthenticatedPeerEnvelope<G, PeerId>, AdmissionRefusal>
where
    G: Ord,
{
    match directory
        .inbound_route(
            frame.group_id(),
            local_peer,
            authenticated_peer,
            frame.from(),
            frame.to(),
        )
        .map_err(|_| AdmissionRefusal::Terminal)?
    {
        InboundRoute::Authorized => {}
        InboundRoute::UnknownGroup | InboundRoute::Unauthorized => {
            return Err(AdmissionRefusal::Unauthorized);
        }
        InboundRoute::IdentityMismatch => return Err(AdmissionRefusal::Identity),
        InboundRoute::Retired => return Err(AdmissionRefusal::Retired),
    }

    let (_sequence, group_id, from, to, message) = frame.into_parts();
    Ok(AuthenticatedPeerEnvelope {
        group_id,
        authenticated_peer: authenticated_peer.clone(),
        raft_from: from,
        raft_to: to,
        message,
    })
}
