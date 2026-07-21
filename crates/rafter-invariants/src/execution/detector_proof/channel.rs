//! Private inherited descriptor allocation and one-shot challenge ownership.

use std::{
    fs::File,
    io::Read,
    os::{
        fd::{AsFd, BorrowedFd},
        unix::net::UnixStream,
    },
};

use super::{
    responder::ChallengeResponder, wire, ChallengeExchange, ChallengeProtocol, DetectorChallenge,
    TransportError,
};

/// A one-shot detector challenge endpoint carried by an inherited descriptor.
pub(crate) struct ChallengeGate {
    child: UnixStream,
    challenge: DetectorChallenge,
    responder: ChallengeResponder,
}

impl ChallengeGate {
    pub(crate) fn open() -> Result<Self, TransportError> {
        let challenge = random_challenge()?;
        let (parent, child) = UnixStream::pair()
            .map_err(|error| TransportError::context("create detector proof channel", error))?;
        let responder = ChallengeResponder::start(parent, challenge.clone())?;
        Ok(Self {
            child,
            challenge,
            responder,
        })
    }

    pub(crate) fn child_descriptor(&self) -> BorrowedFd<'_> {
        self.child.as_fd()
    }

    pub(crate) fn protocol() -> ChallengeProtocol {
        wire::protocol()
    }

    pub(crate) fn challenge(&self) -> &DetectorChallenge {
        &self.challenge
    }

    pub(crate) fn finish(self) -> ChallengeExchange {
        let Self {
            child, responder, ..
        } = self;
        drop(child);
        responder.finish()
    }
}

fn random_challenge() -> Result<DetectorChallenge, TransportError> {
    let mut challenge = [0_u8; wire::CHALLENGE_BYTES];
    let mut random = File::open("/dev/urandom")
        .map_err(|error| TransportError::context("open operating-system randomness", error))?;
    random
        .read_exact(&mut challenge)
        .map_err(|error| TransportError::context("read detector challenge randomness", error))?;
    Ok(DetectorChallenge::new(challenge))
}
