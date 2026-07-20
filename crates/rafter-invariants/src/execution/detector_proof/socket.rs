//! Managed Unix socket allocation, confinement, stale cleanup, and ownership.

use std::{
    fs::{self, File},
    io::Read,
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::UnixListener,
    },
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

use super::{
    responder::ChallengeResponder, wire, ChallengeExchange, ChallengeProtocol, DetectorChallenge,
    TransportError,
};

const STALE_AFTER: Duration = Duration::from_secs(24 * 60 * 60);
static NEXT_SOCKET: AtomicU64 = AtomicU64::new(0);

/// A one-shot detector challenge endpoint with managed cleanup.
pub(crate) struct ChallengeGate {
    socket: PathBuf,
    challenge: DetectorChallenge,
    responder: Option<ChallengeResponder>,
}

impl ChallengeGate {
    pub(crate) fn open() -> Result<Self, TransportError> {
        let (challenge, socket_nonce) = random_material()?;
        let directory = Path::new(wire::PROOF_SOCKET_DIRECTORY);
        prepare_socket_directory(directory)?;
        let socket = managed_socket_path(directory, &socket_nonce)?;
        let listener = UnixListener::bind(&socket)
            .map_err(|error| TransportError::context("bind detector proof socket", error))?;
        if let Err(error) = fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)) {
            let _ = fs::remove_file(&socket);
            return Err(TransportError::context(
                "set detector proof socket permissions",
                error,
            ));
        }
        let responder = match ChallengeResponder::start(listener, socket.clone(), challenge.clone())
        {
            Ok(responder) => responder,
            Err(error) => {
                let _ = fs::remove_file(&socket);
                return Err(error);
            }
        };
        Ok(Self {
            socket,
            challenge,
            responder: Some(responder),
        })
    }

    pub(crate) fn socket_path(&self) -> &Path {
        &self.socket
    }

    pub(crate) fn protocol() -> ChallengeProtocol {
        wire::protocol()
    }

    pub(crate) fn challenge(&self) -> &DetectorChallenge {
        &self.challenge
    }

    pub(crate) fn finish(mut self) -> ChallengeExchange {
        self.responder
            .take()
            .expect("challenge gate always owns its responder")
            .finish()
    }
}

fn random_material() -> Result<(DetectorChallenge, [u8; wire::SOCKET_NONCE_BYTES]), TransportError>
{
    let mut challenge = [0_u8; wire::CHALLENGE_BYTES];
    let mut socket_nonce = [0_u8; wire::SOCKET_NONCE_BYTES];
    let mut random = File::open("/dev/urandom")
        .map_err(|error| TransportError::context("open operating-system randomness", error))?;
    random
        .read_exact(&mut challenge)
        .map_err(|error| TransportError::context("read detector challenge randomness", error))?;
    random
        .read_exact(&mut socket_nonce)
        .map_err(|error| TransportError::context("read detector socket randomness", error))?;
    Ok((DetectorChallenge::new(challenge), socket_nonce))
}

fn managed_socket_path(
    directory: &Path,
    nonce: &[u8; wire::SOCKET_NONCE_BYTES],
) -> Result<PathBuf, TransportError> {
    let socket = directory.join(format!(
        "{}-{}-{}.sock",
        std::process::id(),
        NEXT_SOCKET.fetch_add(1, Ordering::Relaxed),
        wire::encode_socket_nonce(nonce),
    ));
    if !wire::managed_socket_path(&socket) {
        return Err(TransportError::new(
            "constructed detector proof socket has an invalid path",
        ));
    }
    Ok(socket)
}

fn prepare_socket_directory(directory: &Path) -> Result<(), TransportError> {
    fs::create_dir_all(directory)
        .map_err(|error| TransportError::context("create detector proof directory", error))?;
    let mut current = PathBuf::new();
    for component in directory.components() {
        current.push(component);
        let metadata = fs::symlink_metadata(&current).map_err(|error| {
            TransportError::context("inspect detector proof directory component", error)
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(TransportError::new(format!(
                "detector proof directory component {} is not a normal directory",
                current.display()
            )));
        }
    }
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).map_err(|error| {
        TransportError::context("set detector proof directory permissions", error)
    })?;
    prune_stale_sockets(directory)
}

fn prune_stale_sockets(directory: &Path) -> Result<(), TransportError> {
    for entry in fs::read_dir(directory)
        .map_err(|error| TransportError::context("read detector proof directory", error))?
    {
        let entry =
            entry.map_err(|error| TransportError::context("read detector proof entry", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| TransportError::context("inspect detector proof entry", error))?;
        if !file_type.is_socket() || !wire::managed_socket_path(&entry.path()) {
            continue;
        }
        let Ok(modified) = entry.metadata().and_then(|metadata| metadata.modified()) else {
            continue;
        };
        if stale_at(modified, SystemTime::now()) {
            fs::remove_file(entry.path()).map_err(|error| {
                TransportError::context("remove stale detector proof socket", error)
            })?;
        }
    }
    Ok(())
}

fn stale_at(modified: SystemTime, now: SystemTime) -> bool {
    now.duration_since(modified)
        .is_ok_and(|age| age >= STALE_AFTER)
}

#[cfg(test)]
mod tests;
