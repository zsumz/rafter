//! Private challenge channel proving the fixture remained bound to its runner.

#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::UnixStream;

use super::wire::DETECTOR_PROOF_SOCKET_ENV;
#[cfg(unix)]
use super::wire::{DETECTOR_CHALLENGE_BYTES, DETECTOR_PROOF_REQUEST};

#[derive(Debug)]
pub(super) struct DetectorProofChannel {
    #[cfg(unix)]
    stream: UnixStream,
}

impl DetectorProofChannel {
    #[cfg(unix)]
    pub(super) fn connect() -> Result<Option<Self>, ()> {
        let socket = match std::env::var(DETECTOR_PROOF_SOCKET_ENV) {
            Ok(socket) => socket,
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => return Err(()),
        };
        UnixStream::connect(&socket)
            .map(|stream| {
                std::env::remove_var(DETECTOR_PROOF_SOCKET_ENV);
                Some(Self { stream })
            })
            .map_err(|_| ())
    }

    #[cfg(not(unix))]
    pub(super) fn connect() -> Result<Option<Self>, ()> {
        match std::env::var(DETECTOR_PROOF_SOCKET_ENV) {
            Err(std::env::VarError::NotPresent) => Ok(None),
            Ok(_) | Err(std::env::VarError::NotUnicode(_)) => Err(()),
        }
    }

    #[cfg(unix)]
    pub(super) fn challenge(&mut self) -> Option<String> {
        self.stream.write_all(&[DETECTOR_PROOF_REQUEST]).ok()?;
        self.stream.flush().ok()?;
        let mut challenge = [0_u8; DETECTOR_CHALLENGE_BYTES];
        self.stream.read_exact(&mut challenge).ok()?;
        Some(encode_hex(&challenge))
    }

    #[cfg(not(unix))]
    pub(super) fn challenge(&mut self) -> Option<String> {
        None
    }
}

#[cfg(unix)]
fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
