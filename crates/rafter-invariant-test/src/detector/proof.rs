//! Private challenge channel proving the fixture remained bound to its runner.

#[cfg(unix)]
use std::os::fd::RawFd;

use super::wire::DETECTOR_PROOF_FD_ENV;
#[cfg(unix)]
use super::wire::{DETECTOR_CHALLENGE_BYTES, DETECTOR_PROOF_REQUEST};
#[cfg(unix)]
use nix::{
    errno::Errno,
    sys::socket::{getpeername, recv, send, shutdown, MsgFlags, Shutdown, UnixAddr},
    unistd::close,
};

#[cfg(unix)]
const FIRST_NON_STANDARD_DESCRIPTOR: RawFd = 3;

#[derive(Debug)]
pub(super) struct DetectorProofChannel {
    #[cfg(unix)]
    descriptor: ProofDescriptor,
}

#[cfg(unix)]
#[derive(Debug)]
struct ProofDescriptor(RawFd);

#[cfg(unix)]
impl ProofDescriptor {
    fn claim(descriptor: RawFd) -> Self {
        Self(descriptor)
    }

    fn raw(&self) -> RawFd {
        self.0
    }
}

#[cfg(unix)]
impl Drop for ProofDescriptor {
    fn drop(&mut self) {
        let _ = close(self.0);
    }
}

impl DetectorProofChannel {
    #[cfg(unix)]
    pub(super) fn connect() -> Result<Self, ()> {
        let descriptor = take_descriptor()?;
        let _: UnixAddr = getpeername(descriptor.raw()).map_err(|_| ())?;
        Ok(Self { descriptor })
    }

    #[cfg(not(unix))]
    pub(super) fn connect() -> Result<Self, ()> {
        std::env::remove_var(DETECTOR_PROOF_FD_ENV);
        Err(())
    }

    #[cfg(unix)]
    pub(super) fn challenge(&mut self) -> Option<String> {
        let mut challenge = [0_u8; DETECTOR_CHALLENGE_BYTES];
        let descriptor = self.descriptor.raw();
        let result = send_all(descriptor, &[DETECTOR_PROOF_REQUEST])
            .and_then(|()| recv_exact(descriptor, &mut challenge))
            .map(|()| encode_hex(&challenge));
        let _ = shutdown(descriptor, Shutdown::Both);
        result
    }

    #[cfg(not(unix))]
    pub(super) fn challenge(&mut self) -> Option<String> {
        None
    }
}

#[cfg(unix)]
impl Drop for DetectorProofChannel {
    fn drop(&mut self) {
        let _ = shutdown(self.descriptor.raw(), Shutdown::Both);
    }
}

#[cfg(unix)]
fn take_descriptor() -> Result<ProofDescriptor, ()> {
    let value = std::env::var_os(DETECTOR_PROOF_FD_ENV).ok_or(())?;
    std::env::remove_var(DETECTOR_PROOF_FD_ENV);
    let value = value.into_string().map_err(|_| ())?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(());
    }
    let descriptor = value.parse::<RawFd>().map_err(|_| ())?;
    if descriptor < FIRST_NON_STANDARD_DESCRIPTOR {
        return Err(());
    }
    // The environment contract transfers ownership of any non-standard descriptor it names.
    let descriptor = ProofDescriptor::claim(descriptor);
    if descriptor.raw().to_string() != value {
        return Err(());
    }
    Ok(descriptor)
}

#[cfg(unix)]
fn send_all(descriptor: RawFd, mut bytes: &[u8]) -> Option<()> {
    while !bytes.is_empty() {
        match send(descriptor, bytes, MsgFlags::empty()) {
            Ok(0) => return None,
            Ok(sent) => bytes = &bytes[sent..],
            Err(Errno::EINTR) => {}
            Err(_) => return None,
        }
    }
    Some(())
}

#[cfg(unix)]
fn recv_exact(descriptor: RawFd, mut bytes: &mut [u8]) -> Option<()> {
    while !bytes.is_empty() {
        match recv(descriptor, bytes, MsgFlags::empty()) {
            Ok(0) => return None,
            Ok(received) => bytes = &mut bytes[received..],
            Err(Errno::EINTR) => {}
            Err(_) => return None,
        }
    }
    Some(())
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
