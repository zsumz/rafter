//! Private detector challenge transport constants and byte-level encoding.

use std::path::Path;

use super::ChallengeProtocol;

pub(super) const PROOF_SOCKET_ENV: &str = "RAFTER_INVARIANT_DETECTOR_PROOF_SOCKET";
pub(super) const PROOF_SOCKET_DIRECTORY: &str = "target/rafter-invariants/tmp/detector-proof";
pub(super) const CHALLENGE_BYTES: usize = 32;
pub(super) const SOCKET_NONCE_BYTES: usize = 16;
pub(super) const PROOF_REQUEST: u8 = 0xa7;

pub(super) fn protocol() -> ChallengeProtocol {
    ChallengeProtocol {
        socket_environment: PROOF_SOCKET_ENV,
        socket_directory: PROOF_SOCKET_DIRECTORY,
        challenge_bytes: CHALLENGE_BYTES,
        socket_nonce_bytes: SOCKET_NONCE_BYTES,
        proof_request: PROOF_REQUEST,
        zero_challenge_encoding: encode_challenge(&[0; CHALLENGE_BYTES]),
        zero_socket_nonce_encoding: encode_socket_nonce(&[0; SOCKET_NONCE_BYTES]),
    }
}

pub(super) fn encode_challenge(challenge: &[u8; CHALLENGE_BYTES]) -> String {
    encode_hex(challenge)
}

pub(super) fn encode_socket_nonce(nonce: &[u8; SOCKET_NONCE_BYTES]) -> String {
    encode_hex(nonce)
}

pub(super) fn managed_socket_path(path: &Path) -> bool {
    if path.is_absolute() || path.parent() != Some(Path::new(PROOF_SOCKET_DIRECTORY)) {
        return false;
    }
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let Some(stem) = file_name.strip_suffix(".sock") else {
        return false;
    };
    let mut fields = stem.split('-');
    let (Some(pid), Some(sequence), Some(nonce), None) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return false;
    };
    !pid.is_empty()
        && pid.bytes().all(|byte| byte.is_ascii_digit())
        && !sequence.is_empty()
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && nonce.len() == SOCKET_NONCE_BYTES * 2
        && nonce
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
