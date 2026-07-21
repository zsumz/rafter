//! Private detector challenge descriptor contract and byte-level encoding.

use super::ChallengeProtocol;

pub(super) const PROOF_DESCRIPTOR_ENV: &str = "RAFTER_INVARIANT_DETECTOR_PROOF_FD";
pub(super) const CHALLENGE_BYTES: usize = 32;
pub(super) const PROOF_REQUEST: u8 = 0xa7;

pub(super) fn protocol() -> ChallengeProtocol {
    ChallengeProtocol {
        descriptor_environment: PROOF_DESCRIPTOR_ENV,
        challenge_bytes: CHALLENGE_BYTES,
        proof_request: PROOF_REQUEST,
        zero_challenge_encoding: encode_challenge(&[0; CHALLENGE_BYTES]),
    }
}

pub(super) fn encode_challenge(challenge: &[u8; CHALLENGE_BYTES]) -> String {
    encode_hex(challenge)
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
