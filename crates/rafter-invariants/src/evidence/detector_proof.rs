//! Neutral detector-proof wire decoding across the evidence trust boundary.

use std::path::Path;

pub(crate) const PROOF_SOCKET_ENV: &str = "RAFTER_INVARIANT_DETECTOR_PROOF_SOCKET";
pub(crate) const PROOF_SOCKET_DIRECTORY: &str = "target/rafter-invariants/tmp/detector-proof";
pub(crate) const PROOF_PREFIX: &str = "RAFTER_INVARIANT_DETECTOR_PROOF:";
pub(crate) const WITNESS_PREFIX: &str = "RAFTER_INVARIANT_DETECTOR_WITNESS:";
pub(crate) const CHALLENGE_BYTES: usize = 32;
pub(crate) const SOCKET_NONCE_BYTES: usize = 16;
pub(crate) const PROOF_REQUEST: u8 = 0xa7;

pub(crate) fn encode_challenge(challenge: &[u8; CHALLENGE_BYTES]) -> String {
    encode_hex(challenge)
}

pub(crate) fn validate_challenge(challenge: &str) -> Result<(), String> {
    decode_challenge(challenge).map(|_| ())
}

pub(crate) fn encode_socket_nonce(nonce: &[u8; SOCKET_NONCE_BYTES]) -> String {
    encode_hex(nonce)
}

pub(crate) fn managed_socket_path(path: &Path) -> bool {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TranscriptRecord {
    Witness {
        token: String,
        witness: String,
    },
    Proof {
        token: String,
        witness: String,
        challenge: String,
    },
}

pub(crate) fn decode_transcript(
    stdout: &str,
    stderr: &str,
) -> Result<Vec<TranscriptRecord>, String> {
    let mut records = Vec::new();
    for line in stdout.lines().chain(stderr.lines()).map(str::trim) {
        if let Some(encoded) = line.strip_prefix(WITNESS_PREFIX) {
            let (token, witness) = parse_bound_witness(encoded)?;
            records.push(TranscriptRecord::Witness { token, witness });
        } else if let Some(encoded) = line.strip_prefix(PROOF_PREFIX) {
            let (bound_witness, challenge) = encoded
                .rsplit_once(':')
                .ok_or_else(|| "detector proof omitted its challenge".to_owned())?;
            validate_challenge(challenge)?;
            let (token, witness) = parse_bound_witness(bound_witness)?;
            records.push(TranscriptRecord::Proof {
                token,
                witness,
                challenge: challenge.to_owned(),
            });
        }
    }
    Ok(records)
}

fn parse_bound_witness(encoded: &str) -> Result<(String, String), String> {
    let (token, witness) = encoded
        .split_once(':')
        .ok_or_else(|| "detector marker omitted its execution token".to_owned())?;
    if token.is_empty() {
        return Err("detector marker uses an empty execution token".to_owned());
    }
    Ok((token.to_owned(), parse_witness(witness)?))
}

fn parse_witness(witness: &str) -> Result<String, String> {
    let witness = witness
        .strip_suffix("()")
        .ok_or_else(|| "detector witness omitted its call suffix".to_owned())?;
    let (kind, identity) = witness
        .split_once(':')
        .ok_or_else(|| "detector witness omitted its kind".to_owned())?;
    if !matches!(kind, "expect-err" | "recorder") || !valid_identity(identity) {
        return Err(format!("detector witness is malformed: {witness}"));
    }
    Ok(witness.to_owned())
}

fn valid_identity(identity: &str) -> bool {
    let mut segments = identity.split("::");
    segments.next().is_some_and(valid_identifier) && segments.all(valid_identifier)
}

fn valid_identifier(identifier: &str) -> bool {
    !identifier.is_empty()
        && identifier.chars().enumerate().all(|(index, character)| {
            character == '_'
                || character.is_ascii_alphanumeric() && (index > 0 || !character.is_ascii_digit())
        })
}

fn decode_challenge(encoded: &str) -> Result<[u8; CHALLENGE_BYTES], String> {
    let bytes = decode_hex(encoded)
        .ok_or_else(|| "detector challenge is not lowercase hexadecimal".to_owned())?;
    bytes
        .try_into()
        .map_err(|_| "detector challenge has the wrong length".to_owned())
}

fn decode_hex(encoded: &str) -> Option<Vec<u8>> {
    if !encoded.len().is_multiple_of(2)
        || encoded
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return None;
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            u8::try_from((high << 4) | low).ok()
        })
        .collect()
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

#[cfg(test)]
mod tests;
