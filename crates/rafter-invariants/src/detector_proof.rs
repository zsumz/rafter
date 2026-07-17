use std::{collections::BTreeMap, path::Path};

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

pub(crate) fn verify_transcript(
    stdout: &str,
    stderr: &str,
    token: &str,
    expected_challenge: &str,
) -> Result<BTreeMap<String, usize>, String> {
    decode_challenge(expected_challenge)?;
    let witness_prefix = format!("{WITNESS_PREFIX}{token}:");
    let proof_prefix = format!("{PROOF_PREFIX}{token}:");
    let mut witnesses = BTreeMap::<String, usize>::new();
    let mut proofs = BTreeMap::<String, usize>::new();

    for line in stdout.lines().chain(stderr.lines()).map(str::trim) {
        if let Some(witness) = line.strip_prefix(&witness_prefix) {
            let witness = parse_witness(witness)?;
            *witnesses.entry(witness).or_default() += 1;
        } else if line.starts_with(WITNESS_PREFIX) {
            return Err("detector witness is bound to another execution token".to_owned());
        }

        if let Some(proof) = line.strip_prefix(&proof_prefix) {
            let (witness, challenge) = proof
                .rsplit_once(':')
                .ok_or_else(|| "detector proof omitted its challenge".to_owned())?;
            let witness = parse_witness(witness)?;
            if challenge != expected_challenge {
                return Err("detector proof used the wrong post-invocation challenge".to_owned());
            }
            *proofs.entry(witness).or_default() += 1;
        } else if line.starts_with(PROOF_PREFIX) {
            return Err("detector proof is bound to another execution token".to_owned());
        }
    }

    if witnesses.is_empty() {
        return Err("detector transcript contains no runtime witnesses".to_owned());
    }
    if witnesses != proofs {
        return Err(format!(
            "detector witness and proof inventories differ: witnesses={witnesses:?}, proofs={proofs:?}"
        ));
    }
    Ok(witnesses)
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
mod tests {
    use super::*;

    #[test]
    fn transcript_requires_the_parent_issued_post_invocation_challenge() {
        let challenge = encode_challenge(&[0x5a; CHALLENGE_BYTES]);
        let token = "token";
        let witness = "expect-err:crate_name::detector";
        let stderr = format!(
            "{WITNESS_PREFIX}{token}:{witness}()\n{PROOF_PREFIX}{token}:{witness}():{challenge}\n"
        );
        assert_eq!(
            verify_transcript("", &stderr, token, &challenge).expect("valid transcript"),
            BTreeMap::from([(witness.to_owned(), 1)])
        );
        assert!(verify_transcript("", &stderr, token, &"0".repeat(64)).is_err());
    }

    #[test]
    fn proof_socket_path_has_one_exact_managed_shape() {
        assert!(managed_socket_path(Path::new(
            "target/rafter-invariants/tmp/detector-proof/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.sock"
        )));
        for path in [
            "/target/rafter-invariants/tmp/detector-proof/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.sock",
            "target/rafter-invariants/tmp/detector-proof/nested/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.sock",
            "target/rafter-invariants/tmp/detector-proof/12-3.sock",
            "target/rafter-invariants/tmp/detector-proof/12-3-5A5A5A5A5A5A5A5A5A5A5A5A5A5A5A5A.sock",
            "target/rafter-invariants/tmp/detector-proof/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.txt",
        ] {
            assert!(!managed_socket_path(Path::new(path)), "accepted {path}");
        }
    }
}
