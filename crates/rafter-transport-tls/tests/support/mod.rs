#![allow(dead_code)]

pub mod runtime;
pub mod session_store;
pub mod tls;

use std::{error::Error, fmt, str};

use rafter::{LogIndex, Message, NodeId, RequestVote, Term};
use rafter_transport_tls::GroupIdCodec;

#[derive(Clone, Copy, Debug)]
pub struct StringGroupCodec {
    maximum: usize,
}

impl StringGroupCodec {
    pub const fn new(maximum: usize) -> Self {
        Self { maximum }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StringGroupCodecError {
    InvalidUtf8,
    TooLong { actual: usize, maximum: usize },
}

impl fmt::Display for StringGroupCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUtf8 => formatter.write_str("group ID is not UTF-8"),
            Self::TooLong { actual, maximum } => {
                write!(
                    formatter,
                    "group ID is {actual} bytes, maximum is {maximum}"
                )
            }
        }
    }
}

impl Error for StringGroupCodecError {}

impl GroupIdCodec<String> for StringGroupCodec {
    type Error = StringGroupCodecError;

    fn max_encoded_len(&self) -> usize {
        self.maximum
    }

    fn encode(&self, group_id: &String, output: &mut Vec<u8>) -> Result<(), Self::Error> {
        output.clear();
        if group_id.len() > self.maximum {
            return Err(StringGroupCodecError::TooLong {
                actual: group_id.len(),
                maximum: self.maximum,
            });
        }
        output.extend_from_slice(group_id.as_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<String, Self::Error> {
        if input.len() > self.maximum {
            return Err(StringGroupCodecError::TooLong {
                actual: input.len(),
                maximum: self.maximum,
            });
        }
        str::from_utf8(input)
            .map(str::to_owned)
            .map_err(|_| StringGroupCodecError::InvalidUtf8)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LowercaseGroupCodec {
    maximum: usize,
}

impl LowercaseGroupCodec {
    pub const fn new(maximum: usize) -> Self {
        Self { maximum }
    }
}

impl GroupIdCodec<String> for LowercaseGroupCodec {
    type Error = StringGroupCodecError;

    fn max_encoded_len(&self) -> usize {
        self.maximum
    }

    fn encode(&self, group_id: &String, output: &mut Vec<u8>) -> Result<(), Self::Error> {
        output.clear();
        let canonical = group_id.to_ascii_lowercase();
        if canonical.len() > self.maximum {
            return Err(StringGroupCodecError::TooLong {
                actual: canonical.len(),
                maximum: self.maximum,
            });
        }
        output.extend_from_slice(canonical.as_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<String, Self::Error> {
        let value = str::from_utf8(input)
            .map_err(|_| StringGroupCodecError::InvalidUtf8)?
            .to_ascii_lowercase();
        if value.len() > self.maximum {
            return Err(StringGroupCodecError::TooLong {
                actual: value.len(),
                maximum: self.maximum,
            });
        }
        Ok(value)
    }
}

pub fn request_vote(sender: NodeId) -> Message {
    Message::RequestVote(RequestVote {
        term: Term(3),
        candidate_id: sender,
        last_log_index: LogIndex(42),
        last_log_term: Term(2),
    })
}

pub fn decode_hex(input: &str) -> Vec<u8> {
    let input = input.trim();
    assert_eq!(input.len() % 2, 0, "hex fixture length must be even");
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]);
            let low = hex_nibble(pair[1]);
            high << 4 | low
        })
        .collect()
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex fixture byte {byte:#04x}"),
    }
}
