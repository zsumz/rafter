//! Whitespace-tolerant tokenization of macro input for conservative scanning.
//!
//! Macro bodies are not parsed as Rust, so the scan works over a flat
//! identifier-and-punctuation stream that skips string, raw-string, and
//! character literals rather than interpreting them.

use syn::Macro;

#[derive(Debug, Eq, PartialEq)]
pub(super) enum MacroToken {
    Ident(String),
    Punct(char),
}

pub(super) fn macro_tokens(invocation: &Macro) -> Vec<MacroToken> {
    let source = invocation.tokens.to_string();
    let bytes = source.as_bytes();
    let mut values = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
        } else if byte == b'"' {
            index = skip_quoted(bytes, index, byte);
        } else if byte == b'\'' {
            if let Some(end) = char_literal_end(bytes, index) {
                index = end;
            } else {
                values.push(MacroToken::Punct('\''));
                index += 1;
            }
        } else if byte == b'r' {
            if let Some(end) = raw_string_end(bytes, index) {
                index = end;
            } else if bytes.get(index + 1) == Some(&b'#')
                && bytes
                    .get(index + 2)
                    .is_some_and(|byte| *byte == b'_' || byte.is_ascii_alphabetic())
            {
                let start = index + 2;
                index = start + 1;
                while index < bytes.len()
                    && (bytes[index] == b'_' || bytes[index].is_ascii_alphanumeric())
                {
                    index += 1;
                }
                values.push(MacroToken::Ident(source[start..index].to_owned()));
            } else {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index] == b'_' || bytes[index].is_ascii_alphanumeric())
                {
                    index += 1;
                }
                values.push(MacroToken::Ident(source[start..index].to_owned()));
            }
        } else if byte == b'_' || byte.is_ascii_alphabetic() {
            let start = index;
            index += 1;
            while index < bytes.len()
                && (bytes[index] == b'_' || bytes[index].is_ascii_alphanumeric())
            {
                index += 1;
            }
            values.push(MacroToken::Ident(source[start..index].to_owned()));
        } else {
            values.push(MacroToken::Punct(char::from(byte)));
            index += 1;
        }
    }
    values
}

fn char_literal_end(bytes: &[u8], start: usize) -> Option<usize> {
    let content = start + 1;
    if bytes.get(content) == Some(&b'\\') {
        return (content + 2 < bytes.len() && bytes[content + 2] == b'\'').then_some(content + 3);
    }
    (content + 1 < bytes.len() && bytes[content + 1] == b'\'').then_some(content + 2)
}

fn skip_quoted(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            index = (index + 2).min(bytes.len());
        } else if bytes[index] == quote {
            return index + 1;
        } else {
            index += 1;
        }
    }
    bytes.len()
}

fn raw_string_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut opening = start + 1;
    while opening < bytes.len() && bytes[opening] == b'#' {
        opening += 1;
    }
    if bytes.get(opening) != Some(&b'"') {
        return None;
    }
    let hashes = opening - start - 1;
    let mut index = opening + 1;
    while index < bytes.len() {
        if bytes[index] == b'"'
            && bytes.get(index + 1..index + 1 + hashes) == Some(&bytes[start + 1..opening])
        {
            return Some(index + 1 + hashes);
        }
        index += 1;
    }
    Some(bytes.len())
}
