//! Strict sha256sum-compatible verifier-artifact manifest parsing.

use std::collections::BTreeMap;

pub(super) fn parse(bytes: &[u8]) -> Result<BTreeMap<String, String>, String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| "verifier artifact manifest is not UTF-8".to_owned())?;
    if text.is_empty() || !text.ends_with('\n') {
        return Err("verifier artifact manifest is empty or unterminated".to_owned());
    }
    let mut entries = BTreeMap::new();
    for line in text.lines() {
        let Some((digest, name)) = line.split_once("  ") else {
            return Err("verifier artifact manifest row is malformed".to_owned());
        };
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || !canonical_name(name)
        {
            return Err("verifier artifact manifest row is noncanonical".to_owned());
        }
        if entries.insert(name.to_owned(), digest.to_owned()).is_some() {
            return Err(format!(
                "verifier artifact manifest repeats filename {name}"
            ));
        }
    }
    Ok(entries)
}

pub(super) fn canonical_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && name.bytes().all(|byte| byte.is_ascii_graphic())
}
