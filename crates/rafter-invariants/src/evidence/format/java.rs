//! Neutral decoding of the major version from Java version text.

pub(crate) fn major(version: &str) -> Option<u32> {
    version.split_whitespace().find_map(|part| {
        let part = part.trim_matches('"');
        let mut components = part.split('.');
        let first = components.next()?.parse::<u32>().ok()?;
        if first == 1 {
            components.next()?.parse().ok()
        } else {
            Some(first)
        }
    })
}
