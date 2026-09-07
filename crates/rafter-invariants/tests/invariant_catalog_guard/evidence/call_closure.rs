//! Static call closure inside one source file, and the bridge it proves.
//!
//! A negative control either drives the registered checker itself or names a
//! bridge reachable from both; the closure below is what makes "reachable"
//! decidable without compiling anything.

use std::{collections::BTreeSet, fs, path::Path};

use super::Evidence;

/// Requires the negative control to reach the checker the row registers.
///
/// Everything above proves *fixture -> detector*. None of it opens
/// `record.path` to ask whether `detector` has anything to do with `symbol`, so
/// a row could register checker X while its control exercised unrelated
/// function Y, and twelve of them did.
///
/// The relation must be one of two, and both are checked:
///
/// * `detector == symbol` -- the control drives the registered checker itself.
/// * the row names a `negative_fixture_detector_bridge` reached from both, and
///   states in `negative_fixture_uncovered` what the checker does that the
///   control does not.
///
/// The bridge is resolved by static call closure inside each declaring file.
/// That is deliberately weaker than it sounds and the second field is why: a
/// static closure cannot tell whether the bridge is evaluated on the input the
/// fixture actually supplies, so a bridged row is a row whose control covers
/// some proper part of its checker. `negative_fixture_uncovered` is where that
/// part is named, in prose, rather than left for a later reader to discover.
pub(super) fn assert_detector_relates_to_registered_checker(
    workspace: &Path,
    record: &Evidence,
    detector: &str,
    detector_source: &str,
) {
    let bridge = record.negative_fixture_detector_bridge.as_deref();
    let uncovered = record.negative_fixture_uncovered.as_deref();

    if detector == record.symbol {
        assert!(
            bridge.is_none() && uncovered.is_none(),
            "{} registers its own checker as the detector; drop negative_fixture_detector_bridge and negative_fixture_uncovered",
            record.id,
        );
        return;
    }

    let bridge = bridge.unwrap_or_else(|| {
        panic!(
            "{} {} negative control drives `{detector}` but registers checker `{}`; \
             name the function both reach in negative_fixture_detector_bridge, \
             or register the checker the control actually drives",
            record.id,
            record.clauses.join(","),
            record.symbol,
        )
    });
    let uncovered = uncovered.unwrap_or_else(|| {
        panic!(
            "{} {} bridges `{detector}` to `{}` via `{bridge}`; \
             a bridged control covers part of its checker, so negative_fixture_uncovered \
             must say which part it does not reach",
            record.id,
            record.clauses.join(","),
            record.symbol,
        )
    });
    assert!(
        uncovered.trim().len() >= 20,
        "{} negative_fixture_uncovered must state the residual, not a placeholder: {uncovered:?}",
        record.id,
    );

    let checker_path = workspace.join(&record.path);
    let checker_source = fs::read_to_string(&checker_path).unwrap_or_else(|error| {
        panic!(
            "read registered checker source {}: {error}",
            checker_path.display()
        )
    });

    assert!(
        call_closure(detector_source, detector).contains(bridge),
        "{} bridge `{bridge}` is not reachable from detector `{detector}`",
        record.id,
    );
    assert!(
        call_closure(&checker_source, &record.symbol).contains(bridge),
        "{} bridge `{bridge}` is not reachable from registered checker `{}` in {}",
        record.id,
        record.symbol,
        record.path,
    );
}

/// Names reachable from `start` by static call edges inside one source file.
///
/// Includes `start` itself, so a bridge may be either endpoint. Method-call
/// syntax counts: the ST-01 bridge is `node.validate_derived_state()` on one
/// side and a bare call on the other.
pub(super) fn call_closure(source: &str, start: &str) -> BTreeSet<String> {
    let mut reached = BTreeSet::new();
    let mut pending = vec![start.to_owned()];
    while let Some(name) = pending.pop() {
        if !reached.insert(name.clone()) {
            continue;
        }
        for callee in function_body(source, &name)
            .map(|body| called_names(&body))
            .unwrap_or_default()
        {
            if !reached.contains(&callee) {
                pending.push(callee);
            }
        }
    }
    reached
}

/// The brace-balanced body of `fn name`, if this file declares it.
pub(super) fn function_body(source: &str, name: &str) -> Option<String> {
    let mut search = 0;
    while let Some(offset) = source[search..].find("fn ") {
        let start = search + offset + "fn ".len();
        search = start;
        let rest = &source[start..];
        let identifier_len = rest
            .find(|character: char| !character.is_alphanumeric() && character != '_')
            .unwrap_or(rest.len());
        if &rest[..identifier_len] != name {
            continue;
        }
        let open = source[start..].find('{')? + start;
        let mut depth = 0usize;
        for (index, character) in source[open..].char_indices() {
            match character {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(source[open..=open + index].to_owned());
                    }
                }
                _ => {}
            }
        }
        return None;
    }
    None
}

/// Identifiers this body invokes, as bare calls or method calls.
pub(super) fn called_names(body: &str) -> Vec<String> {
    let bytes = body.as_bytes();
    let mut names = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let character = bytes[index] as char;
        if character.is_alphabetic() || character == '_' {
            let start = index;
            while index < bytes.len() {
                let current = bytes[index] as char;
                if current.is_alphanumeric() || current == '_' {
                    index += 1;
                } else {
                    break;
                }
            }
            let mut cursor = index;
            while cursor < bytes.len() && (bytes[cursor] as char).is_whitespace() {
                cursor += 1;
            }
            if cursor < bytes.len() && bytes[cursor] == b'(' {
                names.push(body[start..index].to_owned());
            }
        } else {
            index += 1;
        }
    }
    names
}
