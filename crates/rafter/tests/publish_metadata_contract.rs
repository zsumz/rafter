//! Scenario: the crates RELEASE.md promises to publish stay publishable.
//!
//! Registry metadata is only checked by crates.io at upload time, which is the
//! worst moment to discover a missing keyword list or a licence copy that
//! drifted from the root. This guard reads the publish list straight out of
//! `RELEASE.md` so the inventory cannot be forgotten here when it changes
//! there, and holds every crate on that list to the metadata a crates.io
//! upload needs.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

/// crates.io rejects a keyword list longer than this.
const MAX_KEYWORDS: usize = 5;
/// crates.io rejects a category list longer than this.
const MAX_CATEGORIES: usize = 5;

/// The crates.io category slugs this workspace uses. crates.io rejects any
/// slug outside its own taxonomy, and that rejection only arrives at upload
/// time, so new slugs are reviewed against the taxonomy and listed here.
const REVIEWED_CATEGORY_SLUGS: &[&str] = &[
    "algorithms",
    "asynchronous",
    "concurrency",
    "database-implementations",
    "encoding",
    "filesystem",
    "network-programming",
];

#[test]
fn release_publish_list_matches_the_workspace_crates_without_publish_false() {
    let root = workspace_root();
    let declared = release_publish_list(&root);
    let publishable = workspace_publishable_crates(&root);

    assert_eq!(
        declared, publishable,
        "RELEASE.md's 0.0.1 publish list and the workspace crates without \
         `publish = false` have diverged; update whichever one is wrong"
    );
}

#[test]
fn publishable_crates_carry_complete_registry_metadata() {
    let root = workspace_root();
    let mut violations = Vec::new();

    for crate_name in release_publish_list(&root) {
        let manifest_path = root.join("crates").join(&crate_name).join("Cargo.toml");
        let manifest = read(&manifest_path);
        let package = manifest_section(&manifest, "package");
        let mut report = |problem: String| violations.push(format!("{crate_name}: {problem}"));

        match string_value(&package, "description") {
            None => report("no `description`; crates.io requires one".to_owned()),
            Some(description) if description.trim().is_empty() => {
                report("empty `description`".to_owned());
            }
            Some(_) => {}
        }

        match string_value(&package, "readme") {
            None => report("no `readme`; the crates.io page would render blank".to_owned()),
            Some(readme) => {
                let readme_path = root.join("crates").join(&crate_name).join(&readme);
                if !readme_path.is_file() {
                    report(format!("`readme = \"{readme}\"` points at no file"));
                }
            }
        }

        match array_values(&package, "keywords") {
            None => report("no `keywords`".to_owned()),
            Some(keywords) if keywords.is_empty() => report("empty `keywords`".to_owned()),
            Some(keywords) if keywords.len() > MAX_KEYWORDS => report(format!(
                "{} keywords; crates.io allows at most {MAX_KEYWORDS}",
                keywords.len()
            )),
            Some(_) => {}
        }

        match array_values(&package, "categories") {
            None => report("no `categories`".to_owned()),
            Some(categories) if categories.is_empty() => report("empty `categories`".to_owned()),
            Some(categories) if categories.len() > MAX_CATEGORIES => report(format!(
                "{} categories; crates.io allows at most {MAX_CATEGORIES}",
                categories.len()
            )),
            Some(categories) => {
                for category in categories {
                    if !REVIEWED_CATEGORY_SLUGS.contains(&category.as_str()) {
                        report(format!(
                            "category `{category}` is not a reviewed crates.io slug; \
                             check the crates.io category list and add it to \
                             REVIEWED_CATEGORY_SLUGS"
                        ));
                    }
                }
            }
        }

        for inherited in ["license", "repository", "rust-version"] {
            if !package
                .lines()
                .any(|line| strip_comment(line).trim() == format!("{inherited}.workspace = true"))
            {
                report(format!(
                    "`{inherited}` is not inherited from `[workspace.package]`"
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "publishable crate metadata is incomplete:\n\n{}",
        violations.join("\n")
    );
}

#[test]
fn publishable_crates_ship_the_root_licence_texts_unchanged() {
    let root = workspace_root();
    let license = read(&root.join("LICENSE"));
    let notice = read(&root.join("NOTICE"));
    let mut violations = Vec::new();

    for crate_name in release_publish_list(&root) {
        let crate_dir = root.join("crates").join(&crate_name);
        for (file, expected) in [("LICENSE", &license), ("NOTICE", &notice)] {
            let path = crate_dir.join(file);
            if !path.is_file() {
                violations.push(format!(
                    "{crate_name}: no {file}; Cargo packages only files under a crate's \
                     own directory, so the archive would ship without it"
                ));
                continue;
            }
            if &read(&path) != expected {
                violations.push(format!(
                    "{crate_name}: {file} differs from the root {file}; a drifted copy \
                     ships a licence this project did not grant"
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "publishable crate licence copies are wrong:\n\n{}",
        violations.join("\n")
    );
}

/// Reads the crate names from the fenced block under RELEASE.md's
/// "Publish these crates" heading, which is the release document's own
/// authoritative inventory.
fn release_publish_list(root: &Path) -> BTreeSet<String> {
    const MARKER: &str = "Publish these crates for 0.0.1:";
    let release = read(&root.join("RELEASE.md"));
    let start = release
        .lines()
        .position(|line| line.trim() == MARKER)
        .unwrap_or_else(|| panic!("RELEASE.md should contain the line `{MARKER}`"));
    let crates = release
        .lines()
        .skip(start)
        .skip_while(|line| !line.trim_start().starts_with("```"))
        .skip(1)
        .take_while(|line| !line.trim_start().starts_with("```"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();

    assert!(
        !crates.is_empty(),
        "RELEASE.md's publish list should name at least one crate"
    );
    crates
}

/// Every workspace member whose manifest does not opt out of publishing.
fn workspace_publishable_crates(root: &Path) -> BTreeSet<String> {
    let root_manifest = read(&root.join("Cargo.toml"));
    let workspace = manifest_section(&root_manifest, "workspace");
    let mut publishable = BTreeSet::new();

    for member in array_values(&workspace, "members").expect("workspace should declare members") {
        let manifest_path = root.join(&member).join("Cargo.toml");
        let manifest = read(&manifest_path);
        let package = manifest_section(&manifest, "package");
        let opts_out = package
            .lines()
            .filter_map(|line| {
                strip_comment(line)
                    .trim()
                    .split_once('=')
                    .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()))
            })
            .any(|(key, value)| key == "publish" && value == "false");
        if opts_out {
            continue;
        }
        let name = string_value(&package, "name")
            .unwrap_or_else(|| panic!("{} should declare package.name", manifest_path.display()));
        publishable.insert(name);
    }

    publishable
}

/// The lines of a top-level manifest table, up to the next table header.
fn manifest_section(manifest: &str, section: &str) -> String {
    let header = format!("[{section}]");
    let Some(start) = manifest.lines().position(|line| line.trim() == header) else {
        return String::new();
    };
    manifest
        .lines()
        .skip(start + 1)
        .take_while(|line| !line.trim_start().starts_with('['))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The value of a single-line `key = "value"` entry in a manifest table.
fn string_value(section: &str, key: &str) -> Option<String> {
    section.lines().find_map(|line| {
        let entry = strip_comment(line);
        let (found, value) = entry.trim().split_once('=')?;
        (found.trim() == key).then(|| unquote(value.trim()))
    })
}

/// The entries of a `key = ["a", "b"]` array, written on one line or many.
fn array_values(section: &str, key: &str) -> Option<Vec<String>> {
    let prefix = format!("{key} =");
    let start = section
        .lines()
        .position(|line| strip_comment(line).trim_start().starts_with(&prefix))?;
    let mut body = String::new();
    for line in section.lines().skip(start) {
        let line = strip_comment(line);
        body.push_str(&line);
        if line.contains(']') {
            break;
        }
    }
    let open = body.find('[')?;
    let close = body.rfind(']')?;
    Some(
        body[open + 1..close]
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(unquote)
            .collect(),
    )
}

fn strip_comment(line: &str) -> String {
    line.split_once('#')
        .map_or_else(|| line.to_owned(), |(before, _)| before.to_owned())
}

fn unquote(value: &str) -> String {
    value.trim().trim_matches('"').to_owned()
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}
