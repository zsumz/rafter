//! Scenarios: architecture source analysis rejects syntactic indirection and cfg ambiguity.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use syn::visit::Visit;

use super::architecture_support::{
    declared_module_graph_from_roots, module_is_test_only, BlockingProcessCollector, PathContext,
    RustPathCollector,
};

static SCRATCH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn inline_module_paths_use_their_effective_module_context() {
    let syntax = syn::parse_file(
        r"
        mod nested {
            use super::super::execution::process::run;
        }
        ",
    )
    .expect("parse inline-module fixture");
    let mut paths = RustPathCollector::new(vec!["producer".to_owned()]);
    paths.visit_file(&syntax);

    assert!(paths.occurrences.iter().any(|occurrence| {
        occurrence.context == PathContext::Import
            && occurrence.normalized == ["crate", "execution", "process", "run"]
    }));
}

#[test]
fn macro_lifetimes_cannot_hide_process_paths() {
    let syntax = syn::parse_file(
        r"
        macro_rules! hidden_run {
            () => {{
                fn borrow<'a>(value: &'a str) -> &'a str { value }
                crate::execution::process::run()
            }};
        }
        ",
    )
    .expect("parse macro lifetime fixture");
    let mut paths = RustPathCollector::new(Vec::new());
    paths.visit_file(&syntax);

    assert_eq!(paths.process_macro_tokens.len(), 1);
}

#[test]
fn every_blocking_child_and_command_form_is_detected() {
    let syntax = syn::parse_file(
        r"
        fn blocked(mut child: std::process::Child, mut command: std::process::Command) {
            let _ = child.wait();
            let _ = child.wait_with_output();
            let _ = command.output();
            let _ = command.status();
            let _ = std::process::Child::wait(&mut child);
            let _ = <std::process::Child>::wait(&mut child);
            macro_rules! hidden { () => { command.output() }; }
        }
        ",
    )
    .expect("parse blocking process fixture");
    let mut blocking = BlockingProcessCollector::default();
    blocking.visit_file(&syntax);

    assert_eq!(
        blocking.calls.len(),
        7,
        "detected calls: {:?}",
        blocking.calls
    );
}

#[test]
fn grouped_and_inline_relative_domain_paths_are_normalized() {
    let syntax = syn::parse_file(
        r"
        use crate::{verification::Verifier, verdict};
        mod nested {
            use super::super::verification::IndependentVerifier;
        }
        ",
    )
    .expect("parse domain indirection fixture");
    let mut paths = RustPathCollector::new(vec!["producer".to_owned()]);
    paths.visit_file(&syntax);
    let dependencies = paths
        .occurrences
        .iter()
        .filter_map(|occurrence| occurrence.normalized.get(1).map(String::as_str))
        .collect::<Vec<_>>();

    assert_eq!(
        dependencies
            .iter()
            .filter(|dependency| **dependency == "verification")
            .count(),
        2
    );
    assert!(dependencies.contains(&"verdict"));
}

#[test]
fn module_graph_follows_both_roots_cfg_attr_paths_and_attributed_includes() {
    let scratch = ScratchTree::new();
    scratch.write(
        "src/lib.rs",
        r#"
        #[cfg_attr(test, path = "selected.rs")]
        mod selected_module;
        #[cfg_attr(not(test), cfg(any()))]
        mod cfg_attr_test_only;
        #[cfg(test)]
        include!("included.rs");
        "#,
    );
    scratch.write("src/main.rs", "mod binary_only;");
    scratch.write("src/selected.rs", "pub fn selected() {}");
    scratch.write("src/cfg_attr_test_only.rs", "pub fn test_only() {}");
    scratch.write("src/included.rs", "pub fn included() {}");
    scratch.write("src/binary_only.rs", "pub fn binary_only() {}");

    let graph = declared_module_graph_from_roots(
        scratch.path(),
        &[
            scratch.path().join("src/lib.rs"),
            scratch.path().join("src/main.rs"),
        ],
    );

    assert!(module_is_test_only(&graph, "src/selected.rs"));
    assert!(module_is_test_only(&graph, "src/cfg_attr_test_only.rs"));
    assert!(module_is_test_only(&graph, "src/included.rs"));
    assert!(!module_is_test_only(&graph, "src/binary_only.rs"));
}

struct ScratchTree {
    root: PathBuf,
}

impl ScratchTree {
    fn new() -> Self {
        let sequence = SCRATCH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "rafter-invariant-architecture-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create architecture scratch tree");
        Self { root }
    }

    fn path(&self) -> &Path {
        &self.root
    }

    fn write(&self, relative: &str, source: &str) {
        let path = self.root.join(relative);
        fs::create_dir_all(path.parent().expect("scratch file has parent"))
            .expect("create architecture scratch parent");
        fs::write(path, source).expect("write architecture scratch source");
    }
}

impl Drop for ScratchTree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
