//! Adversarial scenarios for resolved Rust source inputs.

use std::{
    collections::HashSet,
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use super::{validate_resolved_tracked_rust_inputs, validate_tracked_rust_inputs};

struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "rafter-rust-inputs-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join("src")).expect("create Rust input scratch tree");
        Self { root }
    }

    fn write(&self, relative: &str, bytes: &[u8]) {
        let path = self.root.join(relative);
        fs::create_dir_all(path.parent().expect("input parent")).expect("create input parent");
        fs::write(path, bytes).expect("write Rust input fixture");
    }

    fn git(&self, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .args(arguments)
            .current_dir(&self.root)
            .output()
            .expect("run Git in Rust input fixture");
        assert!(
            output.status.success(),
            "git {arguments:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("Git fixture output is UTF-8")
    }

    fn tracked_ignored_lib_fixture(
        &self,
        source: &[u8],
        generated: &str,
        generated_bytes: &[u8],
    ) -> HashSet<PathBuf> {
        self.git(&["init", "-q"]);
        self.git(&["config", "user.email", "invariants@example.invalid"]);
        self.git(&["config", "user.name", "Invariant Tests"]);
        self.git(&["config", "commit.gpgsign", "false"]);
        self.write(".gitignore", b"/target/\n");
        self.write(
            "Cargo.toml",
            b"[package]\nname = \"rust-input-fixture\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
        );
        self.write("src/lib.rs", source);
        self.git(&["add", "--", ".gitignore", "Cargo.toml", "src/lib.rs"]);
        self.git(&["commit", "-qm", "fixture"]);
        self.write(generated, generated_bytes);
        assert!(
            self.git(&["status", "--porcelain=v1", "--untracked-files=all"])
                .trim()
                .is_empty(),
            "generated input must be ignored and status-clean"
        );
        self.git(&["ls-files", "-z"])
            .split('\0')
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .collect()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).expect("remove Rust input scratch tree");
    }
}

fn tracked(paths: &[&str]) -> HashSet<PathBuf> {
    paths.iter().map(PathBuf::from).collect()
}

fn validate(scratch: &Scratch, paths: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    validate_tracked_rust_inputs(&scratch.root, &tracked(paths))
}

fn metadata_for_target(scratch: &Scratch, target: &str) -> String {
    let source = scratch.root.join(target);
    serde_json::json!({
        "packages": [{
            "source": null,
            "targets": [{"src_path": source}]
        }]
    })
    .to_string()
}

fn detect_ignored_generated_lib_input(
    source: &[u8],
    generated: &str,
    generated_bytes: &[u8],
) -> String {
    let scratch = Scratch::new();
    let tracked = scratch.tracked_ignored_lib_fixture(source, generated, generated_bytes);
    validate_resolved_tracked_rust_inputs(
        &scratch.root,
        &tracked,
        &metadata_for_target(&scratch, "src/lib.rs"),
    )
    .expect_err("ignored generated compiler input must fail closed")
    .to_string()
}

#[test]
fn literal_tracked_include_inputs_are_bound() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"const BOUND: &str = include_str!(\"bound.txt\");\n",
    );
    scratch.write("src/bound.txt", b"bound\n");

    validate(&scratch, &["src/lib.rs", "src/bound.txt"])
        .expect("literal tracked include is part of the source materialization");
}

#[test]
fn nested_expression_macro_includes_are_validated() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"fn check() { assert_eq!(include_str!(\"bound.txt\"), \"bound\\n\"); }\n",
    );
    scratch.write("src/bound.txt", b"bound\n");

    validate(&scratch, &["src/lib.rs", "src/bound.txt"])
        .expect("nested literal include is parsed and bound");

    scratch.write(
        "src/lib.rs",
        b"fn check() { assert_eq!(include_str!(\"../target/generated.txt\"), \"x\"); }\n",
    );
    scratch.write("target/generated.txt", b"x\n");
    let error = validate(&scratch, &["src/lib.rs", "src/bound.txt"])
        .expect_err("nested generated include must fail closed")
        .to_string();
    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn included_non_rs_rust_payloads_are_validated_transitively() {
    let scratch = Scratch::new();
    scratch.write("src/lib.rs", b"include!(\"payload.inc\");\n");
    scratch.write(
        "src/payload.inc",
        b"const PAYLOAD: &[u8] = include_bytes!(\"../target/unbound.bin\");\n",
    );
    scratch.write("target/unbound.bin", b"unbound\n");

    let error = validate(&scratch, &["src/lib.rs", "src/payload.inc"])
        .expect_err("include! payloads are Rust inputs regardless of extension")
        .to_string();
    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn non_rs_cargo_target_roots_are_validated_as_rust() {
    let scratch = Scratch::new();
    scratch.write(
        "src/root.inc",
        b"const PAYLOAD: &[u8] = include_bytes!(\"../target/unbound.bin\");\n",
    );
    scratch.write("target/unbound.bin", b"unbound\n");
    let source = scratch.root.join("src/root.inc");
    let metadata = serde_json::json!({
        "packages": [{
            "source": null,
            "targets": [{"src_path": source}]
        }]
    })
    .to_string();

    let error = validate_resolved_tracked_rust_inputs(
        &scratch.root,
        &tracked(&["src/root.inc"]),
        &metadata,
    )
    .expect_err("Cargo target roots are Rust inputs regardless of extension")
    .to_string();
    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn resolved_detector_rejects_raw_include_bytes_from_ignored_output() {
    let error = detect_ignored_generated_lib_input(
        b"const PAYLOAD: &[u8] = r#include_bytes!(\"../target/generated.bin\");\n",
        "target/generated.bin",
        b"generated\n",
    );

    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn resolved_detector_rejects_raw_include_alias_from_ignored_output() {
    let error = detect_ignored_generated_lib_input(
        b"use std::r#include_bytes as r#load;\nconst PAYLOAD: &[u8] = r#load!(\"../target/generated.bin\");\n",
        "target/generated.bin",
        b"generated\n",
    );

    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn resolved_detector_rejects_raw_path_from_ignored_output() {
    let error = detect_ignored_generated_lib_input(
        b"#[r#path = \"../target/generated.rs\"] mod generated;\n",
        "target/generated.rs",
        b"pub fn substituted() {}\n",
    );

    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn resolved_detector_rejects_multi_hop_alias_from_ignored_output() {
    let error = detect_ignored_generated_lib_input(
        b"use std::include_bytes as load;\nuse load as read;\nconst PAYLOAD: &[u8] = read!(\"../target/generated.bin\");\n",
        "target/generated.bin",
        b"generated\n",
    );

    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn qualified_and_included_include_aliases_are_validated() {
    for (root, extra) in [
        (
            b"mod macros { pub use std::include_bytes as load; }\nconst PAYLOAD: &[u8] = macros::load!(\"../target/unbound.bin\");\n"
                .as_slice(),
            None,
        ),
        (
            b"include!(\"macros.inc\");\nconst PAYLOAD: &[u8] = macros::load!(\"../target/unbound.bin\");\n"
                .as_slice(),
            Some(
                b"mod macros { pub use std::include_bytes as load; }\n".as_slice(),
            ),
        ),
    ] {
        let scratch = Scratch::new();
        scratch.write("src/lib.rs", root);
        let mut paths = vec!["src/lib.rs"];
        if let Some(extra) = extra {
            scratch.write("src/macros.inc", extra);
            paths.push("src/macros.inc");
        }
        scratch.write("target/unbound.bin", b"unbound\n");

        let error = validate(&scratch, &paths)
            .expect_err("qualified include aliases cannot hide compiler inputs")
            .to_string();
        assert!(error.contains("is not tracked"), "{error}");
    }
}

#[cfg(unix)]
#[test]
fn ignored_symlink_cannot_select_a_tracked_include_input() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"const PAYLOAD: &[u8] = include_bytes!(\"../target/selected.bin\");\n",
    );
    scratch.write("src/first.bin", b"first\n");
    scratch.write("src/second.bin", b"second\n");
    fs::create_dir_all(scratch.root.join("target")).expect("create generated output root");
    std::os::unix::fs::symlink("../src/first.bin", scratch.root.join("target/selected.bin"))
        .expect("create compiler-input symlink");

    let error = validate(&scratch, &["src/lib.rs", "src/first.bin", "src/second.bin"])
        .expect_err("filesystem aliases cannot select compiler inputs")
        .to_string();
    assert!(error.contains("unbound filesystem symlink"), "{error}");
}

#[test]
fn resolved_lib_target_ignores_unreferenced_tracked_rust_files() {
    let scratch = Scratch::new();
    scratch.write("src/lib.rs", b"pub fn used() {}\n");
    scratch.write(
        "src/unused.rs",
        b"const UNUSED: &str = include_str!(\"missing.txt\");\n",
    );

    validate_resolved_tracked_rust_inputs(
        &scratch.root,
        &tracked(&["src/lib.rs", "src/unused.rs"]),
        &metadata_for_target(&scratch, "src/lib.rs"),
    )
    .expect("compiler-inactive source does not add effective inputs");
}

#[test]
fn resolved_target_roots_follow_tracked_default_modules_transitively() {
    let scratch = Scratch::new();
    scratch.write("src/lib.rs", b"mod used;\n");
    scratch.write(
        "src/used.rs",
        b"const GENERATED: &str = include_str!(\"../target/generated.txt\");\n",
    );
    scratch.write("target/generated.txt", b"generated\n");

    let error = validate_resolved_tracked_rust_inputs(
        &scratch.root,
        &tracked(&["src/lib.rs", "src/used.rs"]),
        &metadata_for_target(&scratch, "src/lib.rs"),
    )
    .expect_err("transitive default modules must not hide generated compiler inputs")
    .to_string();
    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn resolved_target_roots_follow_tracked_path_modules_transitively() {
    let scratch = Scratch::new();
    scratch.write(
        "src/bin/main.rs",
        b"#[path = \"../../shared.rs\"] mod shared; fn main() {}\n",
    );
    scratch.write(
        "shared.rs",
        b"const GENERATED: &str = include_str!(\"target/generated.txt\");\n",
    );
    scratch.write("target/generated.txt", b"generated\n");
    let source = scratch.root.join("src/bin/main.rs");
    let metadata = serde_json::json!({
        "packages": [{
            "source": null,
            "targets": [{"src_path": source}]
        }]
    })
    .to_string();

    let error = validate_resolved_tracked_rust_inputs(
        &scratch.root,
        &tracked(&["src/bin/main.rs", "shared.rs"]),
        &metadata,
    )
    .expect_err("transitive path modules must not hide generated compiler inputs")
    .to_string();
    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn generated_include_input_fails_even_inside_an_allowed_output_root() {
    let scratch = Scratch::new();
    scratch.write("src/lib.rs", b"include!(\"../target/generated.rs\");\n");
    scratch.write("target/generated.rs", b"pub fn substituted() {}\n");

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("generated output cannot become a compiler source input")
        .to_string();
    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn dynamic_include_paths_fail_closed() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"include_str!(concat!(env!(\"OUT_DIR\"), \"/generated.rs\"));\n",
    );

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("dynamic include paths are not statically bound")
        .to_string();
    assert!(error.contains("one literal tracked path"), "{error}");
}

#[test]
fn aliased_include_inputs_are_validated() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"use std::include_str as load; const VALUE: &str = load!(\"../target/generated.txt\");\n",
    );
    scratch.write("target/generated.txt", b"substituted\n");

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("aliased include macros cannot bypass tracked-input validation")
        .to_string();
    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn macro_generated_include_inputs_fail_closed() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"macro_rules! load { () => { include_str!(concat!(env!(\"OUT_DIR\"), \"/generated\")) } }\n",
    );

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("macro expansion cannot hide compiler input discovery")
        .to_string();
    assert!(error.contains("macro-generated compiler inputs"), "{error}");
}

#[test]
fn macro_generated_include_aliases_fail_closed() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"macro_rules! load { () => {{ use std::include_str as i; i!(\"generated\") }} }\n",
    );

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("an alias introduced by expansion cannot hide an include input")
        .to_string();
    assert!(error.contains("macro-generated compiler inputs"), "{error}");
}

#[test]
fn lifetimes_do_not_hide_macro_generated_include_inputs() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"macro_rules! load { () => {{ fn borrow<'a>(value: &'a str) -> &'a str {{ value }} include_str!(\"generated\") }} }\n",
    );

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("token scanning must continue after Rust lifetimes")
        .to_string();
    assert!(error.contains("macro-generated compiler inputs"), "{error}");
}

#[test]
fn dynamically_selected_macro_inputs_fail_closed() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"macro_rules! load { ($loader:ident) => { $loader!(\"generated\") } }\n",
    );

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("a macro parameter could resolve to an include macro")
        .to_string();
    assert!(error.contains("macro-generated compiler inputs"), "{error}");
}

#[test]
fn macro_generated_out_of_line_modules_fail_closed() {
    for source in [
        b"macro_rules! declare { () => { mod generated; } }\n".as_slice(),
        b"macro_rules! declare { ($name:ident) => { pub(crate) mod $name; } }\n".as_slice(),
        b"macro_rules! declare { () => { const _: () = { mod generated; }; } }\n".as_slice(),
    ] {
        let scratch = Scratch::new();
        scratch.write("src/lib.rs", source);
        scratch.write("src/generated.rs", b"pub fn generated() {}\n");

        let error = validate(&scratch, &["src/lib.rs"])
            .expect_err("macro expansion cannot hide an out-of-line module input")
            .to_string();
        assert!(error.contains("macro-generated compiler inputs"), "{error}");
    }
}

#[test]
fn opaque_macro_arguments_with_out_of_line_modules_fail_closed() {
    for source in [
        b"items! { mod generated; }\n".as_slice(),
        b"forward! { items! { pub(crate) mod generated; } }\n".as_slice(),
        b"items! { mod r#generated; }\n".as_slice(),
    ] {
        let scratch = Scratch::new();
        scratch.write("src/lib.rs", source);
        scratch.write("src/generated.rs", b"pub fn generated() {}\n");

        let error = validate(&scratch, &["src/lib.rs"])
            .expect_err("opaque macro input cannot forward an unbound module declaration")
            .to_string();
        assert!(error.contains("macro-generated compiler inputs"), "{error}");
    }
}

#[test]
fn definitively_inactive_macro_generated_modules_are_pruned() {
    for attribute in [
        "#[cfg(any())]",
        "#[cfg(all(any(), target_os = \"linux\"))]",
        "#[cfg_attr(all(), cfg(any()))]",
        "#[cfg_attr(all(), cfg_attr(all(), cfg(any())))]",
    ] {
        let scratch = Scratch::new();
        scratch.write(
            "src/lib.rs",
            format!("macro_rules! declare {{ () => {{ {attribute} mod generated; }} }}\n")
                .as_bytes(),
        );
        scratch.write("src/generated.rs", b"pub fn generated() {}\n");

        validate(&scratch, &["src/lib.rs"])
            .expect("a definitively inactive generated module adds no compiler input");
    }
}

#[test]
fn unknown_cfg_macro_generated_modules_fail_closed() {
    for attribute in [
        "#[cfg(target_os = \"linux\")]",
        "#[cfg_attr(target_os = \"linux\", cfg(any()))]",
    ] {
        let scratch = Scratch::new();
        scratch.write(
            "src/lib.rs",
            format!("macro_rules! declare {{ () => {{ {attribute} mod generated; }} }}\n")
                .as_bytes(),
        );
        scratch.write("src/generated.rs", b"pub fn generated() {}\n");

        let error = validate(&scratch, &["src/lib.rs"])
            .expect_err("an unknown generated module cfg may remain active")
            .to_string();
        assert!(error.contains("macro-generated compiler inputs"), "{error}");
    }
}

#[test]
fn macro_matchers_and_inline_modules_do_not_look_out_of_line() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"macro_rules! inspect { (mod $name:ident;) => { stringify!($name) } }\n\
          macro_rules! declare { () => { mod generated { pub fn generated() {} } } }\n",
    );

    validate(&scratch, &["src/lib.rs"])
        .expect("only out-of-line modules emitted by a transcriber add source files");
}

#[test]
fn tracked_path_modules_are_bound() {
    let scratch = Scratch::new();
    scratch.write("src/lib.rs", b"#[path = \"bound.rs\"] mod bound;\n");
    scratch.write("src/bound.rs", b"pub fn bound() {}\n");

    validate(&scratch, &["src/lib.rs", "src/bound.rs"])
        .expect("literal tracked path modules are bound inputs");
}

#[test]
fn generated_path_modules_fail_closed() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"#[path = \"../target/generated.rs\"] mod generated;\n",
    );
    scratch.write("target/generated.rs", b"pub fn substituted() {}\n");

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("path modules cannot load generated source")
        .to_string();
    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn conditional_path_modules_fail_closed() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"#[cfg_attr(unix, path = \"unix.rs\")] mod selected;\n",
    );

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("target-dependent module source selection is not portable")
        .to_string();
    assert!(error.contains("target-conditional #[path]"), "{error}");
}

#[test]
fn effective_nested_cfg_attr_path_modules_are_bound() {
    let error = detect_ignored_generated_lib_input(
        b"#[cfg_attr(all(), cfg_attr(all(), path = \"../target/generated.rs\"))] mod generated;\n",
        "target/generated.rs",
        b"pub fn substituted() {}\n",
    );

    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn inactive_nested_cfg_attr_paths_leave_default_module_resolution() {
    for source in [
        b"#[cfg_attr(any(), cfg_attr(all(), path = \"../target/generated.rs\"))] mod selected;\n"
            .as_slice(),
        b"#[cfg_attr(all(), cfg_attr(any(), path = \"../target/generated.rs\"))] mod selected;\n"
            .as_slice(),
    ] {
        let scratch = Scratch::new();
        scratch.write("src/lib.rs", source);
        scratch.write("src/selected.rs", b"pub fn selected() {}\n");
        scratch.write("target/generated.rs", b"pub fn substituted() {}\n");

        validate(&scratch, &["src/lib.rs", "src/selected.rs"])
            .expect("a definitively inactive nested path must not select generated source");
    }
}

#[test]
fn unknown_nested_cfg_attr_paths_fail_closed() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"#[cfg_attr(all(), cfg_attr(target_os = \"linux\", path = \"linux.rs\"))] mod selected;\n",
    );

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("target-dependent nested source selection is not statically bound")
        .to_string();
    assert!(error.contains("target-conditional #[path]"), "{error}");
}

#[test]
fn path_modules_inside_inline_modules_fail_closed() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"mod inline { #[path = \"generated.rs\"] mod generated; }\n",
    );
    scratch.write("src/generated.rs", b"pub fn wrong_file() {}\n");

    let error = validate(&scratch, &["src/lib.rs", "src/generated.rs"])
        .expect_err("inline modules change the compiler's relative path base")
        .to_string();
    assert!(error.contains("inside an inline module"), "{error}");
}

#[test]
fn definitively_inactive_cfg_items_do_not_add_inputs() {
    for attribute in [
        "#[cfg(any())]",
        "#[cfg(all(any(), target_os = \"linux\"))]",
        "#[cfg(not(all()))]",
        "#[cfg_attr(all(), cfg(any()))]",
        "#[cfg_attr(all(), cfg_attr(all(), cfg(any())))]",
    ] {
        let scratch = Scratch::new();
        scratch.write(
            "src/lib.rs",
            format!("{attribute}\nconst UNAVAILABLE: &[u8] = include_bytes!(\"missing.bin\");\n")
                .as_bytes(),
        );

        validate(&scratch, &["src/lib.rs"])
            .expect("a definitively inactive item has no compiler inputs");
    }
}

#[test]
fn definitively_inactive_modules_and_aliases_are_pruned() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"#[cfg(any())] mod generated;\n\
          #[cfg(any())] use std::include_str as load;\n\
          macro_rules! load { () => { \"not an include\" } }\n\
          const VALUE: &str = load!();\n",
    );
    scratch.write("src/generated.rs", b"pub fn generated() {}\n");

    validate(&scratch, &["src/lib.rs"])
        .expect("inactive modules and aliases cannot affect the compiler-input closure");
}

#[test]
fn unknown_cfg_items_still_fail_closed() {
    for attribute in [
        "#[cfg(target_os = \"linux\")]",
        "#[cfg_attr(target_os = \"linux\", cfg(any()))]",
    ] {
        let scratch = Scratch::new();
        scratch.write(
            "src/lib.rs",
            format!(
                "{attribute}\nconst GENERATED: &[u8] = include_bytes!(\"../target/generated.bin\");\n"
            )
            .as_bytes(),
        );
        scratch.write("target/generated.bin", b"generated\n");

        let error = validate(&scratch, &["src/lib.rs"])
            .expect_err("an unknown cfg branch may remain active")
            .to_string();
        assert!(error.contains("is not tracked"), "{error}");
    }
}

#[test]
fn include_aliases_are_lexically_scoped_between_modules_and_blocks() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"mod alias_scope {\n\
              use std::include_str as load;\n\
              const BOUND: &str = load!(\"bound.txt\");\n\
          }\n\
          mod unrelated {\n\
              macro_rules! load { () => { \"local module macro\" } }\n\
              const VALUE: &str = load!();\n\
          }\n\
          fn block_scopes() {\n\
              { use std::include_str as read; let _ = read!(\"bound.txt\"); }\n\
              { macro_rules! read { () => { \"local block macro\" } } let _ = read!(); }\n\
          }\n",
    );
    scratch.write("src/bound.txt", b"bound\n");

    validate(&scratch, &["src/lib.rs", "src/bound.txt"])
        .expect("an include alias cannot contaminate a sibling lexical scope");
}

#[test]
fn qualified_include_aliases_do_not_contaminate_sibling_macro_paths() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"mod exported { pub use std::include_str as load; }\n\
          mod local {\n\
              macro_rules! load { () => { \"not an include\" } }\n\
              pub(crate) use load;\n\
          }\n\
          const VALUE: &str = local::load!();\n",
    );

    validate(&scratch, &["src/lib.rs"])
        .expect("a qualified include alias cannot contaminate a sibling macro path");
}

#[test]
fn block_local_qualified_include_aliases_are_validated() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"fn block_local() {\n\
              mod exported { pub use std::include_str as load; }\n\
              let _ = exported::load!(\"../target/generated.txt\");\n\
          }\n",
    );
    scratch.write("target/generated.txt", b"generated\n");

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("block-local qualified include aliases cannot hide compiler inputs")
        .to_string();
    assert!(error.contains("is not tracked"), "{error}");
}

#[test]
fn block_local_qualified_macro_shadows_outer_include_aliases() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"mod exported { pub use std::include_str as load; }\n\
          fn block_local() {\n\
              mod exported { macro_rules! load { () => { \"not an include\" } } pub(crate) use load; }\n\
              let _ = exported::load!();\n\
          }\n",
    );

    validate(&scratch, &["src/lib.rs"])
        .expect("a block-local qualified macro shadows an outer include alias");
}

#[test]
fn included_include_aliases_are_scoped_to_the_including_module() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"include!(\"aliases.inc\");\n\
          const BOUND: &str = load!(\"bound.txt\");\n\
          mod sibling {\n\
              macro_rules! load { () => { \"not an include\" } }\n\
              pub(crate) use load;\n\
              const VALUE: &str = load!();\n\
          }\n",
    );
    scratch.write("src/aliases.inc", b"use std::include_str as load;\n");
    scratch.write("src/bound.txt", b"bound\n");

    validate(
        &scratch,
        &["src/lib.rs", "src/aliases.inc", "src/bound.txt"],
    )
    .expect("include-file aliases apply only where the include text is inserted");
}

#[test]
fn local_macros_shadow_outer_include_aliases() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"use std::include_str as load;\n\
          const BOUND: &str = load!(\"bound.txt\");\n\
          fn local() {\n\
              macro_rules! load { () => { \"shadowed locally\" } }\n\
              let _ = load!();\n\
          }\n",
    );
    scratch.write("src/bound.txt", b"bound\n");

    validate(&scratch, &["src/lib.rs", "src/bound.txt"])
        .expect("a local macro definition shadows an outer include alias");
}

#[test]
fn include_aliases_remain_active_inside_their_own_scope() {
    let scratch = Scratch::new();
    scratch.write(
        "src/lib.rs",
        b"mod aliases {\n\
              use std::include_bytes as load;\n\
              const GENERATED: &[u8] = load!(\"../target/generated.bin\");\n\
          }\n",
    );
    scratch.write("target/generated.bin", b"generated\n");

    let error = validate(&scratch, &["src/lib.rs"])
        .expect_err("a scoped include alias still binds its compiler input")
        .to_string();
    assert!(error.contains("is not tracked"), "{error}");
}
