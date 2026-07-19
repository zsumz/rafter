//! The attribute expansion requires the canonical runtime crate name.

#[path = "support/compile_fixture.rs"]
mod compile_fixture;

use compile_fixture::{runtime_dependency, CargoFixture};

#[test]
fn a_renamed_dependency_must_restore_the_canonical_expansion_name() {
    let fixture = CargoFixture::new("detector-name-contract", &runtime_dependency("renamed"));

    write_fixture(&fixture, false);
    let rejected = fixture.compile();
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("rafter_invariant_test"),
        "renamed dependency failed for an unrelated reason: {}",
        String::from_utf8_lossy(&rejected.stderr)
    );

    write_fixture(&fixture, true);
    let accepted = fixture.compile();
    assert!(
        accepted.status.success(),
        "canonical alias did not satisfy the expansion ABI: {}",
        String::from_utf8_lossy(&accepted.stderr)
    );
}

fn write_fixture(fixture: &CargoFixture, restore_canonical_name: bool) {
    let alias = if restore_canonical_name {
        "extern crate renamed as rafter_invariant_test;\n"
    } else {
        ""
    };
    fixture.write_source(&format!(
        "use renamed::{{detector_test, oracle_expect_err}};\n{alias}\nfn reject() -> Result<(), ()> {{ Err(()) }}\n\n#[detector_test]\nfn fixture() {{ let _ = oracle_expect_err!(reject(), \"reject\"); }}\n\nfn main() {{}}\n"
    ));
}
