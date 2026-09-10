//! Telemetry observes attempted work without changing results or default behavior.
use super::*;

#[test]
fn diagnostics_are_opt_in_and_preserve_errors() {
    let before = snapshot()[Stage::LogSync as usize].1.calls;
    measure(Stage::LogSync, || ());
    assert_eq!(snapshot()[Stage::LogSync as usize].1.calls, before);
    set_enabled(true);
    let result: Result<(), &str> = measure(Stage::LogSync, || Err("injected"));
    set_enabled(false);
    assert_eq!(result, Err("injected"));
    let metric = &snapshot()[Stage::LogSync as usize].1;
    assert_eq!(metric.calls, before + 1);
    assert_eq!(metric.buckets.iter().sum::<u64>(), metric.calls);
}
