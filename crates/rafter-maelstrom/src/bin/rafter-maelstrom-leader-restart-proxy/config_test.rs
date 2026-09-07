//! Restart-mode parsing for the proxy's environment configuration.
//!
//! Every documented mode and the aliases the harness scripts already use must
//! parse, and an unrecognized mode must fall back to the historical
//! leader-triggered behaviour rather than failing a run outright.

use super::*;

#[test]
fn parses_restart_modes() {
    // The parser accepts aliases used by scripts but keeps the proxy's
    // historical leader-triggered behavior as the fallback.
    assert_eq!(
        proxy_mode_from_value(Some("scheduled")),
        ProxyMode::Scheduled
    );
    assert_eq!(
        proxy_mode_from_value(Some("staggered")),
        ProxyMode::Scheduled
    );
    assert_eq!(
        proxy_mode_from_value(Some("lease-isolation")),
        ProxyMode::LeaseIsolation
    );
    assert_eq!(proxy_mode_from_value(Some("leader")), ProxyMode::Leader);
    assert_eq!(proxy_mode_from_value(None), ProxyMode::Leader);
}
