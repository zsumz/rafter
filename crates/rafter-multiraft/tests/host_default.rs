//! Generic Default construction must not require default application types or group keys.

use rafter_multiraft::{MultiRaftHost, TypedMultiRaftHost};
// Application marker deliberately implements no traits.
struct NotDefault;

#[test]
fn a_default_host_needs_no_default_key_command_or_result() {
    // Neither the comparable group key nor the command/result types need a
    // default value to construct an empty host. Keep the trait call explicit.
    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct GroupKey;

    let host = <TypedMultiRaftHost<GroupKey, NotDefault, NotDefault> as Default>::default();
    assert!(host.is_empty());
    assert_eq!(host.len(), 0);

    let untyped = <MultiRaftHost<GroupKey> as Default>::default();
    assert!(untyped.is_empty());
}
