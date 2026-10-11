//! Exact normal dependencies and reasoned dev-only exceptions for the DAG guard.

use super::set;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn allowed_normal_workspace_deps(crate_name: &str) -> BTreeSet<String> {
    set(match crate_name {
        "rafter" | "rafter-crc32" | "rafter-invariant-test-macros" | "rafter-invariants" => &[],
        "rafter-invariant-test" => &["rafter-invariant-test-macros"],
        "rafter-app" => &["rafter", "rafter-runtime-api"],
        "rafter-codec" | "rafter-storage" => &["rafter", "rafter-crc32"],
        "rafter-runtime-api" | "rafter-sim" => &["rafter"],
        "rafter-maelstrom" => &["rafter", "rafter-codec", "rafter-runtime", "rafter-storage"],
        "rafter-multiraft" => &["rafter-app", "rafter-runtime-api"],
        "rafter-runtime" => &[
            "rafter",
            "rafter-crc32",
            "rafter-runtime-api",
            "rafter-storage",
        ],
        "rafter-service" => &["rafter", "rafter-app", "rafter-runtime-api"],
        "rafter-transport-tcp-insecure" => &["rafter", "rafter-codec"],
        "rafter-transport-tls" => &["rafter", "rafter-codec", "rafter-crc32", "rafter-service"],
        unexpected => panic!("missing normal dependency policy for `{unexpected}`"),
    })
}

const RAFTER_TRANSPORT_TLS_DEV_DEPS: &[(&str, &str)] = &[
    (
        "rafter-app",
        "transport integration tests exercise the public application adapter",
    ),
    (
        "rafter-runtime",
        "transport integration tests instantiate durable runtimes",
    ),
    (
        "rafter-storage",
        "transport integration tests use public durable stores",
    ),
];

pub(super) fn allowed_dev_workspace_deps(crate_name: &str) -> BTreeMap<String, &'static str> {
    let entries: &[(&str, &str)] = match crate_name {
        "rafter-app" => &[
            (
                "rafter-invariant-test",
                "registered app tests emit typed invariant-oracle verdicts",
            ),
            (
                "rafter-runtime",
                "app tests/examples may instantiate DurableRaftNode without making app depend on it",
            ),
            (
                "rafter-storage",
                "app tests/examples may use stores without making app depend on storage",
            ),
        ],
        "rafter-multiraft" => &[
            (
                "rafter",
                "multiraft tests may inspect core IDs and reports without widening normal deps",
            ),
            (
                "rafter-runtime",
                "multiraft tests may instantiate durable runtimes without widening normal deps",
            ),
            (
                "rafter-storage",
                "multiraft tests may use stores without widening normal deps",
            ),
        ],
        "rafter-runtime" => &[
            (
                "rafter-invariant-test",
                "registered runtime tests emit typed invariant-oracle verdicts",
            ),
            (
                "rafter-transport-tcp-insecure",
                "runtime examples may use the demo TCP transport without making runtime depend on transport",
            ),
        ],
        "rafter-invariants" => &[
            (
                "rafter",
                "validator integration tests construct canonical node configs without widening verifier runtime dependencies",
            ),
            (
                "rafter-sim",
                "validator integration tests consume actual simulator liveness JSON without widening verifier runtime dependencies",
            ),
        ],
        "rafter-service" => &[
            (
                "rafter-runtime",
                "service tests/examples may instantiate durable runtimes without making service depend on runtime",
            ),
            (
                "rafter-storage",
                "service tests/examples may use stores without making service depend on storage",
            ),
        ],
        "rafter" => &[(
            "rafter-invariant-test",
            "registered core tests emit typed invariant-oracle verdicts",
        )],
        "rafter-maelstrom" => &[(
            "rafter-invariant-test",
            "registered Maelstrom tests emit typed invariant-oracle verdicts",
        )],
        "rafter-sim" => &[(
            "rafter-invariant-test",
            "registered simulator tests emit typed invariant-oracle verdicts",
        )],
        "rafter-storage" => &[(
            "rafter-invariant-test",
            "registered storage tests emit typed invariant-oracle verdicts",
        )],
        "rafter-transport-tls" => RAFTER_TRANSPORT_TLS_DEV_DEPS,
        "rafter-codec"
        | "rafter-crc32"
        | "rafter-invariant-test"
        | "rafter-invariant-test-macros"
        | "rafter-runtime-api"
        | "rafter-transport-tcp-insecure" => &[],
        unexpected => panic!("missing dev dependency policy for `{unexpected}`"),
    };

    entries
        .iter()
        .map(|(dep, reason)| ((*dep).to_owned(), *reason))
        .collect()
}
