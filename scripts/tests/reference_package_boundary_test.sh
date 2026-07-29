#!/usr/bin/env bash
# Adversarial checks for the exact-package metadata and manifest boundary.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
checker="$repo_root/scripts/reference-package-boundary-check"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/rafter-package-boundary-test.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

checkout="$scratch/checkout"
unpacked="$scratch/unpacked"
consumer="$scratch/consumer/reference"
harness="$consumer/harness"
never_publish="$scratch/never-publish"
baseline="$scratch/baseline.json"
mkdir -p "$checkout" "$unpacked/rafter-0.0.1" "$consumer/ledger" "$harness"
printf 'rafter-sim\n' > "$never_publish"
printf '[workspace.dependencies]\nrafter-reference-harness = { path = "harness" }\n' \
    > "$consumer/Cargo.toml"
printf '[dependencies]\nrafter = "0.0.1"\n' > "$consumer/ledger/Cargo.toml"
printf '[package]\nname = "rafter-reference-harness"\n' > "$harness/Cargo.toml"

jq -n \
    --arg consumer "$consumer/ledger/Cargo.toml" \
    --arg harness "$harness" \
    --arg rafter "$unpacked/rafter-0.0.1/Cargo.toml" \
    '{
        workspace_members: ["consumer"],
        packages: [
            {
                id: "consumer",
                name: "rafter-reference-ledger",
                manifest_path: $consumer,
                dependencies: [{
                    name: "rafter-reference-harness",
                    path: $harness,
                    features: []
                }]
            },
            {
                id: "rafter",
                name: "rafter",
                manifest_path: $rafter,
                dependencies: []
            }
        ],
        resolve: {nodes: [{id: "rafter", features: []}]}
    }' > "$baseline"

check_args=(
    --checkout-root "$checkout"
    --unpack-root "$unpacked"
    --consumer-workspace "$consumer"
    --harness-path "$harness"
    --never-publish-file "$never_publish"
)

"$checker" --metadata "$baseline" "${check_args[@]}" > /dev/null

expect_rejected() {
    local label="$1"
    local metadata="$2"
    if "$checker" --metadata "$metadata" "${check_args[@]}" > /dev/null 2>&1; then
        echo "error: boundary checker accepted adversarial case: $label" >&2
        exit 1
    fi
}

jq --arg path "$checkout/crates/rafter/Cargo.toml" \
    '(.packages[] | select(.id == "rafter") | .manifest_path) = $path' \
    "$baseline" > "$scratch/checkout-path.json"
expect_rejected "checkout path" "$scratch/checkout-path.json"

jq '(.packages[] | select(.id == "rafter") | .manifest_path) =
    "/registry/rafter-0.0.1/Cargo.toml"' \
    "$baseline" > "$scratch/registry-rafter.json"
expect_rejected "registry Rafter package" "$scratch/registry-rafter.json"

jq --arg path "$unpacked/rafter-sim-0.0.1/Cargo.toml" \
    '.packages += [{
        id: "rafter-sim",
        name: "rafter-sim",
        manifest_path: $path,
        dependencies: []
    }]' \
    "$baseline" > "$scratch/unpublished.json"
expect_rejected "unpublished crate" "$scratch/unpublished.json"

jq '(.resolve.nodes[] | select(.id == "rafter") | .features) =
    ["internal-test-hooks"]' \
    "$baseline" > "$scratch/internal-hook.json"
expect_rejected "internal test hook" "$scratch/internal-hook.json"

jq '(.packages[] | select(.id == "consumer") | .dependencies) += [{
        name: "forbidden",
        path: "/outside/forbidden",
        features: []
    }]' \
    "$baseline" > "$scratch/path-dependency.json"
expect_rejected "forbidden path dependency" "$scratch/path-dependency.json"

printf '\n[patch.crates-io]\nrafter = { path = "/checkout/rafter" }\n' \
    >> "$consumer/ledger/Cargo.toml"
expect_rejected "consumer patch table" "$baseline"

echo "reference package boundary adversarial checks passed"
