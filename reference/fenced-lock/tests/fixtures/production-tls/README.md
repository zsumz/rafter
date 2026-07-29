# Test-only production-composition credentials

These certificates belong only to the fenced-lock production-composition
acceptance suite. They protect no external service, contain no production
secret, and must never be reused outside tests.

- `ca.pem` is the dedicated test trust root. Its signing key is deliberately
  absent from the repository.
- Nodes 1 through 4 are the fixture's provisioned replica principals.
- Node 9 is signed by the same test CA but absent from the fixture's principal
  map, so the security suite can prove that a trusted-but-unknown certificate is
  refused before Raft sees a frame.

Every leaf certificate is valid for the fixture-only `rafter-peer` DNS name and
for both TLS client and server authentication.
