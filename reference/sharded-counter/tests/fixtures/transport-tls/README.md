# Test-only sharded-counter transport credentials

These certificates belong only to the authenticated sharded-counter process
suite. They protect no external service, contain no production secret, and
must never be reused outside tests.

The dedicated test CA signing key is absent. Nodes 1 through 3 are the three
stable physical transport principals, and every leaf is valid for the
fixture-only `rafter-peer` DNS name and for TLS client and server authentication.
