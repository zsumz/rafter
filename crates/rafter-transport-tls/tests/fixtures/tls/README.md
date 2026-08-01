# TLS test credentials

These certificates and private keys exist only for deterministic transport tests.
They are not deployment credentials and must never be reused outside this test
suite.

The three leaf certificates are signed by `ca.pem`, include both TLS client and
server extended-key usages, and name `rafter-peer.test` in their subject
alternative names. `node-a.pem` and `node-a-next.pem` are distinct credentials
for the same stable test principal so certificate-rotation behavior can be
verified without changing `PeerId`.
