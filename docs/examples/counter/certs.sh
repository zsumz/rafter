#!/bin/sh
# Create credentials for this local exercise. Never replace an existing set.
set -eu
umask 077
mkdir certs
cat > certs/ca.cnf <<'CONFIG'
[req]
prompt = no
distinguished_name = dn
x509_extensions = ca
[dn]
CN = Rafter counter local CA
[ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
CONFIG
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
  -config certs/ca.cnf -keyout certs/ca-key.pem -out certs/ca.pem
for id in 1 2 3; do
  cat > "certs/node-$id.cnf" <<CONFIG
[req]
prompt = no
distinguished_name = dn
[dn]
CN = node-$id
[peer]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = DNS:node-$id
CONFIG
  openssl req -new -newkey rsa:2048 -nodes \
    -config "certs/node-$id.cnf" -keyout "certs/node-$id-key.pem" -out "certs/node-$id.csr"
  openssl x509 -req -days 30 -in "certs/node-$id.csr" \
    -CA certs/ca.pem -CAkey certs/ca-key.pem -set_serial "$id" \
    -extfile "certs/node-$id.cnf" -extensions peer -out "certs/node-$id.pem"
done
printf '\nCreated local TLS certificates in certs/.\n'
