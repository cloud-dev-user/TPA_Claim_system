#!/usr/bin/env bash
# Generates a self-signed TLS certificate for local HTTPS development.
# Valid for: localhost, healthonetpa.local, 127.0.0.1
# Validity:  825 days (~2.25 years)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cat > /tmp/cert.conf << 'CONF'
[req]
default_bits       = 4096
prompt             = no
default_md         = sha256
distinguished_name = dn
x509_extensions    = v3_req

[dn]
C  = IN
ST = Maharashtra
L  = Mumbai
O  = HealthOne TPA
CN = localhost

[v3_req]
subjectAltName = @alt_names
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth

[alt_names]
DNS.1 = localhost
DNS.2 = healthonetpa.local
IP.1  = 127.0.0.1
CONF

openssl req -x509 -newkey rsa:4096 -sha256 -days 825 \
  -nodes \
  -keyout "${SCRIPT_DIR}/server.key" \
  -out "${SCRIPT_DIR}/server.crt" \
  -config /tmp/cert.conf

echo "Certificate generated:"
openssl x509 -in "${SCRIPT_DIR}/server.crt" -text -noout \
  | grep -E "(Subject:|DNS:|IP:|Not After)"
echo "Files: ${SCRIPT_DIR}/server.crt  ${SCRIPT_DIR}/server.key"
