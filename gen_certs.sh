#!/bin/bash
# Generate SSL certificate with Custom CA for Android/iOS trust
# Usage: ./gen_certs.sh

set -e

echo "=== Generating CA + Server Certificate ==="

# Clean up
rm -f ca.key ca.pem server.key server.pem server.csr ca.srl

# 1. CA key
openssl genrsa -out ca.key 4096 2>/dev/null

# 2. CA cert (self-signed)
openssl req -new -x509 -days 3650 -key ca.key -out ca.pem \
    -subj "/C=US/ST=State/L=City/O=SubStream/CN=SubStream CA" 2>/dev/null

# 3. Server key
openssl genrsa -out server.key 2048 2>/dev/null

# 4. Server CSR
openssl req -new -key server.key -out server.csr \
    -subj "/C=US/ST=State/L=City/O=SubStream/CN=localhost" 2>/dev/null

# 5. Server cert signed by CA (with all local IPs)
cat > /tmp/server.ext << EOF
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=@alt_names

[alt_names]
DNS.1=localhost
DNS.2=*.local
DNS.3=substream.local
IP.1=127.0.0.1
IP.2=::1
IP.3=0.0.0.0
IP.4=100.66.0.2
IP.5=192.168.1.106
IP.6=192.168.1.1
IP.7=192.168.0.1
IP.8=192.168.1.254
IP.9=10.0.0.1
IP.10=10.0.0.2
IP.11=10.1.1.1
IP.12=172.16.0.1
EOF

openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -out server.pem -days 365 -extfile /tmp/server.ext 2>/dev/null

# Rename for compatibility
cp server.pem cert.pem
cp server.key key.pem

echo ""
echo "=== Generated ==="
echo "  cert.pem / key.pem   - Server certificate (use these)"
echo "  ca.pem               - CA certificate (install on Android/iOS)"
echo ""
echo "=== Install CA on Android ==="
echo "1. adb push ca.pem /sdcard/Download/"
echo "2. Settings > Security > Install certificate > CA certificate"
echo "3. Select ca.pem"
echo ""
rm -f server.csr /tmp/server.ext ca.srl
