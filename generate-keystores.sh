#!/bin/bash
# Generates self-signed TLS keystores for all servers and a shared truststore.
# Run this once before building the Docker image.

set -e

VALIDITY=3650
PWD=changeme

SERVERS=(
    "users.ourorg0"
    "messages0.ourorg0"
    "messages1.ourorg0"
    "messages2.ourorg0"
    "users.ourorg1"
    "messages0.ourorg1"
    "messages1.ourorg1"
    "messages2.ourorg1"
    "users.ourorg2"
    "messages.ourorg2"
)

mkdir -p tls

# Remove existing truststore so we build it fresh
rm -f tls/client-truststore.ks

for SERVER in "${SERVERS[@]}"; do
    echo "Generating keystore for $SERVER ..."

    rm -f "tls/${SERVER}.ks" "tls/${SERVER}.crt"

    # Generate self-signed key pair with SAN matching the hostname
    keytool -genkeypair \
        -alias "$SERVER" \
        -keyalg RSA -keysize 2048 \
        -validity "$VALIDITY" \
        -storetype PKCS12 \
        -keystore "tls/${SERVER}.ks" \
        -storepass "$PWD" \
        -dname "CN=$SERVER" \
        -ext "SAN=dns:${SERVER}"

    # Export the certificate
    keytool -exportcert \
        -alias "$SERVER" \
        -keystore "tls/${SERVER}.ks" \
        -storepass "$PWD" \
        -file "tls/${SERVER}.crt" \
        -rfc

    # Import into the shared truststore
    keytool -importcert \
        -alias "$SERVER" \
        -file "tls/${SERVER}.crt" \
        -keystore tls/client-truststore.ks \
        -storepass "$PWD" \
        -noprompt

    rm -f "tls/${SERVER}.crt"
done

echo ""
echo "Done. Keystores in tls/, truststore: tls/client-truststore.ks"
