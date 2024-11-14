#!/bin/bash
# Script is used to (re)generate self-signed certificates needed to run the tests
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CERTS_DIR=$SCRIPT_DIR/certs
rm -rf $CERTS_DIR
mkdir -p $CERTS_DIR
cd $CERTS_DIR
openssl genrsa -out ca.key 2048
openssl req -x509 -subj "/CN=clickhouseserver.test CA" -nodes -key ca.key -days 3650 -out ca.crt
openssl req -newkey rsa:2048 -nodes -subj "/CN=clickhouseserver.test" -keyout server.key -out server.csr
openssl x509 -req -in server.csr -out server.crt -CA ca.crt -CAkey ca.key -days 3650 -copy_extensions copy
openssl req -newkey rsa:2048 -nodes -subj "/CN=clickhouseserver.test" -keyout client.key -out client.csr
openssl x509 -req -in client.csr -out client.crt -CA ca.crt -CAkey ca.key -days 3650 -copy_extensions copy

openssl pkcs12 -export -in client.crt -inkey client.key -out keystore.p12 -name client -CAfile ca.crt -caname 'clickhouseserver.test CA' -password pass:password

keytool -importkeystore -deststorepass password -destkeypass password -destkeystore keystore.jks -deststoretype JKS -srckeystore keystore.p12 -srcstoretype PKCS12 -srcstorepass password -alias client -noprompt

keytool -importcert -alias ca -file ca.crt -keystore keystore.jks -storepass password -noprompt
rm ca.key client.key client.csr client.crt server.csr keystore.p12 