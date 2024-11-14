#!/bin/bash
CERTS_DIR=/etc/clickhouse-server/certs/
mkdir -p $CERTS_DIR
# Copy cert files to $CERTS_DIR and apply required rights so as not to affect the original files
cp /tmp/certs/* $CERTS_DIR
chown clickhouse:clickhouse $CERTS_DIR*
chmod 644 $CERTS_DIR*
/entrypoint.sh