#!/usr/bin/env bash
set -e

# Run init scripts
. /scripts/export-gitlab-ci-container-id.sh

exec "$@"
