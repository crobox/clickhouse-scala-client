#!/usr/bin/env bash
set -e

# Retrieve the container id from current running docker processes
HOST_NAME=$(hostname)
CONTAINER_ID=$(docker ps | grep "$HOST_NAME" | awk '{print $1}')

# Expose the container id as an environment variable
export GITLAB_CI_CONTAINER_ID="$CONTAINER_ID"
