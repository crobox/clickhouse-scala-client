#!/bin/bash

#
# Test your docker locally by first running:
# docker build -t test_build .
#

export REGISTRY_IMAGE="crobox/clickhouse-scala-client-build"
export VERSION_ID="0.1"

docker build --pull -t $REGISTRY_IMAGE:latest .
docker tag $REGISTRY_IMAGE:latest $REGISTRY_IMAGE:$VERSION_ID
docker push $REGISTRY_IMAGE:latest
docker push $REGISTRY_IMAGE:$VERSION_ID
docker rmi $REGISTRY_IMAGE:$VERSION_ID