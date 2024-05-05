#!/bin/bash

set -eux -o pipefail

###########################################################
# Set up a running instance of the built dynamodb_fdw PG
# container

docker kill dynamodb_fdw || true # OK to fail if container doesn't exist
docker rm dynamodb_fdw || true # OK to fail if container doesn't exist

export vtag=ghcr.io/mfenniak/dynamodb_fdw:$CODEBUILD_BUILD_NUMBER
docker load < result

docker run \
    --name dynamodb_fdw \
    -d \
    --health-cmd "pg_isready -h localhost" --health-interval 2s --health-timeout 5s --health-retries 20 \
    -e AWS_CONTAINER_CREDENTIALS_RELATIVE_URI="$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" \
    $vtag

timeout=60  # timeout in seconds
start_time=$(date +%s)  # record start time

until [ "`docker inspect -f {{.State.Health.Status}} dynamodb_fdw`" = "healthy" ];
do
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))

    if [ "$elapsed" -ge "$timeout" ]; then
        echo "Timeout reached. Container did not become healthy.  Printing container logs:"
        docker logs dynamodb_fdw
        exit 1
    fi

    echo "Waiting for container to be healthy..."
    sleep 2
done

export FDW_IP=$(docker inspect dynamodb_fdw | jq -r '.[0].NetworkSettings.Networks | .[].IPAddress')

echo "Container is healthy.  FDW IP: $FDW_IP"

###########################################################
# Run tests

pytest
