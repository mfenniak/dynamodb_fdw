#!/bin/bash

set -eux -o pipefail

echo "CODEBUILD_BUILD_NUMBER = $CODEBUILD_BUILD_NUMBER"
echo "CODEBUILD_WEBHOOK_TRIGGER = $CODEBUILD_WEBHOOK_TRIGGER"
echo "CODEBUILD_RESOLVED_SOURCE_VERSION = $CODEBUILD_RESOLVED_SOURCE_VERSION"

sed -i"" -e "s/9.9/$CODEBUILD_BUILD_NUMBER/" ./flake.nix

###########################################################
# Build container

nix build --print-build-logs ".#docker"

###########################################################
# Perform publish of docker image

export vtag=ghcr.io/mfenniak/dynamodb_fdw:$CODEBUILD_BUILD_NUMBER
export ltag=ghcr.io/mfenniak/dynamodb_fdw:latest

# `result` should be the built container from 04-build.sh
docker load < result

# if [ "$CODEBUILD_WEBHOOK_TRIGGER" == "branch/main" ];
# then

# ensure latest & versioned tag are applied to the image
docker tag $vtag $ltag
docker push $vtag
docker push $ltag

# fi

###########################################################
# Create GitHub release tag

# if [ "$CODEBUILD_WEBHOOK_TRIGGER" == "branch/main" ];
# then

set +x # stop cmd logging for auth security
curl -v -X POST -H "Content-Type:application/json" \
    --data "{\"tag_name\":\"$CODEBUILD_BUILD_NUMBER\",\"name\":\"$CODEBUILD_BUILD_NUMBER\",\"target_commitish\":\"$CODEBUILD_RESOLVED_SOURCE_VERSION\" }" \
    -u $GITHUB_USER:$GITHUB_TOKEN \
    https://api.github.com/repos/mfenniak/dynamodb_fdw/releases
set -x

# fi
