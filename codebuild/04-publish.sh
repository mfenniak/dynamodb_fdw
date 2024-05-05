#!/bin/bash

set -eux -o pipefail

###########################################################
# Avoid publishing if not on main...

if [[ $CODEBUILD_WEBHOOK_HEAD_REF != "refs/heads/main" || $CODEBUILD_SOURCE_REPO_URL != "https://github.com/mfenniak/dynamodb_fdw.git" ]];
then
    echo "Not a main branch build; aborting deployment."
    exit 0
fi

###########################################################
# Retrieve secrets required for interaction with services

set +x # disable shell command logging temporarily to avoid logging...
GITHUB_PAT=$(aws ssm get-parameter --name /mfenniak/yycpathways/github-personal-access-token --with-decryption | jq -r '.Parameter.Value')
set -x

###########################################################
# Auth for github package registry

set +x # disable shell command logging temporarily to avoid logging...
echo $GITHUB_PAT | docker login ghcr.io -u mfenniak --password-stdin
set -x

###########################################################
# Perform publish of docker image

export vtag=ghcr.io/mfenniak/dynamodb_fdw:$CODEBUILD_BUILD_NUMBER
export ltag=ghcr.io/mfenniak/dynamodb_fdw:latest

# `result` should be the built container from 02-build.sh
docker load < result

# ensure latest & versioned tag are applied to the image
docker tag $vtag $ltag
docker push $vtag
docker push $ltag

###########################################################
# Create GitHub release tag

set +x # stop cmd logging for PAT security
curl -v -X POST -H "Content-Type:application/json" \
    --data "{\"tag_name\":\"$VERSION\",\"name\":\"$VERSION\",\"target_commitish\":\"$CODEBUILD_RESOLVED_SOURCE_VERSION\" }" \
    https://mfenniak:$GITHUB_PAT@api.github.com/repos/mfenniak/dynamodb_fdw/releases
set -x
