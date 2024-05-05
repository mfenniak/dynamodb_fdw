#!/bin/bash

set -eux -o pipefail

###########################################################
# Avoid publishing if not on main...

# if [[ $CODEBUILD_WEBHOOK_HEAD_REF != "refs/heads/main" || $CODEBUILD_SOURCE_REPO_URL != "https://github.com/mfenniak/dynamodb_fdw.git" ]];
# then
#     echo "Not a main branch build; aborting deployment."
#     exit 0
# fi

###########################################################
# Perform publish of docker image

export vtag=ghcr.io/mfenniak/dynamodb_fdw:$CODEBUILD_BUILD_NUMBER
export ltag=ghcr.io/mfenniak/dynamodb_fdw:latest

# `result` should be the built container from 04-build.sh
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
    -u $GITHUB_USER:$GITHUB_TOKEN \
    https://api.github.com/repos/mfenniak/dynamodb_fdw/releases
set -x
