#!/bin/bash

set -eux -o pipefail

echo "CODEBUILD_BUILD_NUMBER = $CODEBUILD_BUILD_NUMBER"
echo "CODEBUILD_WEBHOOK_TRIGGER = $CODEBUILD_WEBHOOK_TRIGGER"
echo "CODEBUILD_RESOLVED_SOURCE_VERSION = $CODEBUILD_RESOLVED_SOURCE_VERSION"

docker build -f Dockerfile -t mfenniak/dynamodb_fdw:${CODEBUILD_BUILD_NUMBER} .

if [ "$CODEBUILD_WEBHOOK_TRIGGER" == "branch/main" ];
then
    docker tag mfenniak/dynamodb_fdw:${CODEBUILD_BUILD_NUMBER} mfenniak/dynamodb_fdw:latest
    docker push mfenniak/dynamodb_fdw:${CODEBUILD_BUILD_NUMBER}
    docker push mfenniak/dynamodb_fdw:latest

    curl -f -v -X POST \
        -H "Content-Type: application/json" \
        --data "{\"tag_name\":\"$BUILD_NUM\",\"name\":\"$BUILD_NUM\",\"target_commitish\":\"$CODEBUILD_RESOLVED_SOURCE_VERSION\" }" \
        https://$GITHUB_USER:$GITHUB_TOKEN@api.github.com/repos/mfenniak/dynamodb_fdw/releases

    # Test commit
fi
