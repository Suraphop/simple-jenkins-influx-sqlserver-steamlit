#!/bin/bash

echo "********************"
echo "** Pushing image ***"
echo "********************"

IMAGE="machine-data-project"

echo "** Logging in ***"
docker login -u suraphop -p $PASS
echo "*** Tagging image ***"
docker tag $IMAGE:$BUILD_TAG suraphop/$IMAGE:$BUILD_TAG
echo "*** Pushing image ***"
docker push suraphop/$IMAGE:$BUILD_TAG
docker logout