#!/usr/bin/env bash

_release_dir=$1

if [ "$_system_type" == "Darwin" ]; then
  sed () {
    gsed "$@"
  }
fi

if [ -z "$ARTEFACTS_BUCKET_NAME" ]
then
  echo "Env variable ARTEFACTS_BUCKET_NAME is not set."
  exit 1
fi

_version="$CIRCLE_BUILD_NUM"

echo "Releasing v$_version to S3"
_s3_target_uri="s3://$ARTEFACTS_BUCKET_NAME/$CIRCLE_PROJECT_REPONAME/v$_version/"
aws s3 cp --recursive $_release_dir $_s3_target_uri
echo "Published: $_s3_target_uri"
