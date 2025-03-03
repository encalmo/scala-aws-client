#!/bin/sh

$(scala run --dependency=org.encalmo::setup-aws-credentials:0.9.1 --main-class=org.encalmo.aws.SetupAwsCredentials --quiet -- --profile encalmo-sandbox)
echo "Using AWS profile $AWS_PROFILE and access key $AWS_ACCESS_KEY_ID"
export MUNIT_FLAKY_OK=true
if [ $# -ge 1 ]; then
    SUFFIX="--test-only *$1"
else
    SUFFIX=''
fi
if scala test . $SUFFIX --suppress-experimental-feature-warning --suppress-directives-in-multiple-files-warning --suppress-outdated-dependency-warning; then
    echo "[32mDone.[0m"
else
    echo "[41m[37mTests failed, check the log for the details.[0m"
fi
