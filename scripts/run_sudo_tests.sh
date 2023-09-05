#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -eu

# Run this from the root of the repository
if ! test -d scripts
then
    echo "Must run from the root of the repository"
    exit 1
fi

DO_BUILD="False"
BUILD_ONLY="False"
while [[ "${1:-}" != "" ]]; do
    case $1 in
        -h|--help)
            echo "Usage: run_sudo_tests.sh [--build]"
            exit 1
            ;;
        --build)
            DO_BUILD="True"
            ;;
        --build-only)
            BUILD_ONLY="True"
            ;;
        *)
            echo "Unrecognized parameter: $1"
            exit 1
            ;;
    esac
    shift
done

# Copying the dist/ dir can cause permission issues, so just nuke it.
hatch clean 2> /dev/null || true

ARGS=""

if test "${PIP_INDEX_URL:-}" != ""; then
    # If PIP_INDEX_URL is set, then export that in to the container
    # so that `pip install` run in the container will fetch packages
    # from the correct repository.
    ARGS="${ARGS} -e  PIP_INDEX_URL=${PIP_INDEX_URL}"
fi

ARGS="${ARGS} -h localuser.environment.internal"
CONTAINER_IMAGE_TAG="job_attachment_localuser_test"
CONTAINER_IMAGE_DIR="localuser_sudo_environment"

if test "${DO_BUILD}" == "True"; then
    docker build testing_containers/"${CONTAINER_IMAGE_DIR}" -t "${CONTAINER_IMAGE_TAG}"
fi

if test "${BUILD_ONLY}" == "True"; then
    exit 0
fi

docker run --name test_sudo --rm -v $(pwd):/code:ro ${ARGS} "${CONTAINER_IMAGE_TAG}":latest 
