#!/bin/sh
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Set the -e option
set -e

./pipeline/build.sh
twine upload --repository codeartifact dist/* --verbose