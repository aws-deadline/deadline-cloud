#!/bin/sh
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Set the -e option
set -e

pip install --upgrade pip
pip install --upgrade hatch
pip install --upgrade twine
hatch -v run codebuild:lint
hatch run codebuild:test
hatch -v run codebuild:build