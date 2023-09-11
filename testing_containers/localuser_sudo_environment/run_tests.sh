#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -eux

mkdir -p /home/hostuser/code/
cp -r /code/* /home/hostuser/code/
cp -r /code/.git /home/hostuser/code/

cd code
python -m venv .venv
source .venv/bin/activate
pip install hatch
# Use the codebuild env so that PIP_INDEX_URL isn't set in the hatch config files.
hatch run codebuild:test -m docker
