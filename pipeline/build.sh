#!/bin/sh
# Set the -e option
set -e

pip install --upgrade pip
pip install --upgrade hatch
pip install --upgrade twine
hatch run codebuild:lint
hatch run codebuild:test
hatch run codebuild:build