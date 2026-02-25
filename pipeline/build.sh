#!/bin/sh
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Set the -e option
set -e

pip install --upgrade pip
pip install --upgrade hatch "virtualenv<21"
pip install --upgrade twine
hatch -v run lint
hatch run test
hatch -v build
