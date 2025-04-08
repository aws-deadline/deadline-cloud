#!/bin/sh
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Set the -e option
set -e

pip install --upgrade pip
pip install --upgrade hatch

hatch run installer:build
hatch run installer:make_exe