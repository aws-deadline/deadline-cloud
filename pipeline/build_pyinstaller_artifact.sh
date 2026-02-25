#!/bin/sh
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Set the -e option
set -e

pip3 install --upgrade pip
pip3 install --upgrade hatch "virtualenv<21"

hatch run attributions:generate
hatch build
hatch run installer:make_exe
hatch run installer:validate_exe
