#!/bin/sh
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Set the -e option
set -e

hatch run installer:prepare_artifacts
hatch run installer:build_installer "$@"