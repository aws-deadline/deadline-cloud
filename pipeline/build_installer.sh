#!/bin/sh
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Set the -e option
set -e

hatch run attributions:generate
hatch run installer:prepare_artifacts
ls -al installer/components/DeadlineClient
ls -al installer/components/DeadlineClient/_internal
hatch run installer:build_installer "$@"
