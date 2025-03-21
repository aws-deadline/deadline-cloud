#!/bin/sh
# Set the -e option
set -e

hatch run installer:prepare_artifacts
hatch run installer:build_installer "$@"