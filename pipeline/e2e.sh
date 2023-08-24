#!/bin/sh
# Set the -e option
set -e

pip install --upgrade pip
pip install --upgrade hatch
hatch run e2e:test