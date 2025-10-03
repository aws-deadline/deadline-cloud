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
hatch run pytest --cov=src/deadline --cov-report=html:build/coverage --cov-report=xml:build/coverage/coverage.xml --cov-report=term-missing --cov-fail-under=25 -m docker
