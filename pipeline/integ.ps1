# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

$ErrorActionPreference = "Stop"

pip install --upgrade pip
pip install --upgrade hatch "click<8.3"
hatch run integ:test
if ($LASTEXITCODE -ne 0) { throw "Failed to run integration tests" }
