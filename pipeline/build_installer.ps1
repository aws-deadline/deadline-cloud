# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

$ErrorActionPreference = "Stop"

hatch run installer:prepare_artifacts
if ($LASTEXITCODE -ne 0) { throw "Failed to prepare artifacts" }
hatch run installer:build_installer @args
if ($LASTEXITCODE -ne 0) { throw "Failed to build installer" }
