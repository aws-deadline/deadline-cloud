# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

$ErrorActionPreference = "Stop"

hatch run test_installer
if ($LASTEXITCODE -ne 0) { throw "Failed to test installer" }
