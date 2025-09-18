# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

$ErrorActionPreference = "Stop"

pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "Failed to update pip" }
pip install --upgrade hatch "click<8.3"
if ($LASTEXITCODE -ne 0) { throw "Failed to update hatch" }

hatch run installer:build
if ($LASTEXITCODE -ne 0) { throw "Failed to build project" }
hatch run installer:make_exe
if ($LASTEXITCODE -ne 0) { throw "Failed to build pyinstaller artifact" }
