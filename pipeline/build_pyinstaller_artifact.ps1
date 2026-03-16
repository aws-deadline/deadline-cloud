# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

$ErrorActionPreference = "Stop"

hatch run attributions:generate
if ($LASTEXITCODE -ne 0) { throw "Failed to generate attributions document" }

pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "Failed to update pip" }
pip install --upgrade hatch
if ($LASTEXITCODE -ne 0) { throw "Failed to update hatch" }

hatch build
if ($LASTEXITCODE -ne 0) { throw "Failed to build project" }
hatch run installer:make_exe
if ($LASTEXITCODE -ne 0) { throw "Failed to build pyinstaller artifact" }
hatch run installer:validate_exe
if ($LASTEXITCODE -ne 0) { throw "Failed to validate pyinstaller artifact" }
