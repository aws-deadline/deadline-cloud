# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Stop on first error
$ErrorActionPreference = "Stop"

# Install/upgrade packages
python -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "Failed to update pip" }
python -m pip install --upgrade hatch
if ($LASTEXITCODE -ne 0) { throw "Failed to update hatch" }
python -m pip install --upgrade twine
if ($LASTEXITCODE -ne 0) { throw "Failed to update twine" }

# Run hatch commands
hatch -v run codebuild:lint
if ($LASTEXITCODE -ne 0) { throw "Failed to run lint" }
hatch run codebuild:test
if ($LASTEXITCODE -ne 0) { throw "Failed to run test" }
hatch -v run codebuild:build
if ($LASTEXITCODE -ne 0) { throw "Failed to run build" }
