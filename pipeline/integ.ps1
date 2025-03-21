$ErrorActionPreference = "Stop"

pip install --upgrade pip
pip install --upgrade hatch
hatch run integ:test
if ($LASTEXITCODE -ne 0) { throw "Failed to run integration tests" }

