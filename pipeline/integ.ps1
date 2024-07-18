$ErrorActionPreference = "Stop"

pip install --upgrade pip
pip install --upgrade hatch
hatch run integ:test