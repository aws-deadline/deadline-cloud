# Attributions Document Generation

In order to comply with the conditions attached to open source licenses, we attribute what we are distributing.
Where possible, we use pip-licenses to get the license information from our dependencies. In some cases, it is not possible to get this information using pip-licenses,
so we supplement what is being added to the attributions document with hard-coded license texts for packages where it is necessary. Additionally, we keep track of
the sha256 hashes of licenses to make sure they do not change when updating to new versions of packages.

## Quick Reference

### Prerequisites

- uv (`pip install uv`)

### Generate Attributions

1. `uv venv .venv-pip-licenses`
1. `source .venv-pip-licenses/bin/activate` (bash/zsh), or `source .venv-pip-licenses/bin/activate.fish` (fish), or `.venv-pip-licenses/Scripts/activate` (powershell)
1. `uv pip install pip-licenses`
1. `python scripts/attributions/cli.py -o THIRD_PARTY_LICENSES --dev --python uv`
1. `deactivate`

The `--dev` flag allows specifying `uv` or `mise` with the `--python` argument. If `--dev` is not specified, the `--python` argument must specify
the path to a Python interpreter. Before creating the virtual environment used to collect licenses, the version of the Python interpreter will be checked
to make sure it is the correct version for the platform.

Specifying `--python uv` will instruct uv to download the appropriate Python version when creating the virtual environment used
to collect licenses. `--python mise` will query mise to get the path of a Python interpreter of the appropriate version. This will fail if the appropriate version
is not installed with mise.

