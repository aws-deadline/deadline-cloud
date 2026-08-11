# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Guards AWS Console sign-in support in the frozen installer.

The installer is a PyInstaller bundle with no pip, so a consumer cannot add the
"console" extra after installing it. If awscrt isn't bundled, botocore's
LoginProvider won't load and `deadline auth login` on a `login_session` profile
fails pointing at a `pip install` the user has no way to run.

Bundling it takes two things edited in separate files and easy to change apart
from each other: the `installer` Hatch env installing the extra (via
envs.default's features), and `scripts/pyinstaller/allowlist.py` permitting
awscrt in the signed artifact.

These assert on config, which cannot see whether awscrt reached the artifact --
PyInstaller finds it only by static analysis through botocore, and the allowlist
permits files rather than requiring them. `test/installer/test_installer.py`
asserts on the built bundle; attributions are covered by
`_validate_bundled_attributions`, which `attributions:check` runs over every
DEPENDENCIES entry.
"""

from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.9/3.10
    import tomli as tomllib  # type: ignore[no-redef]

_REPO_ROOT = Path(__file__).absolute().parents[2]
_HATCH_CONFIG = _REPO_ROOT / "hatch.toml"
_ALLOWLIST = _REPO_ROOT / "scripts" / "pyinstaller" / "allowlist.py"

# The extra that pulls in awscrt, and the distribution it pulls in.
_CONSOLE_EXTRA = "console"
_CONSOLE_DISTRIBUTION = "awscrt"


@pytest.fixture(scope="module")
def installer_env_features() -> list[str]:
    """The features the `installer` env resolves to, including what it inherits."""
    with open(_HATCH_CONFIG, "rb") as f:
        envs = tomllib.load(f)["envs"]
    # Hatch falls back to envs.default's features when an env declares none.
    return envs["installer"].get("features", envs["default"].get("features", []))


def test_installer_env_installs_console_extra(installer_env_features: list[str]) -> None:
    assert _CONSOLE_EXTRA in installer_env_features, (
        f"The installer build must install the {_CONSOLE_EXTRA!r} extra. Without it "
        "the frozen bundle has no awscrt, and AWS Console sign-in is unreachable "
        "there because a PyInstaller bundle has no pip to add the extra with."
    )


def test_console_distribution_is_allowlisted() -> None:
    """Bundling awscrt without allowlisting it fails installer:validate_exe."""
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("_allowlist", _ALLOWLIST)
    assert spec is not None and spec.loader is not None
    allowlist = module_from_spec(spec)
    spec.loader.exec_module(allowlist)

    assert _CONSOLE_DISTRIBUTION in allowlist.DEPENDENCIES, (
        f"{_CONSOLE_DISTRIBUTION!r} is bundled into the installer, so it must be in "
        "allowlist.DEPENDENCIES or installer:validate_exe rejects the artifact."
    )

    # The .so globs generated for a DEPENDENCIES entry only cover
    # lib-dynload/*.cpython-3*-*.so, so the abi3-tagged extension at the bundle root
    # needs its own entry. Windows is already covered by the generated "**/_{dep}.pyd".
    globs = allowlist.ALLOWLIST["files"] + allowlist.ALLOWLIST["globs"]
    assert "_internal/_awscrt.abi3.so" in globs, (
        "_internal/_awscrt.abi3.so must be allowlisted: an abi3-tagged extension at "
        "the bundle root matches none of the globs generated for a DEPENDENCIES entry."
    )
