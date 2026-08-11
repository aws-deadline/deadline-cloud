# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Guards AWS Console sign-in support in the frozen installer.

The installer is a PyInstaller bundle with no pip, so a consumer cannot add the
"console" extra after installing it. If awscrt isn't bundled, botocore's
LoginProvider won't load and `deadline auth login` on a `login_session` profile
fails pointing at a `pip install` the user has no way to run.

Bundling it takes three things that are edited in separate files and are easy to
change apart from each other:

  * the `installer` Hatch env installing the extra (via envs.default's features),
  * `scripts/pyinstaller/allowlist.py` permitting awscrt in the signed artifact,
  * an attributions entry, since Apache-2.0 requires shipping the license text.
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

    # DEPENDENCIES only auto-generates globs for the package directory. The native
    # extension is underscore-prefixed at the bundle root, so it needs its own entry
    # on every platform we ship.
    globs = allowlist.ALLOWLIST["files"] + allowlist.ALLOWLIST["globs"]
    for native_extension in ("_internal/_awscrt.abi3.so", "_internal/_awscrt.pyd"):
        assert native_extension in globs, (
            f"{native_extension} must be allowlisted: awscrt's native extension does "
            "not match the globs generated for a DEPENDENCIES entry."
        )


def test_console_distribution_has_attribution() -> None:
    """Apache-2.0 obliges us to ship awscrt's license text in the installer."""
    import sys

    sys.path.insert(0, str(_REPO_ROOT / "scripts" / "attributions"))
    try:
        from cli import _ADDITIONAL_ATTRIBUTIONS, _ATTRIBUTIONS_ALLOW_LIST
    finally:
        sys.path.pop(0)

    attributed = set(_ATTRIBUTIONS_ALLOW_LIST) | {e["name"] for e in _ADDITIONAL_ATTRIBUTIONS}
    assert _CONSOLE_DISTRIBUTION in attributed, (
        f"{_CONSOLE_DISTRIBUTION!r} is bundled into the installer, so it needs an "
        "attributions entry. Run `hatch run attributions:update_approved_text` after "
        "adding one to refresh the golden license text."
    )
