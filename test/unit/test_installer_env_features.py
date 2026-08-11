# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Guards what PyInstaller bundles into the shipped installer.

The `installer` Hatch env's features decide which extras are installed when
`installer:make_exe` runs, and therefore which packages end up inside the signed
artifact. Hatch envs inherit `features` from `envs.default` when they don't
declare their own, so adding an extra to the default env silently changes the
installer's contents -- and `installer:validate_exe` then fails against
scripts/pyinstaller/allowlist.py, at release time rather than in PR CI.
"""

from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.9/3.10
    import tomli as tomllib  # type: ignore[no-redef]

_HATCH_CONFIG = Path(__file__).absolute().parents[2] / "hatch.toml"

# Extras that must never be bundled, mapped to why. An extra listed here brings in
# a distribution that scripts/pyinstaller/allowlist.py does not allow.
_EXTRAS_EXCLUDED_FROM_INSTALLER = {
    "console": "pulls in awscrt, a compiled wheel absent from the PyInstaller allowlist",
}


@pytest.fixture(scope="module")
def hatch_envs() -> dict:
    with open(_HATCH_CONFIG, "rb") as f:
        return tomllib.load(f)["envs"]


def test_installer_env_declares_features_explicitly(hatch_envs: dict) -> None:
    """Without its own `features`, the installer env inherits envs.default's."""
    assert "features" in hatch_envs["installer"], (
        "envs.installer must declare `features` explicitly. Inheriting from "
        "envs.default lets an extra added there change what PyInstaller bundles "
        "into the signed installer."
    )


@pytest.mark.parametrize("extra, reason", sorted(_EXTRAS_EXCLUDED_FROM_INSTALLER.items()))
def test_installer_env_omits_unbundleable_extra(hatch_envs: dict, extra: str, reason: str) -> None:
    assert extra not in hatch_envs["installer"].get("features", []), (
        f"envs.installer must not enable the {extra!r} extra: it {reason}. "
        "Either drop it or add the distribution to scripts/pyinstaller/allowlist.py."
    )
