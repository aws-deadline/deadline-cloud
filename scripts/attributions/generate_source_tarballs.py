#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Generates source code tarballs for LGPL-licensed dependencies (Qt, PySide6, Shiboken6)
to comply with LGPLv3 source distribution requirements.

Usage:
    python scripts/attributions/generate_source_tarballs.py --output-dir ./source-tarballs
    python scripts/attributions/generate_source_tarballs.py --output-dir ./source-tarballs --pyside-version 6.8.3
"""

import argparse
import re
import subprocess
import tempfile
from pathlib import Path

# Qt supermodule repo (contains all Qt modules as submodules)
_QT_REPO = "https://github.com/qt/qt5.git"

# PySide/Shiboken repo (contains both PySide6 and Shiboken6)
_PYSIDE_REPO = "https://code.qt.io/pyside/pyside-setup.git"


def _get_pinned_version() -> str:
    """Read the pinned PySide6 version from requirements-installer.txt."""
    req_file = Path(__file__).parent.parent.parent / "requirements-installer.txt"
    if not req_file.exists():
        raise RuntimeError(f"Cannot find {req_file}")
    content = req_file.read_text()
    match = re.search(r"PySide6-Essentials\s*==\s*(\S+)", content, re.IGNORECASE)
    if not match:
        raise RuntimeError(f"Cannot find PySide6-Essentials version pin in {req_file}")
    return match.group(1)


def _run(cmd: list[str], **kwargs) -> None:
    print(f"  $ {' '.join(cmd)}")
    subprocess.check_call(cmd, **kwargs)


def _clone_and_tar(
    repo_url: str,
    tag: str,
    archive_name: str,
    output_dir: Path,
    init_submodules: bool = False,
) -> Path:
    output_path = output_dir / f"{archive_name}.tar.gz"
    print(f"\nGenerating {output_path.name}...")

    with tempfile.TemporaryDirectory() as td:
        clone_dir = Path(td) / archive_name
        clone_cmd = ["git", "clone", "--depth", "1", "--branch", tag, repo_url, str(clone_dir)]
        _run(clone_cmd)

        if init_submodules:
            print("  Initializing submodules (this may take a while)...")
            _run(
                ["git", "submodule", "update", "--init", "--recursive", "--depth", "1"],
                cwd=clone_dir,
            )

        _run(
            [
                "tar",
                "-czf",
                str(output_path),
                "-C",
                td,
                archive_name,
            ]
        )

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Created {output_path.name} ({size_mb:.1f} MB)")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate source tarballs for LGPL dependencies")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write tarballs to",
    )
    parser.add_argument(
        "--pyside-version",
        type=str,
        default=None,
        help="PySide6/Qt version tag (default: read from requirements-installer.txt)",
    )
    parser.add_argument(
        "--skip-qt",
        action="store_true",
        help="Skip the Qt source tarball (large, ~1GB+)",
    )
    args = parser.parse_args()

    version = args.pyside_version or _get_pinned_version()
    tag = f"v{version}"
    print(f"Using version: {version} (tag: {tag})")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    tarballs = []

    # PySide6 + Shiboken6 (same repo)
    tarballs.append(
        _clone_and_tar(
            _PYSIDE_REPO,
            tag,
            f"pyside-setup-{version}-src",
            args.output_dir,
            init_submodules=False,
        )
    )

    # Qt framework
    if not args.skip_qt:
        tarballs.append(
            _clone_and_tar(
                _QT_REPO,
                tag,
                f"qt-{version}-src",
                args.output_dir,
                init_submodules=True,
            )
        )
    else:
        print("\nSkipping Qt source tarball (--skip-qt)")

    print("\n--- Summary ---")
    for t in tarballs:
        size_mb = t.stat().st_size / (1024 * 1024)
        print(f"  {t.name}  ({size_mb:.1f} MB)")
    print("\nUpload these to the S3 compliance bucket under: deadline-cloud/")
    print("Bucket: amazon-source-code-downloads")


if __name__ == "__main__":
    main()
