# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import shutil
from pathlib import Path

import click


@click.command()
@click.option(
    "--archive-path",
    required=True,
    type=Path,
    help="Path to the archive file to be extracted",
)
@click.option(
    "--output-path",
    required=True,
    type=Path,
    help="Path to the output directory where the archive will be extracted",
)
def main(archive_path: Path, output_path: Path) -> None:
    """Extracts the archive file to the output directory."""
    shutil.unpack_archive(archive_path, output_path)


if __name__ == "__main__":
    main()
