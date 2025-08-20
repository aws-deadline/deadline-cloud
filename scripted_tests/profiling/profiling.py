# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import subprocess

from enum import Enum, unique
from pathlib import Path
from typing import Optional, Union

import click


@unique
class OutputFormat(Enum):
    HTML = "html"
    JSON = "json"


def profile(
    name: str,
    farm_id: str,
    queue_id: str,
    job_bundle: Path,
    output_dir: Path,
    output_format: OutputFormat,
    parameters: Optional[dict[str, Union[int, float, str, Path]]] = None,
) -> None:
    expanded_params = [
        ["--parameter", f"{key}={str(value)}"]
        for key, value in (parameters.values() if parameters is not None else {})
    ]
    subprocess.run(
        [
            "pyinstrument",
            "--renderer",
            str(output_format.value).lower(),
            "--outfile",
            str(output_dir / f"{name}.{str(output_format.value).lower()}"),
            "--from-path",
            "deadline",
            "bundle",
            "submit",
            str(job_bundle),
            "--farm-id",
            farm_id,
            "--queue-id",
            queue_id,
            *expanded_params,
            "--yes",
        ],
        input="y\n",
        text=True,
        check=True,
    )


@click.command()
@click.option("--farm-id", envvar="FARM_ID", type=str, required=True, help="The farm to submit to")
@click.option(
    "--queue-id", envvar="QUEUE_ID", type=str, required=True, help="The queue to submit to"
)
@click.option("--output-dir", type=Path, required=True, help="The prefix to output the results to")
@click.option(
    "--output-format",
    type=click.Choice(OutputFormat),
    default=OutputFormat.HTML,
    help="The format the output should be in",
)
def cli(farm_id: str, queue_id: str, output_dir: Path, output_format: OutputFormat) -> None:
    job_bundle_dir = Path(__file__).parent.parent / "job_bundles"
    if not output_dir.is_dir():
        output_dir.mkdir(parents=True)
    profile(
        "minimal_job_bundle",
        farm_id,
        queue_id,
        job_bundle_dir / "minimal_job_bundle",
        output_dir,
        output_format,
    )
    profile(
        "with_job_attachments",
        farm_id,
        queue_id,
        job_bundle_dir / "with_job_attachments",
        output_dir,
        output_format,
    )


if __name__ == "__main__":
    cli()
