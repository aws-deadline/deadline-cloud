# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
The AWS Deadline Cloud CLI interface.
"""

from logging import getLogger
from typing import cast

import click

from . import _groups
from .. import version
from ._main import ContextTrackingGroup, CONTEXT_SETTINGS, deadline as deadline_original

logger = getLogger(__name__)


# Extract all the command names without the trailing "_group" or "_command"
subcommand_names = [v.rsplit("_", 1)[0].replace("_", "-") for v in sorted(_groups.__all__)]
SECTION_LINKS = "\nAvailable sub-commands:\n\n" + "".join(
    f"- [{v}](deadline_{v}.md)\n" for v in subcommand_names
)


# Make a clone of the deadline command, but without the subcommands, for use
# in generating the index.md for the CLI reference documentation. In the CLI reference,
# each subcommand is split out into a separate page, but mkdocs-click always includes
# subcommand docs.
@click.group(
    cls=ContextTrackingGroup,
    context_settings=CONTEXT_SETTINGS,
    help=cast(str, deadline_original.__doc__) + SECTION_LINKS,
)
@click.version_option(version=version, prog_name="deadline")
def deadline():
    pass


deadline.params = deadline_original.params
