# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import click
import json
import typing as t


class ClickLogger:
    """
    Wrapper around click that is JSON aware. Users can instantiate this as a
    replacement for using `click.echo`. A helper JSON function is also provided
    to output JSON.
    """

    def __init__(self, is_json: bool):
        self._is_json = is_json

    def is_json(self) -> bool:
        """
        Is logging in JSON mode.
        """
        return self._is_json

    def echo(
        self,
        message: t.Optional[t.Any] = None,
        file: t.Optional[t.IO[t.Any]] = None,
        nl: bool = True,
        err: bool = False,
        color: t.Optional[bool] = None,
    ):
        if not self._is_json:
            click.echo(message, file, nl, err, color)

    def json(
        self,
        message: t.Optional[dict] = None,
        file: t.Optional[t.IO[t.Any]] = None,
        nl: bool = True,
        err: bool = False,
        color: t.Optional[bool] = None,
        indent=None,
    ):
        if self._is_json:
            click.echo(json.dumps(obj=message, indent=indent), file, nl, err, color)
